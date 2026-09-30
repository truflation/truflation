"""Stage TN oracle tips to disk and choose which files a drain should send.

Calc runs write ``trufnetwork/pending/*.json``. A later drain broadcasts them.
``event_time`` is not stored at stage time; the drain stamps ``now()`` so the
on-chain date is the broadcast time.

``TN_WRITE_MODE=live`` (default) still inserts immediately. ``stage`` only
writes the pending file. Watermarks are updated after a successful insert,
not when the file is staged. The drain keeps the newest staged tip per stream
so earlier files from the same day are not sent twice.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

# select(batch_key, table) -> True when that staged stream should be sent now.
# ``table`` is None for files staged before streams recorded their table.
StreamSelector = Callable[[str, Optional[str]], bool]


def tn_root() -> Path:
    return Path(os.environ.get('TN_STAGE_DIR', 'trufnetwork'))


def tn_write_mode() -> str:
    mode = os.environ.get('TN_WRITE_MODE', 'live').strip().lower()
    if mode not in ('live', 'stage'):
        raise ValueError(f"TN_WRITE_MODE must be 'live' or 'stage', got {mode!r}")
    return mode


def _safe_name(batch_key: str) -> str:
    cleaned = re.sub(r'[^A-Za-z0-9._-]+', '_', batch_key).strip('._')
    return cleaned or 'batch'


def _json_value(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return value


def stage_batches(batch_key: str, batches: list[dict], insert_mode: str, root: Path | None = None) -> Path:
    """Write one pending file for a finalized in-memory batch. Returns the path."""
    root = root or tn_root()
    pending = root / 'pending'
    pending.mkdir(parents=True, exist_ok=True)
    created_at = datetime.now(timezone.utc).isoformat()
    streams = []
    for batch in batches:
        records = []
        for rec in batch.get('inputs') or []:
            item = {'value': _json_value(rec.get('value'))}
            if 'date' in rec and rec['date'] is not None:
                item['date'] = int(rec['date'])
            records.append(item)
        streams.append({
            'stream_id': batch['stream_id'],
            'table': batch.get('table'),
            'data_provider': batch.get('data_provider'),
            'observation_date': batch.get('observation_date'),
            'rewrite_event_time': bool(batch.get('rewrite_event_time', True)),
            'records': records,
        })
    payload = {
        'batch_key': batch_key,
        'created_at': created_at,
        'if_exists': insert_mode,
        'streams': streams,
    }
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    path = pending / f'{stamp}_{_safe_name(batch_key)}.json'
    tmp = path.with_suffix('.json.tmp')
    with tmp.open('w') as handle:
        json.dump(payload, handle)
    tmp.replace(path)
    return path


def recover_processing(root: Path | None = None) -> list[Path]:
    """Move leftover processing files back to pending so a crashed drain retries."""
    root = root or tn_root()
    processing = root / 'processing'
    if not processing.is_dir():
        return []
    moved = []
    for path in sorted(processing.glob('*.json')):
        moved.append(relocate(path, root / 'pending'))
    return moved


def read_actionable(root: Path | None = None) -> tuple[list[tuple[Path, dict]], list[tuple[Path, str]]]:
    """Pending files plus failed files from an earlier drain, oldest name first.

    The second list is files that could not be parsed. Callers should log them
    and move them to ``failed/`` so a bad file does not sit in ``pending/`` forever.
    """
    root = root or tn_root()
    found: list[Path] = []
    for folder in ('pending', 'failed'):
        directory = root / folder
        if directory.is_dir():
            found.extend(directory.glob('*.json'))
    payloads = []
    bad: list[tuple[Path, str]] = []
    for path in sorted(found, key=lambda item: item.name):
        try:
            with path.open() as handle:
                payload = json.load(handle)
        except (OSError, json.JSONDecodeError) as err:
            bad.append((path, str(err)))
            continue
        if isinstance(payload, dict):
            payloads.append((path, payload))
        else:
            bad.append((path, 'payload is not an object'))
    return payloads, bad


@dataclass
class DrainPlan:
    streams: list[dict] = field(default_factory=list)
    source_files: list[Path] = field(default_factory=list)
    superseded_files: list[Path] = field(default_factory=list)
    # Files that also hold streams this drain must not send. Those streams
    # have to be split back into pending before the file is moved.
    held: dict[Path, list[dict]] = field(default_factory=dict)


def plan_drain(payloads: list[tuple[Path, dict]], select: StreamSelector) -> DrainPlan:
    """Newest tip per selected stream. Older files are superseded.

    Streams ``select(batch_key, table)`` rejects are returned in ``held`` so
    the caller keeps them pending.
    """
    chosen: list[tuple[Path, dict, list[dict]]] = []
    held: dict[Path, list[dict]] = {}
    for path, payload in payloads:
        batch_key = str(payload.get('batch_key') or '')
        selected: list[dict] = []
        rejected: list[dict] = []
        for stream in payload.get('streams') or []:
            (selected if select(batch_key, stream.get('table')) else rejected).append(stream)
        if not selected:
            continue
        chosen.append((path, payload, selected))
        if rejected:
            held[path] = rejected

    best: dict[str, tuple[str, Path, dict, str]] = {}
    for path, payload, selected in chosen:
        created = str(payload.get('created_at') or '')
        insert_mode = payload.get('if_exists') or 'append'
        for stream in selected:
            sid = stream.get('stream_id')
            if not sid:
                continue
            previous = best.get(sid)
            if previous is None or created >= previous[0]:
                best[sid] = (created, path, stream, insert_mode)

    source = {item[1] for item in best.values()}
    superseded = [path for path, _payload, _selected in chosen if path not in source]
    streams = []
    for _sid, (_created, _path, stream, insert_mode) in best.items():
        streams.append({**stream, 'if_exists': insert_mode})
    return DrainPlan(
        streams=streams,
        source_files=sorted(source),
        superseded_files=superseded,
        held=held,
    )


def split_held(plan: DrainPlan, by_path: dict[Path, dict], root: Path | None = None) -> list[Path]:
    """Move streams this drain must not send into their own pending file.

    The held file keeps the original ``created_at``, so newest-per-stream
    ordering is unchanged. The original file is rewritten with only the
    selected streams. A crash between the two writes leaves the held streams
    in both files, which the next drain resolves as duplicates.
    """
    root = root or tn_root()
    pending = root / 'pending'
    pending.mkdir(parents=True, exist_ok=True)
    written = []
    for path, keep in plan.held.items():
        payload = by_path[path]
        held_ids = {id(stream) for stream in keep}
        target = pending / f'{path.stem}_held{path.suffix}'
        if target.exists():
            target = pending / f'{path.stem}_held_{datetime.now(timezone.utc).strftime("%H%M%S%f")}{path.suffix}'
        write_payload(target, {**payload, 'streams': keep})
        payload['streams'] = [s for s in payload.get('streams') or [] if id(s) not in held_ids]
        write_payload(path, payload)
        written.append(target)
    return written


def group_send_batches(streams: list[dict]) -> dict[str, list]:
    """One insert row per winning stream.

    A file that won for one stream can still hold an older copy of another.
    Callers must pass the winning records only, not every stream in that file.
    """
    grouped: dict[str, list] = {'append': [], 'replace': []}
    for stream in streams:
        mode = stream.get('if_exists') or 'append'
        if mode not in grouped:
            mode = 'append'
        grouped[mode].append({
            'stream_id': stream.get('stream_id'),
            'data_provider': stream.get('data_provider'),
            'inputs': stream.get('records') or [],
            'observation_date': stream.get('observation_date'),
        })
    return grouped


def unpublished_streams(streams: list[dict], load_watermark, should_skip) -> tuple[list[dict], list[dict]]:
    """Drop tips whose observation day and value already match the watermark.

    A recovered processing file must not be broadcast again after slot-0
    already saved the watermark. History rows have no observation_date and
    are always kept.
    """
    send: list[dict] = []
    skip: list[dict] = []
    for stream in streams:
        records = stream.get('records') or []
        value = records[-1].get('value') if records else None
        obs = stream.get('observation_date')
        if obs and should_skip(obs, value, load_watermark(stream.get('stream_id'))):
            skip.append(stream)
            continue
        send.append(stream)
    return send, skip


def freeze_event_time(payload: dict, event_time: int) -> dict:
    """Stamp event_time once and keep it on retry.

    ``rewrite_event_time`` is cleared so a later drain does not mint a second
    on-chain date for the same staged tip.
    """
    for stream in payload.get('streams') or []:
        if not stream.get('rewrite_event_time', True):
            continue
        for rec in stream.get('records') or []:
            if rec.get('date') is None:
                rec['date'] = int(event_time)
        stream['rewrite_event_time'] = False
    return payload


def write_payload(path: Path, payload: dict) -> None:
    tmp = path.with_suffix('.json.tmp')
    with tmp.open('w') as handle:
        json.dump(payload, handle)
    tmp.replace(path)


def stamp_streams(streams: list[dict], event_time: int) -> list[dict]:
    """Set on-chain ``date`` to the broadcast time for oracle tips."""
    stamped = []
    for stream in streams:
        records = []
        for rec in stream.get('records') or []:
            item = dict(rec)
            if stream.get('rewrite_event_time', True):
                item['date'] = int(event_time)
            records.append(item)
        stamped.append({**stream, 'records': records})
    return stamped


def relocate(path: Path, dest_dir: Path) -> Path:
    dest_dir.mkdir(parents=True, exist_ok=True)
    target = dest_dir / path.name
    if target.exists():
        target = dest_dir / f'{path.stem}_{datetime.now(timezone.utc).strftime("%H%M%S%f")}{path.suffix}'
    path.replace(target)
    return target
