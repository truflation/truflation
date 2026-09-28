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


def tn_root() -> Path:
    return Path(os.environ.get('TN_STAGE_DIR', 'trufnetwork'))


def tn_write_mode() -> str:
    mode = os.environ.get('TN_WRITE_MODE', 'live').strip().lower()
    if mode not in ('live', 'stage'):
        raise ValueError(f"TN_WRITE_MODE must be 'live' or 'stage', got {mode!r}")
    return mode


def is_us_gated_batch(batch_key: str) -> bool:
    """Batches whose API files wait until 08:29 America/New_York.

    Custom indexes that merely contain "us" in the table name (gasoline,
    rent, eggs) are not in this set. They drain with everything else.
    """
    if batch_key.startswith(('cpi-us_', 'cpi-divergence-us_', 'categories_us_')):
        return True
    if batch_key.startswith('mapping-') and '-us_' in batch_key:
        return True
    if batch_key in ('gov_bea', 'gov_bea_yoy'):
        return True
    return False


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
            'data_provider': batch.get('data_provider'),
            'observation_date': batch.get('observation_date'),
            'rewrite_event_time': bool(batch.get('rewrite_event_time', True)),
            'records': records,
        })
    payload = {
        'batch_key': batch_key,
        'created_at': created_at,
        'if_exists': insert_mode,
        'gate': 'us' if is_us_gated_batch(batch_key) else 'default',
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


def read_actionable(root: Path | None = None) -> list[tuple[Path, dict]]:
    """Pending files plus failed files from an earlier drain, oldest name first."""
    root = root or tn_root()
    found: list[Path] = []
    for folder in ('pending', 'failed'):
        directory = root / folder
        if directory.is_dir():
            found.extend(directory.glob('*.json'))
    payloads = []
    for path in sorted(found, key=lambda item: item.name):
        try:
            with path.open() as handle:
                payload = json.load(handle)
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(payload, dict):
            payloads.append((path, payload))
    return payloads


@dataclass
class DrainPlan:
    streams: list[dict] = field(default_factory=list)
    source_files: list[Path] = field(default_factory=list)
    superseded_files: list[Path] = field(default_factory=list)


def plan_drain(payloads: list[tuple[Path, dict]], scope: str) -> DrainPlan:
    """Newest tip per stream for this scope. Older files are superseded.

    ``scope='us'`` is the 08:29 America/New_York drain. ``scope='default'``
    is everything else.
    """
    if scope not in ('us', 'default'):
        raise ValueError(f"scope must be 'us' or 'default', got {scope!r}")
    chosen: list[tuple[Path, dict]] = []
    for path, payload in payloads:
        gate = payload.get('gate')
        if gate not in ('us', 'default'):
            gate = 'us' if is_us_gated_batch(str(payload.get('batch_key') or '')) else 'default'
        if scope == 'us' and gate != 'us':
            continue
        if scope == 'default' and gate == 'us':
            continue
        chosen.append((path, payload))

    best: dict[str, tuple[str, Path, dict, str]] = {}
    for path, payload in chosen:
        created = str(payload.get('created_at') or '')
        insert_mode = payload.get('if_exists') or 'append'
        for stream in payload.get('streams') or []:
            sid = stream.get('stream_id')
            if not sid:
                continue
            previous = best.get(sid)
            if previous is None or created >= previous[0]:
                best[sid] = (created, path, stream, insert_mode)

    source = {item[1] for item in best.values()}
    superseded = [path for path, _payload in chosen if path not in source]
    streams = []
    for _sid, (_created, _path, stream, insert_mode) in best.items():
        streams.append({**stream, 'if_exists': insert_mode})
    return DrainPlan(streams=streams, source_files=sorted(source), superseded_files=superseded)


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
