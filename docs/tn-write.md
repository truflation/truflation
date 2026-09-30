# TN writes

`TNConnector.write_all` publishes an oracle tip. The on-chain `date` is the broadcast time, not the observation day. The observation day is stored beside the writer in a watermark file.

## What gets sent

The frame's newest `date` / `value` row is the tip. Pass `broadcast_history=True` to insert the full dated series instead. Those dates are left as they are.

An empty frame is not buffered. A tip that matches the watermark is not buffered. A batch file or insert therefore contains only the streams that had something new on that call. A later call that omits a stream does not erase that stream from an earlier pending file.

| Incoming tip vs watermark             | Action  |
| ------------------------------------- | ------- |
| No watermark yet                      | Publish |
| Newer observation day, any value      | Publish |
| Same observation day, different value | Publish |
| Same observation day and same value   | Skip    |

The watermark is written after the insert succeeds. It is also written when every transaction was broadcast but confirmation timed out, so a retry does not mint a second on-chain date for that tip.

Default path: `{TN_STAGE_DIR}/watermarks/{stream_id}.json`. `TN_STAGE_DIR` defaults to `trufnetwork`. `TN_WATERMARK_DIR` replaces the directory.

`if_exists` defaults to `append`. `replace` drops and recreates a stream only when that stream is actually buffered. An unchanged tip skips before the drop.

## Stage, then drain

`TN_WRITE_MODE` defaults to `live` (insert during the calc). `stage` writes `pending/*.json` and does not move the watermark. The pending record stores the value and observation day. `event_time` is stamped when a drain broadcasts it, then kept on that file so a retry uses the same timestamp.

Each staged stream records its `table`. `TNConnector.drain_pending(select)` sends the streams for which `select(batch_key, table)` returns true. The caller owns the publish schedule (transformers: `api_schedule.should_broadcast`). When a file mixes selected and rejected streams, the rejected ones are split into `pending/*_held.json` with the same `created_at`, and the rest of the file is sent.

The drain keeps the newest staged tip per stream. An older copy that shares a file with some other stream is not inserted. Older files stay in `pending/` until that insert finishes, then move to `done/`. A tip already stored in the watermark is not sent again. A file that is not valid JSON is logged and moved to `failed/`.

`if_exists=replace` applies only to the streams that were staged with `replace`.

Directories under `TN_STAGE_DIR`: `pending/`, `processing/`, `done/`, `failed/`, `watermarks/`. A crashed drain leaves files in `processing/`; the next drain moves them back to `pending/`.
