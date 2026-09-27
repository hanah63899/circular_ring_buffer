# Circular Ring Buffer

A fixed-capacity in-memory ring buffer that stores the last N log records and lets you query them by level and time range. Standard library only.

## Usage

```python
from circular_ring_buffer import RingBuffer

# Pass a clock so timestamps are deterministic in tests; omit it and supply
# your own (e.g. time.time) in production.
rb = RingBuffer(capacity=1000, clock=lambda: 0.0)

rb.add(level=10, message="starting up")
rb.add(level=30, message="disk almost full", data={"free_mb": 12})
rb.add_at(timestamp=5.0, level=40, message="disk full")

# All records, oldest first.
for rec in rb.all():
    print(rec.timestamp, rec.level, rec.message, rec.data)

# WARN (level >= 30) or worse, within a time window.
for rec in rb.query_range(start=0.0, end=10.0, min_level=30):
    print(rec.message)

rb.clear()
```

The package exports `RingBuffer` and `Record`.

## Why

When something goes wrong in a long-running process the logs you want are the ones just before the failure, but you don't want to keep every log forever. This buffer holds the last N records and drops the oldest when full. The intended use is incident triage: "show me what WARN-or-worse was logged in the last 30 seconds". It is not a durable log, not a structured-logging framework, and not a replacement for shipping logs to a collector — it is the small in-memory scratchpad you read when reproducing an incident.

The trade-off: queries are a linear scan of at most `capacity` records. For a few-thousand-record window that is fine and avoids the cost of maintaining secondary indexes that would rarely be queried.

## Edge cases

- Capacity must be a positive `int`; `RingBuffer(0)` raises `ValueError`.
- `level` must be an `int`; `bool` is rejected even though it is a subclass of `int`, because passing `True` as a level is almost always a bug.
- `data` must be a `dict` or `None`.
- `query_range(start, end)` is inclusive on both ends; `start > end` raises `ValueError` rather than returning an empty list, so a logic error in the caller is not silently swallowed.
- Timestamps come from the `clock` callable passed to the constructor. The default clock returns `0.0` so behavior is deterministic unless you choose otherwise. To use real wall-clock time, pass `clock=time.time`.
- `Record.__hash__` deliberately excludes `data` (a mutable dict); records that differ only in `data` will hash the same. Do not put records in a set expecting `data` to distinguish them.
