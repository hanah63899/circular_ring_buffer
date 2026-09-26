"""Fixed-capacity circular ring buffer for recent log records."""

from collections import deque
from typing import Callable, List, Optional


class Record:
    """A single log record stored in a RingBuffer.

    Levels are compared by their integer ``level`` field, not by name, so
    callers can pass any string ("DEBUG", "INFO", "WARN", "ERROR", "FATAL",
    or a custom label) and the only requirement is that the caller picks
    numeric ``level`` values consistently.
    """

    __slots__ = ("timestamp", "level", "message", "data")

    def __init__(
        self,
        timestamp: float,
        level: int,
        message: str,
        data: Optional[dict] = None,
    ) -> None:
        self.timestamp = timestamp
        self.level = level
        self.message = message
        self.data = data

    def __repr__(self) -> str:
        return (
            f"Record(timestamp={self.timestamp!r}, level={self.level!r}, "
            f"message={self.message!r}, data={self.data!r})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Record):
            return NotImplemented
        return (
            self.timestamp == other.timestamp
            and self.level == other.level
            and self.message == other.message
            and self.data == other.data
        )

    def __hash__(self) -> int:
        # Records are mutable in spirit (data is a dict), but exposing a hash
        # makes integration with sets/dicts easier when callers choose to use
        # records immutably. We deliberately hash on the tuple of the
        # immutable scalar fields and accept that data is not part of it; a
        # full-strength value object would demand frozen data, which is
        # incompatible with the ergonomics of passing plain dicts in.
        return hash((self.timestamp, self.level, self.message))


class RingBuffer:
    """Fixed-capacity ring buffer for the last N log records.

    Records are kept in insertion order. When capacity is exceeded the oldest
    record is dropped (the buffer never grows). Query methods (``query`` and
    ``query_range``) scan the buffer linearly; for the intended use-case — a
    small in-memory window of recent records surfaced after an incident — a
    linear scan of at most ``capacity`` records is the right trade-off: it
    avoids the bookkeeping cost of maintaining auxiliary indexes that would
    rarely be exercised.

    Time semantics are delegated to an injectable ``clock`` callable. This
    makes the buffer deterministic in tests and lets the caller pin down what
    "now" means (real wall clock, a monotonic clock, or a fake). We deliberately
    do not call ``time.time()`` ourselves.

    Timestamps are treated as opaque ordered values. While the common case is
    numeric epoch time, we avoid assumptions about units or epoch so that
    callers can use whatever timeline they prefer.
    """

    def __init__(
        self,
        capacity: int,
        clock: Callable[[], float] = lambda: 0.0,
    ) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be a positive integer")
        self._capacity = capacity
        self._records: deque = deque(maxlen=capacity)
        self._clock = clock

    @property
    def capacity(self) -> int:
        return self._capacity

    def __len__(self) -> int:
        return len(self._records)

    def __iter__(self):
        """Iterate in insertion order (oldest first)."""
        return iter(self._records)

    def add(self, level: int, message: str, data: Optional[dict] = None) -> Record:
        """Append a record, dropping the oldest if at capacity.

        Raises TypeError if level or message have the wrong type. Timestamps
        come from the configured clock; callers wanting a specific timestamp
        should use ``add_at``.
        """
        return self._add(self._clock(), level, message, data)

    def add_at(
        self,
        timestamp: float,
        level: int,
        message: str,
        data: Optional[dict] = None,
    ) -> Record:
        """Append a record with an explicit timestamp.

        Useful when replaying records from an external source whose timestamps
        must be preserved verbatim rather than re-stamped by the buffer's clock.
        """
        return self._add(timestamp, level, message, data)

    def _add(self, ts, level, message, data) -> Record:
        if not isinstance(level, int) or isinstance(level, bool):
            raise TypeError("level must be an int")
        if not isinstance(message, str):
            raise TypeError("message must be a str")
        if data is not None and not isinstance(data, dict):
            raise TypeError("data must be a dict or None")
        rec = Record(ts, level, message, data)
        self._records.append(rec)
        return rec

    def all(self) -> List[Record]:
        """Return all records in insertion order (oldest first)."""
        return list(self._records)

    def query(self, min_level: Optional[int] = None) -> List[Record]:
        """Return records with ``level >= min_level``, in insertion order.

        If ``min_level`` is None, returns all records (equivalent to ``all()``).
        We chose a single ``min_level`` filter (rather than also supporting
        ``max_level``) because the typical question after an incident is "what
        was logged at WARN or worse"; adding max_level would expand the API
        surface for a query shape we don't expect callers to need.
        """
        if min_level is None:
            return list(self._records)
        if not isinstance(min_level, int) or isinstance(min_level, bool):
            raise TypeError("min_level must be an int or None")
        return [r for r in self._records if r.level >= min_level]

    def query_range(
        self,
        start: Optional[float] = None,
        end: Optional[float] = None,
        min_level: Optional[int] = None,
    ) -> List[Record]:
        """Return records within ``[start, end]`` optionally filtered by level.

        The range is inclusive on both ends: a record whose timestamp equals
        ``start`` or ``end`` is included. Omitting ``start`` means "no lower
        bound"; omitting ``end`` means "no upper bound". If both are omitted
        the time filter is a no-op and only ``min_level`` (if any) applies.

        Raises ValueError if both bounds are given and ``start > end``. We treat
        an inverted range as a programmer error rather than silently returning
        an empty list, because the "empty result" interpretation is
        indistinguishable from a genuinely empty window and hides bugs.
        """
        if start is not None and end is not None and start > end:
            raise ValueError("start must not be greater than end")
        if min_level is None:
            return [
                r
                for r in self._records
                if (start is None or r.timestamp >= start)
                and (end is None or r.timestamp <= end)
            ]
        if not isinstance(min_level, int) or isinstance(min_level, bool):
            raise TypeError("min_level must be an int or None")
        return [
            r
            for r in self._records
            if r.level >= min_level
            and (start is None or r.timestamp >= start)
            and (end is None or r.timestamp <= end)
        ]

    def clear(self) -> None:
        """Remove all records without changing capacity."""
        self._records.clear()
