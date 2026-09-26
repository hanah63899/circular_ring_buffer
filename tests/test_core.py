import unittest

from circular_ring_buffer import Record, RingBuffer


class TestConstruction(unittest.TestCase):
    def test_default_capacity_must_be_positive(self):
        with self.assertRaises(ValueError):
            RingBuffer(0)

    def test_negative_capacity_rejected(self):
        with self.assertRaises(ValueError):
            RingBuffer(-3)

    def test_capacity_is_int_not_coerced(self):
        with self.assertRaises(TypeError):
            RingBuffer("10")

    def test_records_attribute_is_separate_per_instance(self):
        a = RingBuffer(3)
        b = RingBuffer(3)
        a.add(level=1, message="a")
        self.assertEqual(len(b), 0)


class TestAdd(unittest.TestCase):
    def test_add_returns_record_with_clock_timestamp(self):
        calls = []

        def fake_clock():
            calls.append(len(calls) + 1)
            return float(calls[-1])

        rb = RingBuffer(5, clock=fake_clock)
        rec = rb.add(level=10, message="hi")
        self.assertEqual(rec.timestamp, 1.0)
        self.assertEqual(rec.level, 10)
        self.assertEqual(rec.message, "hi")
        self.assertIsNone(rec.data)

    def test_add_stores_data_dict(self):
        rb = RingBuffer(5, clock=lambda: 42.0)
        rec = rb.add(level=20, message="boom", data={"k": 7})
        self.assertEqual(rec.data, {"k": 7})

    def test_add_rejects_non_int_level(self):
        rb = RingBuffer(3)
        with self.assertRaises(TypeError):
            rb.add(level="INFO", message="x")

    def test_add_rejects_bool_level(self):
        # bool is a subclass of int in Python; we treat it as wrong type.
        rb = RingBuffer(3)
        with self.assertRaises(TypeError):
            rb.add(level=True, message="x")

    def test_add_rejects_non_str_message(self):
        rb = RingBuffer(3)
        with self.assertRaises(TypeError):
            rb.add(level=1, message=42)

    def test_add_rejects_non_dict_data(self):
        rb = RingBuffer(3)
        with self.assertRaises(TypeError):
            rb.add(level=1, message="x", data=["no"])

    def test_add_at_uses_explicit_timestamp(self):
        rb = RingBuffer(5, clock=lambda: 999.0)
        rec = rb.add_at(timestamp=5.0, level=1, message="past")
        self.assertEqual(rec.timestamp, 5.0)


class TestCapacity(unittest.TestCase):
    def test_drops_oldest_when_full(self):
        rb = RingBuffer(2)
        rb.add(level=1, message="first")
        rb.add(level=2, message="second")
        rb.add(level=3, message="third")
        msgs = [r.message for r in rb.all()]
        self.assertEqual(msgs, ["second", "third"])

    def test_clear_empties_without_changing_capacity(self):
        rb = RingBuffer(3)
        rb.add(level=1, message="a")
        rb.add(level=2, message="b")
        rb.clear()
        self.assertEqual(len(rb), 0)
        self.assertEqual(rb.capacity, 3)
        rb.add(level=3, message="c")
        self.assertEqual(len(rb), 1)

    def test_iteration_is_oldest_first(self):
        rb = RingBuffer(5)
        for i in range(4):
            rb.add(level=i, message=str(i))
        self.assertEqual([r.message for r in rb], ["0", "1", "2", "3"])


class TestQuery(unittest.TestCase):
    def test_query_by_level_filters_ge(self):
        rb = RingBuffer(10)
        rb.add(level=10, message="debug")
        rb.add(level=30, message="warn")
        rb.add(level=20, message="info")
        rb.add(level=40, message="error")
        got = [r.message for r in rb.query(min_level=30)]
        self.assertEqual(got, ["warn", "error"])

    def test_query_none_min_level_returns_all(self):
        rb = RingBuffer(10)
        rb.add(level=10, message="a")
        rb.add(level=40, message="b")
        self.assertEqual(len(rb.query()), 2)

    def test_query_rejects_bool_min_level(self):
        rb = RingBuffer(3)
        with self.assertRaises(TypeError):
            rb.query(min_level=True)

    def test_query_preserves_insertion_order(self):
        rb = RingBuffer(10)
        rb.add(level=10, message="d")
        rb.add(level=30, message="w1")
        rb.add(level=30, message="w2")
        rb.add(level=10, message="d2")
        self.assertEqual([r.message for r in rb.query(min_level=30)], ["w1", "w2"])


class TestQueryRange(unittest.TestCase):
    def test_inclusive_on_both_ends(self):
        rb = RingBuffer(10)
        for t in (1.0, 2.0, 3.0, 4.0, 5.0):
            rb.add_at(timestamp=t, level=10, message=str(t))
        got = [r.timestamp for r in rb.query_range(start=2.0, end=4.0)]
        self.assertEqual(got, [2.0, 3.0, 4.0])

    def test_open_ended_lower(self):
        rb = RingBuffer(10)
        for t in (1.0, 2.0, 3.0):
            rb.add_at(timestamp=t, level=10, message=str(t))
        got = [r.timestamp for r in rb.query_range(end=2.0)]
        self.assertEqual(got, [1.0, 2.0])

    def test_open_ended_upper(self):
        rb = RingBuffer(10)
        for t in (1.0, 2.0, 3.0):
            rb.add_at(timestamp=t, level=10, message=str(t))
        got = [r.timestamp for r in rb.query_range(start=2.0)]
        self.assertEqual(got, [2.0, 3.0])

    def test_inverted_range_raises(self):
        rb = RingBuffer(10)
        with self.assertRaises(ValueError):
            rb.query_range(start=5.0, end=1.0)

    def test_combined_time_and_level(self):
        rb = RingBuffer(10)
        rb.add_at(timestamp=1.0, level=10, message="d1")
        rb.add_at(timestamp=2.0, level=30, message="w1")
        rb.add_at(timestamp=3.0, level=20, message="i1")
        rb.add_at(timestamp=4.0, level=30, message="w2")
        got = [r.message for r in rb.query_range(start=2.0, end=4.0, min_level=30)]
        self.assertEqual(got, ["w1", "w2"])

    def test_empty_buffer_returns_empty_list(self):
        rb = RingBuffer(5)
        self.assertEqual(rb.query_range(start=0.0, end=10.0), [])

    def test_no_bounds_no_level_returns_copy(self):
        rb = RingBuffer(5)
        rb.add(level=10, message="a")
        out = rb.query_range()
        self.assertEqual(out, rb.all())
        self.assertIsNot(out, rb.all())


class TestRecord(unittest.TestCase):
    def test_equality_compares_fields(self):
        a = Record(1.0, 20, "x", {"k": 1})
        b = Record(1.0, 20, "x", {"k": 1})
        self.assertEqual(a, b)

    def test_inequality_on_message(self):
        a = Record(1.0, 20, "x")
        b = Record(1.0, 20, "y")
        self.assertNotEqual(a, b)

    def test_inequality_on_data(self):
        a = Record(1.0, 20, "x", {"k": 1})
        b = Record(1.0, 20, "x", {"k": 2})
        self.assertNotEqual(a, b)

    def test_eq_against_non_record_returns_notimplemented(self):
        r = Record(1.0, 20, "x")
        self.assertNotEqual(r, 42)
        self.assertNotEqual(r, "x")

    def test_repr_contains_fields(self):
        r = Record(1.0, 20, "hi", {"k": 1})
        s = repr(r)
        self.assertIn("Record(", s)
        self.assertIn("1.0", s)
        self.assertIn("20", s)
        self.assertIn("'hi'", s)

    def test_hash_excludes_data(self):
        # Records with same scalar fields but different data hash equal.
        a = Record(1.0, 20, "x", {"k": 1})
        b = Record(1.0, 20, "x", {"k": 2})
        self.assertEqual(hash(a), hash(b))


if __name__ == "__main__":
    unittest.main()
