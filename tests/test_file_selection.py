"""Tests for intelligent file selection functionality."""

from collections import deque
from collections.abc import Iterator

import pytest


# Copy of the functions from balance.py to avoid Linux-specific imports
# These are the exact implementations being tested
class BufferedFileGenerator:
    """A wrapper around a file generator that supports prepending items back."""

    def __init__(self, generator: Iterator[tuple[str, int]]):
        self._generator = generator
        self._buffer: deque[tuple[str, int]] = deque()
        self._exhausted = False

    def __iter__(self) -> "BufferedFileGenerator":
        return self

    def __next__(self) -> tuple[str, int]:
        """Get the next file, from buffer first, then from generator."""
        if self._buffer:
            return self._buffer.popleft()
        if self._exhausted:
            raise StopIteration
        try:
            return next(self._generator)
        except StopIteration:
            self._exhausted = True
            raise

    def prepend(self, items: list[tuple[str, int]]) -> None:
        """Prepend items back to the front of the buffer."""
        for item in reversed(items):
            self._buffer.appendleft(item)

    @property
    def exhausted(self) -> bool:
        """Check if the underlying generator is exhausted and buffer is empty."""
        return self._exhausted and not self._buffer


def _calculate_file_score(file_size: int, bytes_to_move: int) -> float:
    """Score how well file_size matches bytes_to_move. Higher is better."""
    if file_size <= 0:
        return 0.0
    if bytes_to_move <= 0:
        return 1.0
    ratio = file_size / bytes_to_move
    if ratio <= 1.0:
        return ratio
    else:
        return 1.0 / ratio


class TestCalculateFileScore:
    """Tests for the file scoring function."""

    def test_exact_match_returns_one(self):
        """Test that exact size match returns score of 1.0."""
        assert _calculate_file_score(1000, 1000) == 1.0

    def test_half_size_returns_half(self):
        """Test that half the needed size returns score of 0.5."""
        assert _calculate_file_score(500, 1000) == 0.5

    def test_double_size_returns_half(self):
        """Test that double the needed size returns score of 0.5."""
        assert _calculate_file_score(2000, 1000) == 0.5

    def test_quarter_size_returns_quarter(self):
        """Test that quarter the needed size returns score of 0.25."""
        assert _calculate_file_score(250, 1000) == 0.25

    def test_quadruple_size_returns_quarter(self):
        """Test that quadruple the needed size returns score of 0.25."""
        assert _calculate_file_score(4000, 1000) == 0.25

    def test_zero_file_size_returns_zero(self):
        """Test that zero file size returns score of 0.0."""
        assert _calculate_file_score(0, 1000) == 0.0

    def test_negative_file_size_returns_zero(self):
        """Test that negative file size returns score of 0.0."""
        assert _calculate_file_score(-100, 1000) == 0.0

    def test_zero_bytes_to_move_returns_one(self):
        """Test that any file scores 1.0 if drive is already at target."""
        assert _calculate_file_score(1000, 0) == 1.0
        assert _calculate_file_score(1, 0) == 1.0

    def test_negative_bytes_to_move_returns_one(self):
        """Test that negative bytes_to_move (underfull drive) returns 1.0."""
        assert _calculate_file_score(1000, -500) == 1.0

    def test_large_file_small_need(self):
        """Test scoring with large file and small balance need."""
        # 1000 byte file when only 100 bytes needed = ratio 10, score 0.1
        score = _calculate_file_score(1000, 100)
        assert score == pytest.approx(0.1, rel=0.01)

    def test_small_file_large_need(self):
        """Test scoring with small file and large balance need."""
        # 100 byte file when 1000 bytes needed = ratio 0.1, score 0.1
        score = _calculate_file_score(100, 1000)
        assert score == pytest.approx(0.1, rel=0.01)


class TestBufferedFileGenerator:
    """Tests for the BufferedFileGenerator class."""

    def test_basic_iteration(self):
        """Test that basic iteration works like a normal generator."""
        items = [("file1.txt", 100), ("file2.txt", 200), ("file3.txt", 300)]
        gen = BufferedFileGenerator(iter(items))

        results = list(gen)
        assert results == items

    def test_prepend_single_item(self):
        """Test prepending a single item back to the generator."""
        items = [("file1.txt", 100), ("file2.txt", 200)]
        gen = BufferedFileGenerator(iter(items))

        # Get first item
        first = next(gen)
        assert first == ("file1.txt", 100)

        # Prepend it back
        gen.prepend([first])

        # Should get it again
        again = next(gen)
        assert again == ("file1.txt", 100)

        # Then continue with rest
        second = next(gen)
        assert second == ("file2.txt", 200)

    def test_prepend_multiple_items(self):
        """Test prepending multiple items preserves order."""
        items = [("file1.txt", 100), ("file2.txt", 200), ("file3.txt", 300)]
        gen = BufferedFileGenerator(iter(items))

        # Consume all items
        consumed = [next(gen), next(gen), next(gen)]
        assert consumed == items

        # Prepend first two back in order
        gen.prepend([("file1.txt", 100), ("file2.txt", 200)])

        # Should get them back in order
        assert next(gen) == ("file1.txt", 100)
        assert next(gen) == ("file2.txt", 200)

        # Generator should be exhausted after buffer
        with pytest.raises(StopIteration):
            next(gen)

    def test_exhausted_property(self):
        """Test the exhausted property."""
        items = [("file1.txt", 100)]
        gen = BufferedFileGenerator(iter(items))

        assert not gen.exhausted

        next(gen)
        assert not gen.exhausted  # Haven't tried to go past end yet

        with pytest.raises(StopIteration):
            next(gen)
        assert gen.exhausted

    def test_exhausted_with_buffer(self):
        """Test exhausted is False when buffer has items."""
        items = [("file1.txt", 100)]
        gen = BufferedFileGenerator(iter(items))

        # Exhaust the generator
        next(gen)
        with pytest.raises(StopIteration):
            next(gen)

        # Prepend item to buffer
        gen.prepend([("file2.txt", 200)])

        # Should not be exhausted now
        assert not gen.exhausted
        assert next(gen) == ("file2.txt", 200)

    def test_empty_generator(self):
        """Test handling of empty generator."""
        gen = BufferedFileGenerator(iter([]))

        with pytest.raises(StopIteration):
            next(gen)

        assert gen.exhausted

    def test_iter_returns_self(self):
        """Test that __iter__ returns self for use in for loops."""
        items = [("file1.txt", 100)]
        gen = BufferedFileGenerator(iter(items))

        assert iter(gen) is gen


class TestFileSelectionIntegration:
    """Integration tests for file selection behavior."""

    def test_best_file_selected_from_candidates(self):
        """Test that the best-scoring file is selected."""
        # Simulate what _find_file_to_transfer does
        candidates = [
            ("small.txt", 100, _calculate_file_score(100, 1000)),       # 0.1
            ("perfect.txt", 1000, _calculate_file_score(1000, 1000)),   # 1.0
            ("large.txt", 5000, _calculate_file_score(5000, 1000)),     # 0.2
        ]

        best_idx = max(range(len(candidates)), key=lambda i: candidates[i][2])
        best_path, best_size, best_score = candidates[best_idx]

        assert best_path == "perfect.txt"
        assert best_size == 1000
        assert best_score == 1.0

    def test_unused_candidates_can_be_reused(self):
        """Test that unused candidates are properly returned to buffer."""
        items = [
            ("file1.txt", 100),
            ("file2.txt", 500),
            ("file3.txt", 1000),
            ("file4.txt", 2000),
        ]
        gen = BufferedFileGenerator(iter(items))

        # Simulate collecting 3 candidates
        candidates = [next(gen), next(gen), next(gen)]

        # Select the middle one (best match for 500 bytes needed)
        bytes_to_move = 500
        scored = [(c[0], c[1], _calculate_file_score(c[1], bytes_to_move)) for c in candidates]
        best_idx = max(range(len(scored)), key=lambda i: scored[i][2])

        # file2.txt with 500 bytes should be best (score 1.0)
        assert scored[best_idx][0] == "file2.txt"

        # Return unused candidates
        unused = [c for i, c in enumerate(candidates) if i != best_idx]
        gen.prepend(unused)

        # Next calls should return unused candidates first
        next_file = next(gen)
        assert next_file in [("file1.txt", 100), ("file3.txt", 1000)]
