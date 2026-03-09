"""Unit tests for the file_hash module."""

from pathlib import Path

from data_eng_etl_electricity_meteo.utils.file_hash import FileHasher

# SHA-256 of empty input.
_EMPTY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"


# --------------------------------------------------------------------------------------
# FileHasher
# --------------------------------------------------------------------------------------


class TestFileHasher:
    def test_streaming_matches_file_hash(self, tmp_path: Path) -> None:
        content = b"hello world" * 1000
        path = tmp_path / "test.bin"
        path.write_bytes(content)

        hasher = FileHasher()
        hasher.update(content)

        assert hasher.hexdigest == FileHasher.hash_file(path)

    def test_empty_file_hash(self, tmp_path: Path) -> None:
        path = tmp_path / "empty.bin"
        path.write_bytes(b"")
        assert FileHasher.hash_file(path) == _EMPTY_SHA256

    def test_chunked_streaming_matches_single_update(self) -> None:
        content = b"abcdefghij" * 500

        single = FileHasher()
        single.update(content)

        chunked = FileHasher()
        for i in range(0, len(content), 100):
            chunked.update(content[i : i + 100])

        assert single.hexdigest == chunked.hexdigest
