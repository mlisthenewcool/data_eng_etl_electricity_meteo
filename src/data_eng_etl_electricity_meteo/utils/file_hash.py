"""Memory-efficient file and stream hashing utilities."""

import hashlib
from pathlib import Path

from data_eng_etl_electricity_meteo.core.settings import settings


class FileHasher:
    """Memory-efficient file and stream hasher."""

    def __init__(self, algorithm: str = settings.hash_algorithm):
        self._hasher = hashlib.new(algorithm)
        self.algorithm = algorithm

    def update(self, chunk: bytes) -> None:
        """Feed a chunk of data into the hash (streaming mode)."""
        self._hasher.update(chunk)

    @property
    def hexdigest(self) -> str:
        """Current hexadecimal digest."""
        return self._hasher.hexdigest()

    def hash_file(
        self,
        path: Path,
        chunk_size: int = settings.hash_chunk_size,
    ) -> str:
        """Read and hash a file in chunks. Returns hex digest."""
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(chunk_size), b""):
                self.update(chunk)
        return self.hexdigest
