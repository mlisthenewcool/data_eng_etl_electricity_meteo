"""Memory-efficient file and stream hashing utilities."""

import hashlib
from pathlib import Path

from data_eng_etl_electricity_meteo.core.settings import settings

__all__: list[str] = ["FileHasher"]


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

    @staticmethod
    def hash_file(
        path: Path,
        algorithm: str = settings.hash_algorithm,
        chunk_size: int = settings.hash_chunk_size,
    ) -> str:
        """Hash a file in chunks (standalone, no instance state).

        Parameters
        ----------
        path:
            File to hash.
        algorithm:
            Hash algorithm (default from settings).
        chunk_size:
            Read buffer size in bytes (default from settings).

        Returns
        -------
        str
            Hexadecimal digest of the file contents.
        """
        hasher = hashlib.new(algorithm)
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(chunk_size), b""):
                hasher.update(chunk)
        return hasher.hexdigest()
