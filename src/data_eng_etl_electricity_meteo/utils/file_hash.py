"""Memory-efficient file and stream hashing utilities."""

import hashlib
from pathlib import Path

_ALGORITHM = "sha256"
_HASH_CHUNK_SIZE = 128 * 1024  # 128 KB


class FileHasher:
    """Memory-efficient file and stream hasher."""

    def __init__(self) -> None:
        self._hasher = hashlib.new(_ALGORITHM)

    def update(self, chunk: bytes) -> None:
        """Feed a chunk of data into the hash (streaming mode)."""
        self._hasher.update(chunk)

    @property
    def hexdigest(self) -> str:
        """Current hexadecimal digest."""
        return self._hasher.hexdigest()

    @staticmethod
    def hash_file(path: Path) -> str:
        """Hash a file in chunks (standalone, no instance state).

        Parameters
        ----------
        path
            File to hash.

        Returns
        -------
        str
            Hexadecimal digest of the file contents.
        """
        hasher = hashlib.new(_ALGORITHM)
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(_HASH_CHUNK_SIZE), b""):
                hasher.update(chunk)
        return hasher.hexdigest()
