"""
in-memory key-value store with async-safe access.
provides thread-safe operations for the distributed system.
supports both basic (last-writer-wins) and versioned storage modes.
"""

import asyncio
from typing import Any


class KeyValueStore:
    """
    thread-safe in-memory key-value store.
    uses asyncio.lock for safe concurrent access.

    supports two modes:
    - basic: simple key-value storage (last-writer-wins)
    - versioned: key-value with version tracking (rejects stale writes)
    """

    def __init__(self) -> None:
        """initialize empty store with lock."""
        self._data: dict[str, Any] = {}
        self._versions: dict[str, int] = {}  # per-key version tracking
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Any | None:
        """
        retrieve value for given key.

        args:
            key: the key to look up

        returns:
            the value if found, none otherwise
        """
        async with self._lock:
            return self._data.get(key)

    async def get_version(self, key: str) -> int:
        """
        retrieve version for given key.

        args:
            key: the key to look up

        returns:
            the version if found, 0 otherwise
        """
        async with self._lock:
            return self._versions.get(key, 0)

    async def set(self, key: str, value: Any) -> None:
        """
        store value for given key (basic mode - last-writer-wins).

        args:
            key: the key to store
            value: the value to associate with key
        """
        async with self._lock:
            self._data[key] = value

    async def set_versioned(self, key: str, value: Any, version: int) -> bool:
        """
        store value only if version > current version (versioned mode).

        implements optimistic locking - rejects stale writes silently.
        this handles out-of-order message delivery correctly:
        if a newer write arrives first, older writes are automatically rejected.

        args:
            key: the key to store
            value: the value to associate with key
            version: the version number of this write

        returns:
            true if write was accepted (version > current)
            false if write was rejected (version <= current)
        """
        async with self._lock:
            current_version = self._versions.get(key, 0)
            if version > current_version:
                self._data[key] = value
                self._versions[key] = version
                return True
            return False  # silently reject stale write

    async def delete(self, key: str) -> bool:
        """
        delete key from store.

        args:
            key: the key to delete

        returns:
            true if key existed and was deleted, false otherwise
        """
        async with self._lock:
            if key in self._data:
                del self._data[key]
                if key in self._versions:
                    del self._versions[key]
                return True
            return False

    async def get_all(self) -> dict[str, Any]:
        """
        return copy of all stored data.

        returns:
            dictionary containing all key-value pairs
        """
        async with self._lock:
            return self._data.copy()

    async def get_all_with_versions(self) -> dict[str, dict[str, Any]]:
        """
        return copy of all stored data with versions.

        returns:
            dictionary containing all key-value-version tuples
        """
        async with self._lock:
            result = {}
            for key, value in self._data.items():
                result[key] = {
                    "value": value,
                    "version": self._versions.get(key, 0),
                }
            return result

    async def keys(self) -> list[str]:
        """
        return list of all keys.

        returns:
            list of keys in the store
        """
        async with self._lock:
            return list(self._data.keys())

    async def clear(self) -> None:
        """clear all data from store."""
        async with self._lock:
            self._data.clear()
            self._versions.clear()

    async def size(self) -> int:
        """
        return number of items in store.

        returns:
            count of key-value pairs
        """
        async with self._lock:
            return len(self._data)


# global store instance
store = KeyValueStore()
