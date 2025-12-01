"""
in-memory key-value store with async-safe access.
provides thread-safe operations for the distributed system.
"""

import asyncio
from typing import Any


class KeyValueStore:
    """
    thread-safe in-memory key-value store.
    uses asyncio.lock for safe concurrent access.
    """

    def __init__(self) -> None:
        """initialize empty store with lock."""
        self._data: dict[str, Any] = {}
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

    async def set(self, key: str, value: Any) -> None:
        """
        store value for given key.

        args:
            key: the key to store
            value: the value to associate with key
        """
        async with self._lock:
            self._data[key] = value

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
