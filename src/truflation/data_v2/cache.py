from typing import Any

class Cache:
    def __init__(self) -> None:
        self._cache_data = {}

    def write_to_cache(self, key: str, value: Any) -> None:
        """Write data to the cache with the specified key."""
        self._cache_data[key] = value

    def read_from_cache(self, key: str) -> Any:
        """Read data from the cache using the specified key."""
        return self._cache_data.get(key)

    def clear_cache(self) -> None:
        """Clear all data from the cache."""
        self._cache_data.clear()