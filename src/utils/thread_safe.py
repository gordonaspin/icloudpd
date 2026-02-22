"""Thread-safe container implementations.

Provides lightweight thread-safe wrappers around dict, list, and set
behaviors used elsewhere in the project. These wrappers use reentrant locks
to ensure safe concurrent access and support context-manager locking for
batch operations.
"""
from threading import RLock
from collections import UserDict
from typing import Union, Any, Iterator, Tuple, Iterable, Set

class ThreadSafeDict(UserDict):
    """
    A thread-safe dict using RLock that accepts str or Path objects 
    as indices by normalizing them to a standard string representation.
    """

    def __init__(self, *args, **kwargs):
        self._lock = RLock()
        super().__init__(*args, **kwargs)

    def _normalize(self, key: str) -> str:
        return str(key)

    # accessor methods
    def get(self, key, default=None):
        with self._lock:
            return super().get(self._normalize(key), default)

    def pop(self, key, default=None):
        with self._lock:
            return super().pop(self._normalize(key), default)

    def update(self, other=None, **kwargs):  # pylint: disable=arguments-differ
        """Update mapping with another mapping or iterable and/or keyword args.
        Uses the same signature as the built-in `dict.update(other=None, **kwargs)`
        to avoid Pylint W0221 (arguments-differ) when overriding.
        """
        with self._lock:
            if other is None:
                # Only keyword args provided
                super().update(**kwargs)
            else:
                super().update(other, **kwargs)

    def clear(self):
        with self._lock:
            super().clear()

    # --- Context Manager Methods ---
    def __enter__(self):
        """Allows 'with d:' to hold a lock for atomic multi-step operations."""
        self._lock.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._lock.release()

    # --- Thread-Safe Accessors ---
    def __getitem__(self, key: str) -> Any:
        with self._lock:
            return super().__getitem__(self._normalize(key))

    def __setitem__(self, key: str, value: Any) -> None:
        with self._lock:
            super().__setitem__(self._normalize(key), value)

    def __delitem__(self, key: str) -> None:
        with self._lock:
            super().__delitem__(self._normalize(key))

    def __contains__(self, key):
        with self._lock:
            return super().__contains__(self._normalize(key))

    def __len__(self):
        with self._lock:
            return super().__len__()

    def unsafe_len(self):
        """Return the length without acquiring the lock (fast, potentially racy)."""
        return super().__len__()

    # --- Iterator Methods (Snapshotting) ---
    def __iter__(self) -> Iterator[str]:
        """Iterates over a copy of keys to remain thread-safe."""
        with self._lock:
            return iter(list(self.data.keys()))

    def keys(self):
        with self._lock:
            # Returns a snapshot set to support set operations
            return set(self.data.keys())

    def values(self) -> Iterator[Any]:
        with self._lock:
            return iter(list(self.data.values()))

    def items(self) -> Iterator[Tuple[str, Any]]:
        with self._lock:
            return iter(list(self.data.items()))

     # --- Set Operation Support ---
    def __or__(self, other: Union[dict, 'ThreadSafeDict']) -> 'ThreadSafeDict':
        """Union: Returns a NEW dictionary containing keys from both."""
        with self._lock:
            # Create a copy and update it with the other mapping
            new_dict = self.__class__(self.data)
            new_dict.update(other)
            return new_dict

    def __and__(self, other: Iterable[Any]) -> Set[str]:
        """Intersection: Returns a set of keys present in both."""
        # Normalize the other keys first
        other_keys = {self._normalize(k) for k in other}
        with self._lock:
            return set(self.data.keys()) & other_keys

    def __sub__(self, other: Iterable[Any]) -> Set[str]:
        """Difference: Returns a set of keys in self but NOT in other."""
        other_keys = {self._normalize(k) for k in other}
        with self._lock:
            return set(self.data.keys()) - other_keys

    def __xor__(self, other: Iterable[Any]) -> Set[str]:
        """Symmetric Difference: Keys in either self or other, but not both."""
        other_keys = {self._normalize(k) for k in other}
        with self._lock:
            return set(self.data.keys()) ^ other_keys

    def __repr__(self):
        with self._lock:
            return f"ThreadSafeDict({super().__repr__()})"

def thread_safe_dict_to_dict_recursive(d_object):
    """
    Recursively converts a collections.UserDict and its nested contents to a dict.
    """
    if isinstance(d_object, ThreadSafeDict):
        # Access the underlying dictionary using the .data attribute
        d_object = d_object.data

    if isinstance(d_object, dict):
        # Recursively process each value in the dictionary
        return {k: thread_safe_dict_to_dict_recursive(v) for k, v in d_object.items()}

    if isinstance(d_object, list):
        # Recursively process each item in a list
        return [thread_safe_dict_to_dict_recursive(i) for i in d_object]

    # Return the object if it is not a dict or list (base case)
    return d_object
