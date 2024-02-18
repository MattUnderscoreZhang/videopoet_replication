import hashlib
from typing import Callable


def calculate_file_hash(
    filepath: str,
    hash_func: Callable = hashlib.sha256,
):
    hash_obj = hash_func()
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_obj.update(chunk)
    return hash_obj.hexdigest()
