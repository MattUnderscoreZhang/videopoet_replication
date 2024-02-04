import atexit
import os
import tempfile
from typing import Any


def create_temp_dir() -> str:
    temp_dir = tempfile.mkdtemp()

    # Deletes temp_dir if program terminates
    def cleanup_temp_dir() -> None:
        """Deletes the temporary directory. """
        print("Cleaning up temporary directory")
        os.rmdir(temp_dir)

    # Register cleanup_temp_dir to be called when program exits for any reason
    atexit.register(cleanup_temp_dir)

    return temp_dir


if __name__ == "__main__":
    # Create a temporary directory
    temp_dir = create_temp_dir()

    # Your main program logic here
    ...