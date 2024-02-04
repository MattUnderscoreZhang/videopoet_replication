import os
import signal
import tempfile
from typing import Any, Callable


def create_temp_dir() -> tuple[str, Callable]:
    temp_dir = tempfile.mkdtemp()

    def cleanup_temp_dir(signum: int, frame: Any) -> None:
        """
        Signal handler that cleans up the temporary directory.
        This function gets called when the program receives specific signals.
        """
        print(f"Received signal {signum}, cleaning up temporary directory...")
        os.rmdir(temp_dir)
        exit(0)

    return temp_dir, cleanup_temp_dir


if __name__ == "__main__":
    # Create a temporary directory
    temp_dir, cleanup_temp_dir = create_temp_dir()

    # Register the signal handler for SIGINT (Ctrl+C) and SIGTERM (termination request)
    signal.signal(signal.SIGINT, cleanup_temp_dir)
    signal.signal(signal.SIGTERM, cleanup_temp_dir)

    # Your main program logic here
    ...