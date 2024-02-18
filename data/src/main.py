import atexit
import os
import tempfile

from .distributor import multiprocessing_distributor
from .shard_generator import URLShardGeneratorFromCSV


def create_temp_dir() -> str:
    """Create a temporary directory and register it for cleanup on program exit."""

    temp_dir = tempfile.mkdtemp()

    # Deletes temp_dir if program terminates
    def cleanup_temp_dir() -> None:
        print("Cleaning up temporary directory")
        os.rmdir(temp_dir)

    # Register cleanup_temp_dir to be called when program exits for any reason
    atexit.register(cleanup_temp_dir)

    return temp_dir


if __name__ == "__main__":
    temp_dir = create_temp_dir()

    shard_generator = URLShardGeneratorFromCSV(
        csv_url,
        csv_url_col="url",
        n_samples_per_shard=100,
        processed_shard_ids,
        tmp_dir,
    )

    multiprocessing_distributor(
        processes_count=16,
        worker,
        shard_iterator,
        max_shard_retry=1,
    )
