import atexit
import os
import tempfile

from .distributor import multiprocessing_distributor
from .shard_generator import URLShardGeneratorFromCSV


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
    shard_generator = URLShardGeneratorFromCSV(
        url_list,
        url_col="url",
        caption_col=None,
        clip_col=None,
        save_additional_columns=None,
        config["storage"]["number_sample_per_shard"],
        done_shards,
        tmp_path,
        config["reading"]["sampler"],
    )

    multiprocessing_distributor(
        processes_count=16,
        worker,
        shard_iterator,
        max_shard_retry=1,
    )