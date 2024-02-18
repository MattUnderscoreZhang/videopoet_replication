import fsspec
import math
from multiprocessing.pool import ThreadPool
import pandas
import pyarrow
from pyarrow import parquet, csv as pa_csv
import time
from typing import Callable, Union


class URLShardGeneratorFromCSV:
    """
    The URLShardGenerator class reads a list of URLs and returns shards.
    It provides an iterator method (__iter__) to iterate over the shards.
    """

    def __init__(
        self,
        csv_url: str,  # can be either file path or directory path
        csv_url_col: str,  # name of the column containing video URLs
        n_samples_per_shard: int,  # number of sample IDs in each shard file
        processed_shard_ids: set[int],  # skip these IDs during sharding
        shard_dir: str,  # directory to write shard files to
    ) -> None:
        csv_filesystem, csv_filepath = fsspec.core.url_to_fs(csv_url)
        self.csv_filesystem = csv_filesystem
        self.csv_filepaths = (
            [csv_filepath]
            if not csv_filesystem.isdir(csv_filepath)
            else sorted(csv_filesystem.glob(csv_filepath + "/*.csv"))
        )
        if len(self.csv_filepaths) == 0:
            raise ValueError(f"No .csv files found for path {csv_filepath}")

        self.csv_url_col = csv_url_col
        self.n_samples_per_shard = n_samples_per_shard
        self.processed_shard_ids = processed_shard_ids
        self.shard_dir = shard_dir

        self.column_list = ["url"]
    
    def _get_and_format_data_from_csv_file(self, csv_filepath: str) -> None:
        """Extract data from a CSV file, rename the columns, and return data."""
        with self.csv_filesystem.open(csv_filepath, mode="rb") as file:
            data = pyarrow.Table.from_pandas(pandas.read_csv(file))

        # rename self.csv_url_col to "url"
        return data.rename_columns(
            [
                "url"
                if column_name == self.csv_url_col
                else column_name
                for column_name in data.column_names
            ]
        )

    def _save_to_arrow(self, csv_filepath: str, start_shard_id: int) -> tuple[list[tuple[int, str]], int]:
        """Read the input file and save to arrow files in a temporary directory"""
        data = self._get_and_format_data_from_csv_file(csv_filepath)
        number_samples = data.num_rows

        n_shards = math.ceil(data.num_rows / self.n_samples_per_shard)
        shards_to_write = [
            (start_shard_id + shard_id, shard_id)
            for shard_id in range(n_shards)
            if start_shard_id + shard_id not in self.processed_shard_ids
        ]

        n_shards = len(shards_to_write)

        if len(shards_to_write) == 0:
            return [], n_shards

        def write_shard(t: tuple[int, int]) -> tuple[int, str]:
            full_shard_id, shard_id = t
            begin_shard = shard_id * self.n_samples_per_shard
            end_shard = min(number_samples, (1 + shard_id) * self.n_samples_per_shard)
            df_shard = data.slice(begin_shard, end_shard - begin_shard).select(self.column_list)
            tmp_file = self.shard_dir + f"/{full_shard_id}.feather"
            for i in range(10):
                try:
                    csv_filesystem, shard_dir = fsspec.core.url_to_fs(tmp_file)
                    with csv_filesystem.open(shard_dir, "wb") as file:
                        with pyarrow.ipc.new_file(file, df_shard.schema) as writer:
                            writer.write_table(df_shard)
                    return (full_shard_id, tmp_file)
                except Exception as e:  # pylint: disable=broad-except
                    if i != 9:
                        print("retrying to write to file due to error:", e)
                        time.sleep(1)
                    else:
                        raise e
            # can't reach here
            raise ValueError("Failed to write to file.")

        for i in range(10):
            shards = []
            # thread pool to make it faster to write files to low latency file systems (ie s3, hdfs)
            try:
                with ThreadPool(32) as thread_pool:
                    for shard in thread_pool.imap_unordered(write_shard, shards_to_write):
                        shards.append(shard)
                break
            except Exception as e:  # pylint: disable=broad-except
                if i != 9:
                    print("retrying whole sharding to write to files due to error:", e)
                    time.sleep(2 * i)
                else:
                    raise e

        shards.sort(key=lambda k: k[0])

        del data

        return shards, n_shards

    def __iter__(self) -> tuple[str, int]:
        """
        Iterate over the shards and yield each shard.

        Each shard is a tuple (shard_id, shard).
        The shard is an Arrow file containing the data.

        Yields:
            tuple[str, int]: A tuple containing the Arrow file and the shard ID.
        """
        start_shard_id = 0
        for i, csv_filepath in enumerate(self.csv_filepaths):
            print(f"Sharding file number {i + 1} of {len(self.csv_filepaths)} called {csv_filepath}")

            shards, n_shards = self._save_to_arrow(csv_filepath, start_shard_id)
            print(f"File sharded into {len(shards)} shards")
            print(
                "Downloading starting now, check your bandwidth speed (with bwm-ng),"
                "your CPU usage (with htop), and your disk usage (with iotop)!"
            )

            for shard_id, arrow_file in shards:
                yield arrow_file, shard_id
            start_shard_id += n_shards
