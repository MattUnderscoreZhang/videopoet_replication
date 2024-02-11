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
        url_list: str,
        url_col: str,
        caption_col: str,
        clip_col: str,
        save_additional_columns: list[str],
        number_sample_per_shard: int,
        done_shards: set,
        tmp_path: str,
        sampler: Callable[[list[tuple[int, int]]], list[tuple[int, int]]] = lambda x: x,
    ) -> None:
        self.url_col = url_col
        self.caption_col = caption_col
        self.clip_col = clip_col
        self.save_additional_columns = save_additional_columns
        self.number_sample_per_shard = number_sample_per_shard
        self.done_shards = done_shards
        self.shard_sampler = sampler

        fs, url_path = fsspec.core.url_to_fs(url_list)
        self.fs = fs
        self.tmp_path = tmp_path

        if fs.isdir(url_path):
            self.input_files = sorted(fs.glob(url_path + "/*.csv"))
            if len(self.input_files) == 0:
                raise ValueError(f"No file found at path {url_path} with extension csv")
        else:
            self.input_files = [url_path]

        self.column_list = self.save_additional_columns or []
        self.column_list += ["clips"] * bool(self.clip_col) + ["caption"] * bool(self.caption_col) + ["url"]

    def _save_to_arrow(self, input_file: str, start_shard_id: int) -> tuple[list[tuple[int, str]], int]:
        """Read the input file and save to arrow files in a temporary directory"""
        with self.fs.open(input_file, mode="rb") as file:
            data = pyarrow.Table.from_pandas(pandas.read_csv(file))

        column_names = data.column_names
        if self.caption_col is not None:
            column_names = [c if c != self.caption_col else "caption" for c in column_names]
        if self.clip_col is not None:
            column_names = [c if c != self.clip_col else "clips" for c in column_names]
        column_names = [c if c != self.url_col else "url" for c in column_names]

        data = data.rename_columns(column_names)

        number_samples = data.num_rows

        number_shards = math.ceil(data.num_rows / self.number_sample_per_shard)
        shards_to_write = [
            (start_shard_id + shard_id, shard_id)
            for shard_id in range(number_shards)
            if start_shard_id + shard_id not in self.done_shards
        ]

        shards_to_write = self.shard_sampler(shards_to_write)
        number_shards = len(shards_to_write)

        if len(shards_to_write) == 0:
            return [], number_shards

        def write_shard(t: tuple[int, int]) -> tuple[int, str]:
            full_shard_id, shard_id = t
            begin_shard = shard_id * self.number_sample_per_shard
            end_shard = min(number_samples, (1 + shard_id) * self.number_sample_per_shard)
            df_shard = data.slice(begin_shard, end_shard - begin_shard).select(self.column_list)
            tmp_file = self.tmp_path + f"/{full_shard_id}.feather"
            for i in range(10):
                try:
                    fs, tmp_path = fsspec.core.url_to_fs(tmp_file)
                    with fs.open(tmp_path, "wb") as file:
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

        return shards, number_shards

    def __iter__(self) -> tuple[str, int]:
        """
        Iterate over the shards and yield each shard.

        Each shard is a tuple (shard_id, shard).
        The shard is an Arrow file containing the data.

        Yields:
            tuple[str, int]: A tuple containing the Arrow file and the shard ID.
        """
        start_shard_id = 0
        for i, input_file in enumerate(self.input_files):
            print(f"Sharding file number {i + 1} of {len(self.input_files)} called {input_file}")

            shards, number_shards = self._save_to_arrow(input_file, start_shard_id)
            print(f"File sharded into {len(shards)} shards")
            print(
                "Downloading starting now, check your bandwidth speed (with bwm-ng),"
                "your CPU usage (with htop), and your disk usage (with iotop)!"
            )

            for shard_id, arrow_file in shards:
                yield arrow_file, shard_id
            start_shard_id += number_shards