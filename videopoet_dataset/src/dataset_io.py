from typing import Dict, Any
import atexit
import fsspec
import json
import numpy as np
import pyarrow
from pyarrow import parquet
import webdataset


class BufferedParquetWriter:
    """
    Writes sample metadata to a parquet file incrementally with a buffer.
    Stores metadata in memory until buffer fills up, then flushes to disk.
    This reduces the number of I/O operations required.
    """

    def __init__(self, output_file: str, schema: pyarrow.Schema, buffer_size: int = 100) -> None:
        """Initialize the BufferedParquetWriter.

        Args:
            output_file (str): The output file path.
            schema (pyarrow.Schema): The schema for the parquet file.
            buffer_size (int, optional): The buffer size. Defaults to 100.
        """
        self.buffer_size = buffer_size
        self.schema = schema
        self._initialize_buffer()
        fs, output_path = fsspec.core.url_to_fs(output_file)
        self.output_fd = fs.open(output_path, "wb")
        self.parquet_writer = parquet.ParquetWriter(self.output_fd, schema)

    def _initialize_buffer(self) -> None:
        """Initialize the buffer."""
        self.current_buffer_size = 0
        self.buffer = {k: [] for k in self.schema.names}

    def _add_sample_to_buffer(self, sample: Dict[str, Any]) -> None:
        """Add a sample to the buffer."""
        for k in self.schema.names:
            self.buffer[k].append(sample[k])
        self.current_buffer_size += 1

    def write(self, sample: Dict[str, Any]) -> None:
        """Write a sample to the buffer."""
        if self.current_buffer_size >= self.buffer_size:
            self.flush()
        self._add_sample_to_buffer(sample)

    def flush(self) -> None:
        """Write the buffer to disk."""
        if self.current_buffer_size == 0:
            return
        buffer_table = pyarrow.Table.from_pydict(self.buffer, self.schema)
        self.parquet_writer.write_table(buffer_table)
        self._initialize_buffer()

    def close(self) -> None:
        """Close the writer."""
        self.flush()
        if self.parquet_writer is not None:
            self.parquet_writer.close()
            self.parquet_writer = None
            self.output_fd.close()


class WebDatasetSampleWriter:
    """
    Writes videos and captions to webdataset format.
    Webdataset is just a .tar with an implied structure.
    This streaming format is good for sequential file access.
    """

    def __init__(
        self,
        shard_id: int,
        shard_n_sig_figs: int,
        output_folder: str,
        schema: pyarrow.Schema,
        encode_formats: Dict[str, str],
    ) -> None:
        """
        Initialize shard parameters and open tar file for writing videos and captions.

        Args:
            shard_id (int): The ID of the shard.
            shard_n_sig_figs (int): The length of the shard name, including leading zeros.
            output_folder (str): The folder where the output will be stored.
            schema (str): The schema for the metadata.
            encode_formats (Dict[str, str]): A dictionary mapping modalities to their encode formats.
        """
        # Initialize shard parameters
        self.shard_n_sig_figs = shard_n_sig_figs
        self.shard_id = shard_id
        shard_name = str(shard_id).zfill(shard_n_sig_figs)
        # Open tar file for writing videos and captions
        file_system, output_path = fsspec.core.url_to_fs(output_folder)
        self.tar_file = file_system.open(f"{output_path}/{shard_name}.tar", "wb")
        self.tar_writer = webdataset.TarWriter(self.tar_file)
        # Initialize buffered parquet writer for writing metadata
        self.buffered_parquet_writer = BufferedParquetWriter(f"{output_path}/{shard_name}.parquet", schema, 100)
        self.encode_formats = encode_formats
        # Run close function if program exits
        atexit.register(self.close)

    def write(self, streams: Dict[str, Any], key: str, caption: str, metadata: Dict[str, Any]) -> None:
        """Write a sample to this shard's .tar file."""
        sample = {"__key__": key}

        # Convert non-serializable metadata to serializable format
        for k, v in metadata.items():
            if isinstance(v, np.ndarray):
                metadata[k] = v.tolist()

        # Add modalities to sample, including caption for txt modality and metadata for json modality
        for modality, stream in streams.items():
            encode_format = self.encode_formats[modality]
            sample[encode_format] = stream
        sample["txt"] = str(caption) if caption is not None else ""
        sample["json"] = json.dumps(metadata, indent=4)

        # Write sample and metadata
        self.tar_writer.write(sample)
        self.buffered_parquet_writer.write(metadata)

    def close(self) -> None:
        """Close writers and .tar file."""
        self.buffered_parquet_writer.close()
        self.tar_writer.close()
        self.tar_file.close()