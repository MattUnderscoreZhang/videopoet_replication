from .dataset_io import WebDatasetSampleWriter


worker = DownloadWorker(
    sample_writer_class=WebDatasetSampleWriter,
    save_caption=save_caption,
    output_folder=output_folder,
    column_list=shard_iterator.column_list,
    tmp_dir=tmp_dir,
    encode_formats=encode_formats,
    config=config,
)