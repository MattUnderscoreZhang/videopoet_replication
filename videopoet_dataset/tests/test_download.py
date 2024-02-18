import csv
import os

from videopoet_dataset.download import download_from_url
from .utils import calculate_file_hash


def test_download_from_url():
    with open("urls/test/webvid_test_100_samples.csv") as f:
        reader = csv.DictReader(f)
        first_row = next(reader)
    download_filepath = f"{first_row['videoid']}.mp4"
    download_from_url(
        url=first_row['contentUrl'],
        download_filepath=download_filepath,
    )
    downloaded_file_hash = calculate_file_hash(download_filepath)
    os.remove(download_filepath)
    assert downloaded_file_hash == "8920d597d6cc0206f59e49d420f1a08d3e9742e85d1fc6a80bb113dd67d75746"
