from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import monotonically_increasing_id
import subprocess
from typing import Iterator


def download_from_url(
    url: str,
    download_filepath: str,
):
    subprocess.run(["wget", "-q", "-O", download_filepath, url])
