from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import monotonically_increasing_id
import subprocess
from typing import Iterator

from videopoet_dataset.download import download_from_url


def download_and_process_partition(partition: Iterator[Row]):
    for row in partition:
        download_from_url(
            url=row['url'],
            download_filepath=f"videos/{row['ID']}.mp4",
        )


spark = (
    SparkSession.builder
    .appName("HelloWorld")           # display name in Spark UI
    .config(                         # can be overwritten by spark-submit configs
        "spark.master", "local[12]"  # run Spark locally with 12 worker threads
    )
    .getOrCreate()
)
df = spark.read.csv("urls/old_cartoons_sample.csv", header=True, inferSchema=True)
df = df.withColumn("ID", monotonically_increasing_id())
df.foreachPartition(download_and_process_partition)
