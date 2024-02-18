from pyspark.sql import SparkSession


class ParquetGenerator:
    def __init__(
        self,
        parquet_filepath: str,
        processed_rows: list[int],
    ) -> None:
        spark = SparkSession.builder.master("local[*]").appName("MyApp").getOrCreate()
        df = spark.read.parquet(parquet_filepath)
        df_filtered = df.filter(~df[processed_rows])