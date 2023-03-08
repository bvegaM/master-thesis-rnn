from pyspark.sql import SparkSession


def read_file(path: str, spark: SparkSession):
    return spark.read.parquet(path)


def read_file_distributed(path, spark: SparkSession):
    return spark.read.parquet(*path)


def save_file(path: str, df_rating):
    df_rating.write.mode('overwrite').format('parquet').save(path)


def save_file_partition(path: str, df_rating):
    df_rating.repartition(6).write.mode('overwrite').parquet(path)


