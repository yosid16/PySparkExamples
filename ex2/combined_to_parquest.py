import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

COMBINED_JSON_PATH="/Users/yossid/Desktop/BigData/ex1/combined.json"
COMBINED_PARQUET_PATH="/Users/yossid/Desktop/BigData/ex1/combined.parquet"

def main():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("INFO")
    df: DataFrame = spark.read\
        .json(COMBINED_JSON_PATH)

    df.coalesce(1).write\
        .format("parquet")\
        .save(COMBINED_PARQUET_PATH)


def get_spark_session():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    return SparkSession\
        .builder.master("local[6]")\
        .appName('SparkMapReduceExample')\
        .getOrCreate()


if __name__ == '__main__':
    main()