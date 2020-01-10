import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType
import hashlib

COMBINED_JSON_SOURCE_PATH="/Users/yossid/csqa/*/*"
COMBINED_JSON_PATH="/Users/yossid/Desktop/BigData/ex1/combined.json"
COMBINED_PARQUET_PATH="/Users/yossid/Desktop/BigData/ex1/combined.parquet"

JSON_SCHEMA = StructType([
    StructField("question-type", StringType())
])

def main():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("INFO")

    # All you need to do to load this from other format
    # Assuming the schema is the same, is to change the initial dataframe
    df: DataFrame = json_source(spark)
    # No cost, since schema is explicit
    df.printSchema()

    # Cache to prevent reading the data from the source multiple times
    df.cache()

    count_df = df.groupBy("question-type").count()
    # For the example, this is showing the data
    # In the solution you should use: count_df.write.csv..
    count_df.show()

    # calculated column using UDF
    calc_hash_udf = udf(calc_hash, StringType())
    md5_df = count_df.select("question-type", "count", calc_hash_udf("question-type").alias("question-type-hash"))

    # For the example, this is showing the data
    # In the solution you should use: write.option("delimiter", "\t").csv..
    md5_df.show()

    with_window_sum = md5_df.withColumn("portion_of_total_question", F.col('count') * 100 / F.sum('count').over(Window.partitionBy()))
    with_window_sum.show()


def json_source(spark):
    return spark.read.schema(JSON_SCHEMA).json(COMBINED_JSON_PATH)

def json_parquet(spark):
    return spark.read\
        .parquet(COMBINED_PARQUET_PATH)

def calc_hash(value):
    if value is not None:
        return hashlib.md5(value.encode('utf-8')).hexdigest()
    return None

def get_spark_session():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    return SparkSession\
        .builder.master("local")\
        .appName('SparkMapReduceExample')\
        .getOrCreate()


if __name__ == '__main__':
    main()