import os
from pyspark import SparkContext, SparkConf
from pyspark import RDD
from pyspark.sql.types import DoubleType
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType

HEIGHTS_SCHEMA = StructType([
    StructField("id", IntegerType()),
    StructField("Height", DoubleType()),
    StructField("Gender", IntegerType())
])

def main():
    print("Started aggregation example")
    spark = get_spark_session()

    df: DataFrame = spark.read\
        .schema(HEIGHTS_SCHEMA)\
        .option("header", "true")\
        .csv("./sample_datasets/heights.csv")

    df = df.filter("height > 170")
    df.groupBy("Gender").avg("Height").show()

    print("Finished aggregation example")

def get_spark_session():
    return SparkSession\
        .builder.master("local")\
        .appName('SparkMapReduceExample')\
        .getOrCreate()


if __name__ == '__main__':
    main()
