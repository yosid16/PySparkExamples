import os
from pyspark import SparkContext, SparkConf
from pyspark import RDD
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from time import sleep

def main():
    print("Started aggregation example")
    spark = get_spark_session()

    df_heights: DataFrame = spark.read.option("header", "true")\
        .csv("./sample_datasets/heights.csv")
    df_heights.createOrReplaceTempView("heights")

    df_names: DataFrame = spark.read.option("header", "true")\
        .csv("./sample_datasets/names.csv")
    df_names.createOrReplaceTempView("names")
    df_joined = spark.sql("SELECT *, Height / 2.54 as HeightInInch FROM heights join names on heights.id = names.id")
    df_joined.show()

    print("Finished aggregation example")

def get_spark_session():
    return SparkSession\
        .builder.master("local")\
        .appName('SparkMapReduceExample')\
        .getOrCreate()


if __name__ == '__main__':
    main()
