import os
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from time import sleep

def main():
    print("Started aggregation example")
    spark = get_spark_session()

    dfHeights: DataFrame = spark.read.option("header", "true")\
        .csv("./sample_datasets/heights.csv").alias("heights")
    dfNames: DataFrame = spark.read.option("header", "true")\
        .csv("./sample_datasets/names.csv").alias("names")
    joined_df: DataFrame = dfHeights.join(dfNames, "id")
    joined_df.show()

    print("Finished aggregation example")

def get_spark_session():
    return SparkSession\
        .builder.master("local")\
        .appName('SparkMapReduceExample')\
        .getOrCreate()


if __name__ == '__main__':
    main()
