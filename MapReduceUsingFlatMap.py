import os
import sys
from pyspark import SparkContext, SparkConf
from pyspark import RDD
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def main():
    print("Started map reduce example")
    spark = get_spark_session()

    raw_text_rdd: RDD[str] = spark.sparkContext.textFile("./sample_datasets/article.txt")
    word_count_rdd = raw_text_rdd.flatMap(lambda line: line.split(" "))\
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda word1Count, word2Count: word1Count + word2Count)
    show(word_count_rdd)

    print("Finished map reduce example")


def show(rdd: RDD):
    print("\n")
    print("showing top 5 lines from RDD: {}".format(rdd.name()))

    for line in rdd.take(5):
        print(line)

def get_spark_session():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    return SparkSession\
        .builder.master("local")\
        .appName('SparkMapReduceExample')\
        .getOrCreate()


if __name__ == '__main__':
    main()
