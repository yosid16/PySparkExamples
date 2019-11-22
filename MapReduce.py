import os
import sys
from pyspark import SparkContext, SparkConf
from pyspark import RDD
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, ArrayType


def main():
    print("Started map reduce example")
    spark: SparkSession = get_spark_session()

    rdd_of_lines: RDD[str] = spark.sparkContext\
        .textFile("./sample_datasets/article.txt")\
        .setName("RawDataRDD")
    show(rdd_of_lines)

    rdd_of_array_of_words: RDD[list[str]] = rdd_of_lines.map(lambda line: line.split(" "))\
        .setName("splittingLinesRDD")
    show(rdd_of_array_of_words)

    rdd_of_map_of_words: RDD[map[str, int]] = rdd_of_array_of_words\
        .map(lambda wordArray: {word: 1 for word in wordArray})\
        .setName("MapOfWordsInEachArray")
    show(rdd_of_map_of_words)

    unique_words = rdd_of_map_of_words.reduce(lambda words_dict1, words_dict2: sum_dictionaries(words_dict1, words_dict2))

    print("unique words: {}".format(unique_words))

    print("Finished map reduce example")


def sum_dictionaries(dict1, dict2):
    return {key: dict1.get(key, 0) + dict2.get(key, 0) for key in set(dict1) | set(dict2)}


def show(rdd: RDD):
    print("\n")
    print("showing top 5 lines from RDD: {}".format(rdd.name()))

    for line in rdd.take(5):
        print(line)

def get_spark_session():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    return SparkSession\
        .builder\
        .master("local")\
        .appName('SparkMapReduceExample')\
        .getOrCreate()


if __name__ == '__main__':
    main()
