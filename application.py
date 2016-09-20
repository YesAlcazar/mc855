from __future__ import print_function

import sys
from operator import add

from pyspark.sql import SparkSession

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    counts = lines.flatMap(lambda x: x.split(' ')) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add) \
                  .map(lambda (a, b): (b, a)) \
                  .sortByKey(0, 1) \
                  .map(lambda (a, b): (b, a))
    output = counts.collect()

    number = 0;

    for (word, count) in output:
    	if len(word) > 2 and number < 10:
        	print("%s: %i" % (word.encode('utf-8'), count))
        	number += 1

    spark.stop()
