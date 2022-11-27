from pyspark import SparkContext
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable

sc = SparkContext("local[*]", "loglevel")
# sc.setLogLevel("INFO")

# result = sc.textFile("C:/Users/shivi/OneDrive/Desktop/shared/bigLog.txt") \
#     .map(lambda x: (x.split(":")[0], 1)) \
#     .reduceByKey(lambda x,y: x+y)\
#     .collect()

result = sc.textFile("C:/Users/shivi/OneDrive/Desktop/shared/bigLog.txt")  \
    .map(lambda x: (x.split(":")[0], x.split(":")[1])) \
    .groupByKey() \
    .map(lambda x: (x[0], len(x[1]))) \
    .collect()

for x in result:
    print(x)