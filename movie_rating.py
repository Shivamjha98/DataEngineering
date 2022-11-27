from pyspark import SparkContext
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable

sc = SparkContext("local[*]", "movierating")

# below is based rdd
rdd1 = sc.textFile("C:/Users/shivi/OneDrive/Desktop/shared/moviedata.data")

# 180 242 3 485738573
# 171 260 4 493575775
# 150 272 5 594994949
# 171 987 3 484734734

rdd2 = rdd1.map(lambda x: (int(x.split("\t")[2]), 1))

# 3, 1
# 4, 1
# 5, 1
# 3, 1

result = rdd2.reduceByKey(lambda x,y: x+y).sortBy(lambda x: x[1]).collect()

for a in result:
    print(a)