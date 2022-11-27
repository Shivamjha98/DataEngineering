from pyspark import SparkContext, StorageLevel
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable

sc = SparkContext("local[*]", "persist")

base_rdd = sc.textFile("/Users/shivi/OneDrive/Desktop/shared/customerorders.csv")

mapped_rdd = base_rdd.map(lambda x: (x.split(",")[0], float(x.split(",")[2])))

total_by_cust = mapped_rdd.reduceByKey(lambda x,y: x+y)

premium_cust = total_by_cust.filter(lambda x: x[1]> 5000)

double_amt = premium_cust.map(lambda x: (x[0], x[1]*2)).persist(StorageLevel.MEMORY_ONLY)

for x in double_amt.collect():
    print(x)

print(double_amt.count())