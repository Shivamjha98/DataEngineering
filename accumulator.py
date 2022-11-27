from pyspark import SparkContext
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable

sc = SparkContext("local[*]", "keywordcount")

def blanklineschecker(line):
    if len(line) == 0:
        myaccum.add(1)

base_rdd = sc.textFile("C:/Users/shivi/OneDrive/Desktop/shared/blanklines.txt")

myaccum = sc.accumulator(0)

base_rdd.foreach(blanklineschecker)

print(myaccum.value)