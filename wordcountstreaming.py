from pyspark import SparkContext
from pyspark.streaming import StreamingContext


sc = SparkContext("local[*]", "wordcount")

StreamingContext(sc, Seconds(5))

