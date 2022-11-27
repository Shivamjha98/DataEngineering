from pyspark import SparkContext
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable

sc = SparkContext("local[*]", "wordcount")
# sc.setLogLevel("INFO")

# once the sc is set, let's load data in base rdd

input = sc.textFile("C:/Users/shivi/OneDrive/Desktop/shared/search_data.txt")

# # once base rdd is ready we've
# hello how are you
# hello what are you doing.......

words = input.flatMap(lambda x: x.split(" "))
print(words)

# hello
# how
# are
# you
# hello

word_map = words.map(lambda x : (x.upper(),1))
print("word-map", word_map)
# (hello, 1)
# (how,1)
# (are,1)
# (you,1)

word_count = word_map.reduceByKey(lambda x,y: x + y)

# (hello, 2)
# (are,2 )

result = word_count.collect()

for a in result:
    print(a)