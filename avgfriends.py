from pyspark import SparkContext
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable

sc = SparkContext("local[*]", "avgfriends")

input = sc.textFile("C:/Users/shivi/OneDrive/Desktop/shared/friendsdata.csv")

#input
# 0::will::33::385
# 1::jean::26::2
# 2::deanna::33::400

# output we need
# (33, (385, 1))
# (26, (2,1))
# (33, (400, 1))

rdd1 = input.map(lambda x: (int(x.split("::")[2]), (int(x.split("::")[3]), 1)))

# input
# (33, (385, 1)) => x
# (26, (2,1))
# (33, (400, 1)) => y

# output we need
# (33,(385+400, 2)) => (33, (785,2))

rdd2 = rdd1.reduceByKey(lambda x,y : (x[0]+y[0], x[1]+y[1]))

# input
# (33,(785, 2))
#
# output
# (33, (785/2)) => (33, 393)


rdd3 = rdd2.mapValues(lambda x: x[0]/x[1])

result = rdd3.collect()

for a in result:
    print(a)