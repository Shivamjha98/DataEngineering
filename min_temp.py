from pyspark import SparkContext
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable


sc = SparkContext("local[*]", "adcampaign")

def loadBoringWords():
    return set(line.strip() for line in open("C:/Users/shivi/OneDrive/Desktop/shared/boringwords.txt"))


name_set = sc.broadcast(loadBoringWords())

#100 big
#100 data
#200 learn
#200 hadoop
#150 data

# big 100
#data 250
#learn 200
#hadoop 200

result = sc.textFile("C:/Users/shivi/OneDrive/Desktop/shared/bigdatacampaign.csv")\
    .map(lambda x : ( float(x.split(",")[10]), x.split(",")[0].lower())) \
    .flatMapValues(lambda x: x.split(" ")) \
    .map(lambda x : (x[1], x[0])) \
    .filter(lambda x: x[0] not in name_set.value) \
    .reduceByKey(lambda x,y: x+y) \
    .sortBy(lambda x: x[1], False) \
    .collect()

for a in result:
    word = a[0]
    amt_spent_on_ad = a[1]
    print("For word",word, "amount spent on Ad is", amt_spent_on_ad)

