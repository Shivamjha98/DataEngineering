from pyspark import SparkContext
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable

sc = SparkContext("local[*]", "amtspent")

# below is the base rdd, we'll perform transformation on this
input = sc.textFile("C:/Users/shivi/OneDrive/Desktop/shared/customerorders.csv")

# 44, 8602, 37.19
# 35, 8103, 22.12
# 44, 8603, 99.19

#split on the basis of comma
cust_rdd = input.map(lambda x: x.split(","))

# are we interested in cust_id = yes
# we don't need product id
# we need amount spent = yes

# so apply map and select 2 columns

cust = cust_rdd.map(lambda x: (int(x[0]), float(x[2])))

# 44,37.19
# 35,22.12
# 44,99.19

amt_spent = cust.reduceByKey(lambda x,y: x + y)

# 44, 37.19
# 44.99.19             -> (44. 103.45)

# sort on basis of amt spent in desc

final_result = amt_spent.sortBy(lambda x: x[1], False)

# result

results = final_result.collect()

for a in results:
    print(a)

