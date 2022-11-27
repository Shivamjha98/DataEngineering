import datetime

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import unix_timestamp, monotonically_increasing_id, col, count, avg, sum, countDistinct, \
    window, date_format, expr, from_unixtime, lit
import os
import sys


os.environ['PYSPARK_PYTHON'] = sys.executable


spark = SparkSession.builder.appName("my app").master("local[*]").getOrCreate()

res = datetime.datetime.now()
invoiceDf = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("path", "/Users/shivi/OneDrive/Desktop/shared/orders.csv") \
    .load()

df = invoiceDf.withColumn("order_date", lit(res))
df.withColumn("month", date_format(col("order_date"), 'MM')).printSchema()

# invoiceDf.select(
#     count("*").alias("totalCount"),
#     avg("UnitPrice").alias("AvgPrice"),
#     sum("Quantity").alias("totalquantity"),
#     countDistinct("InvoiceNo").alias("countdistinct")
# ).show()
