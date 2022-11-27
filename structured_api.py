from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.types import TimestampType

spark = SparkSession.builder.appName("my application").master("local[*]").getOrCreate()

# orderSchema = StructType([
#     StructField("orderid", IntegerType()),
#     StructField("orderdate", TimestampType()),
#     StructField("customerid", IntegerType()),
#     StructField("status", StringType())
# ])

orderSchemaDDL = """orderid Integer, 
                    orderdate Timestamp, custid Integer,
                    status String"""

ordersDf = spark.read.format("csv")\
            .option("header", True) \
            .schema(orderSchemaDDL) \
            .option("path", "/Users/shivi/OneDrive/Desktop/shared/orders.csv") \
            .load()

ordersDf.show()

ordersDf.printSchema()

spark.stop()