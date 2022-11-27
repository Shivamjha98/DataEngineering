from pyspark.sql import SparkSession
import os
import sys
from pyspark.sql.functions import udf, expr
from pyspark.sql.types import StringType

os.environ['PYSPARK_PYTHON'] = sys.executable


def check_age(age):
    if age > 18:
        return "Y"
    else:
        return "N"


spark = SparkSession.builder.appName("my application").master("local[*]")\
        .getOrCreate()

spark.udf.register("check_age_udf", check_age, StringType())

df = spark.read.format("csv") \
            .option("inferSchema", True) \
            .option("path", "/Users/shivi/OneDrive/Desktop/shared/dataset1.csv") \
            .load()

df_new = df.toDF("name", "age", "city")

for x in spark.catalog.listFunctions():
    print(x)

df_new.createOrReplaceTempView("age_table")


final_df = spark.sql("select check_age_udf(age) as age, name, city from age_table")

final_df.show()