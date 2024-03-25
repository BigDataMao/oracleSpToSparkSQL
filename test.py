# -*- coding: utf-8 -*-
# https://blog.csdn.net/qq_39950572/article/details/136260712
# pip install pyspark==2.4.4

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce

spark = SparkSession.builder \
    .appName("test") \
    .master("local") \
    .getOrCreate()

data1 = [("Alice", "34"), ("Bob", "45"), ("Tom", "23")]
columns1 = ["name", "age"]
df1 = spark.createDataFrame(data1, columns1)

data2 = [("Alice", "Sales"), ("Bob", "Marketing"), ("Jake", "Product")]
columns2 = ["name", "department"]
df2 = spark.createDataFrame(data2, columns2)

df_result = df1.join(df2, df1["name"] == df2["name"], "left") \
    .withColumn("age", coalesce(df2["department"], df1["age"]))
df_result.show()


