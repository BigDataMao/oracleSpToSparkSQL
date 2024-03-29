# -*- coding: utf-8 -*-
# 本机运行测试任务: https://blog.csdn.net/qq_39950572/article/details/136260712
# pip install pyspark==2.4.4

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, lit, col

from utils.task_env import update_dataframe

spark = SparkSession.builder \
    .appName("test") \
    .master("local") \
    .getOrCreate()

data1 = [("Alice", "34", 0), ("Bob", "34", 0)]
columns1 = ["name", "age", "salary"]
df1 = spark.createDataFrame(data1, columns1).alias("a")

data2 = [("Alice", "34", 0), ("Bob", "34", 0)]
columns2 = ["name", "age", "salary"]
df2 = spark.createDataFrame(data2, columns2).alias("b")

df_3 = df1.join(df2, col("a.name") == col("b.name")) \

df_3 = df_3.withColumn("salary_1", col("a.age")) \

df_3.show()

