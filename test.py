# -*- coding: utf-8 -*-
# 本机运行测试任务: https://blog.csdn.net/qq_39950572/article/details/136260712
# pip install pyspark==2.4.4

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, lit

from utils.task_env import drop_duplicate_columns

spark = SparkSession.builder \
    .appName("test") \
    .master("local") \
    .getOrCreate()

data1 = [("Alice", "34", 0)]
columns1 = ["name", "age", "salary"]
df1 = spark.createDataFrame(data1, columns1)

data2 = [("Alice", "Sales", "20000")]
columns2 = ["name", "department", "salary"]
df2 = spark.createDataFrame(data2, columns2)

df_result = df1.join(df2, df1["name"] == df2["name"], "left") \
    .withColumn("salary", coalesce(df2["salary"], df1["salary"]))
df_result.show()

drop_duplicate_columns(df_result).show()


