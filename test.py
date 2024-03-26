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

data1 = [("Alice", "34", 0)]
columns1 = ["name", "age", "salary"]
df1 = spark.createDataFrame(data1, columns1).alias("a")

data2 = [("Alice", "Sales", "20000")]
columns2 = ["name", "department", "salary"]
df2 = spark.createDataFrame(data2, columns2).alias("b")

df_result = update_dataframe(df1, df2, df1["name"] == df2["name"], ["salary"])

print(df_result.columns)
df_result.show()
# df_result.select("a.salary").show(10, False)





