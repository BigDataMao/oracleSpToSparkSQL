# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import col, sum, lit

spark = SparkSession.builder.appName("test").getOrCreate()

# 定义 schema1
schema1 = StructType([
        StructField("name", StringType()),
        StructField("age", IntegerType()),
        StructField("salary", FloatType()),
])

# 定义 schema2
schema2 = StructType([
        StructField("NAME", StringType()),
        StructField("AGE", IntegerType()),
        StructField("height", FloatType()),
])

# 创建数据
data1 = [
    ("Tom", 28, 2000.0),
    ("Jerry", 22, 1500.0),
    ("Mickey", 35, 2500.0),
    ("Minnie", 30, 2200.0),
]

data2 = [
    ("Tom", 280, 2000.0),
    ("Jerry", 220, 1500.0),
    ("Mickey", 80, 2500.0),
    ("zzzz", 90, 2200.0),
]

data3 = [
    ("Tom", 280, 2000.0),
    ("Tom", 280, 2000.0),
    ("Jerry", 220, 1500.0),
    ("Mickey", 80, 2500.0),
    ("zzzz", 90, 2200.0),
]

# 创建 DataFrame 并赋予 schema
df1 = spark.createDataFrame(data1, schema1)
df2 = spark.createDataFrame(data2, schema2)
df3 = spark.createDataFrame(data3, schema2)

df3.groupBy(
    df3['NAME'],
    lit(1)
).agg(
    sum(df3['AGE'])
).show()
