# -*- coding: utf-8 -*-
# 本机运行测试任务: https://blog.csdn.net/qq_39950572/article/details/136260712
# pip install pyspark==2.4.4
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from utils.date_utils import *

spark = SparkSession.builder \
    .appName("test") \
    .enableHiveSupport() \
    .getOrCreate()

get_trade_date_udf = udf(get_trade_date, StringType())
# 创建测试DataFrame
test_df = spark.createDataFrame([
    ("20240101",),
    ("20240102",),
    ("20240103",),
    ("20240104",),
    ("20240105",)
], ["busi_date"])

# 添加交易日期列
result_df = test_df.withColumn(
    "trade_date",
    get_trade_date_udf(spark, col("busi_date"), lit('0'))
)

result_df.show()


