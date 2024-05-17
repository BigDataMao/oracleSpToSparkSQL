# -*- coding: utf-8 -*-
import sys
from imp import reload

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql import Row

from utils.task_env import create_env
reload(sys)
sys.setdefaultencoding("utf-8")
# 创建 SparkSession
spark = create_env()

# 定义 schema
schema = StructType([
        StructField("market_id", StringType()),
        StructField("market_name", StringType()),
        StructField("market_fname", StringType()),
        StructField("market_no", StringType()),
        StructField("exchange_id", StringType()),
        StructField("ds_date", StringType()),
        StructField("data_source", StringType()),
])

# 创建数据
data = spark.table("edw.h11_market").rdd

# 创建 DataFrame 并赋予 schema
df = spark.createDataFrame(data, schema)

# 检查 DataFrame
df.printSchema()
df.select(df.exchange_id, df.market_name).show(5)
