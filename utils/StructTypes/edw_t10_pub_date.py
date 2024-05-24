# -*- coding: utf-8 -*-
from pyspark.sql.types import StructType, StringType, StructField

schema = StructType([
        StructField("busi_date", StringType()),
        StructField("market_no", StringType()),
        StructField("trade_flag", StringType()),
])