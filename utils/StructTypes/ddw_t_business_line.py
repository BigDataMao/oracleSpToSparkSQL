# -*- coding: utf-8 -*-
from pyspark.sql.types import StructType, StringType, StructField

schema = StructType([
        StructField("pk_id", StringType()),
        StructField("business_line_id", StringType()),
        StructField("business_line_name", StringType()),
        StructField("update_user", StringType()),
        StructField("update_time", StringType()),
        StructField("if_use", StringType()),
])
