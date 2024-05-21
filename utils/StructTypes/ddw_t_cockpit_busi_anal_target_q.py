# -*- coding: utf-8 -*-
from pyspark.sql.types import StructType, StringType, StructField, DoubleType

schema = StructType([
        StructField("oa_branch_id", StringType()),
        StructField("busi_type", StringType()),
        StructField("busi_type_name", StringType()),
        StructField("complete_value", DoubleType()),
        StructField("complete_value_rate", DoubleType()),
        StructField("busi_year", StringType()),
        StructField("busi_quarter", StringType()),
])