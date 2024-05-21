# -*- coding: utf-8 -*-
from pyspark.sql.types import StructType, StringType, StructField

schema = StructType([
        StructField("busi_type", StringType()),
        StructField("busi_type_name", StringType()),
        StructField("is_select_flag", StringType()),
])
