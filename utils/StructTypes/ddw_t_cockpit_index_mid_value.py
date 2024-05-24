# -*- coding: utf-8 -*-
from pyspark.sql.types import StructType, StringType, StructField, DoubleType

schema = StructType([
        StructField("book_id", StringType()),
        StructField("index_value", DoubleType()),
])
