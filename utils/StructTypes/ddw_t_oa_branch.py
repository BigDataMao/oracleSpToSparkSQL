# -*- coding: utf-8 -*-
from pyspark.sql.types import StructType, StringType, StructField

schema = StructType([
        StructField("departmentid", StringType()),
        StructField("shortname", StringType()),
        StructField("fullname", StringType()),
        StructField("subcompanyid", StringType()),
        StructField("supdepartmentid", StringType()),
        StructField("showorder", StringType()),
        StructField("code", StringType()),
        StructField("canceled", StringType()),
        StructField("create_time", StringType()),
        StructField("update_user", StringType()),
        StructField("update_time", StringType()),
        StructField("business_line_id", StringType()),
        StructField("business_line_name", StringType()),
        StructField("respons_line_id", StringType()),
        StructField("respons_line_name", StringType()),
])
