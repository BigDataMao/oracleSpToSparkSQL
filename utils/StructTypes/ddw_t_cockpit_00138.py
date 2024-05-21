# -*- coding: utf-8 -*-
from pyspark.sql.types import StructType, StringType, StructField, DoubleType

schema = StructType([
        StructField("pk_id", StringType()),
        StructField("oa_branch_id", StringType()),
        StructField("oa_branch_name", StringType()),
        StructField("index_id", StringType()),
        StructField("index_asses_benchmark", StringType()),
        StructField("index_type", StringType()),
        StructField("index_name", StringType()),
        StructField("year_target_value", DoubleType()),
        StructField("complet_value", DoubleType()),
        StructField("complet_achievement_rate", DoubleType()),
        StructField("weight_rate", DoubleType()),
        StructField("upper_limit_score", DoubleType()),
        StructField("get_score", DoubleType()),
        StructField("actual_complet_value", DoubleType()),
        StructField("adjust_quarter_1", StringType()),
        StructField("adjust_1", DoubleType()),
        StructField("adjust_quarter_2", StringType()),
        StructField("adjust_2", DoubleType()),
        StructField("adjust_quarter_3", StringType()),
        StructField("adjust_3", DoubleType()),
        StructField("year_id", StringType()),
        StructField("quarter_id", StringType()),
])
