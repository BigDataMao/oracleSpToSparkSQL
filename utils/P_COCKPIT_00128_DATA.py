# -*- coding: utf-8 -*-
"""
投资者保障基金调整表-初始化数据生成
"""
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit


def p_cockpit_00128_data(spark: SparkSession, busi_date: str):
    logging.basicConfig(level=logging.INFO)

    v_busi_month = busi_date[:6]
    t_cockpit_00128 = spark.table("ddw.T_COCKPIT_00128").filter(col("busi_month") == lit(v_busi_month))
    df_branch = spark.table("edw.h11_branch")
    df_branch.join(t_cockpit_00128, df_branch["branch_id"] == t_cockpit_00128["branch_id"], "left_anti") \

