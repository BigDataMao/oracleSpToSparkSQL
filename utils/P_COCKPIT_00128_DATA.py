# -*- coding: utf-8 -*-
"""
投资者保障基金调整表-初始化数据生成
"""
import logging

from pyspark.sql.functions import col, lit

from utils.task_env import return_to_hive


def p_cockpit_00128_data(spark, busi_date):
    logging.basicConfig(level=logging.INFO)

    v_busi_month = busi_date[:6]
    df_128_m = spark.table("ddw.T_COCKPIT_00128").filter(col("busi_month") == lit(v_busi_month))
    df_branch = spark.table("edw.h11_branch")
    df_128_m = df_branch.join(df_128_m, df_branch["branch_id"] == df_128_m["branch_id"], "left_anti")

    return_to_hive(
        spark=spark,
        df_result=df_128_m,
        target_table="ddw.T_COCKPIT_00128",
        insert_mode="overwrite",
        partition_column="busi_month",
        partition_value=v_busi_month
    )

    logging.info("ddw.T_COCKPIT_00128写入完成")
