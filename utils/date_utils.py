# -*- coding: utf-8 -*-

"""
专门处理日期参数的工具类
"""
from pyspark.sql.functions import col, min, max, count, lit

pub_date_table = "edw.t10_pub_date"


def get_last_year_begin_month(busi_date):
    return str(int(busi_date[:4]) - 1) + "01"


def get_last_year_end_month(busi_date):
    return str(int(busi_date[:4]) - 1) + "12"


def get_date_period_and_days_this_month(spark, busi_month, trade_flag="1"):
    df_date = spark.table(pub_date_table) \
        .filter(
        (col("busi_date").substr(1, 6) == busi_month) &
        (col("market_no") == "1") &
        ((col("trade_flag") == "1") if trade_flag == "1" else lit(True))
    ).agg(
        min("busi_date").alias("v_begin_date"),
        max("busi_date").alias("v_end_date"),
        count("*").alias("v_trade_days")
    )

    if trade_flag == "1":
        return (df_date.first()["v_begin_date"],
                df_date.first()["v_end_date"],
                df_date.first()["v_trade_days"])
    else:
        return (df_date.first()["v_begin_date"],
                df_date.first()["v_end_date"])
