# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, lit, when

from utils.task_env import log, return_to_hive


@log
def p_cockpit_index_mid_value(spark: SparkSession, busi_month, index_id, cal_direction=0, summary=''):
    """
    宏源，获取科目当月发生额
    TODO: 兼容了P_COCKPIT_INDEX_MID_VALUE_1
    :param spark: SparkSession对象
    :param busi_month: 业务月份
    :param index_id: 指标ID
    :param cal_direction: 计算方向
    :param summary: 摘要
    :return:
    """
    df = spark.table("edw.h16_hync65_account_balance").fillna(0).alias('t').where(
        (col("account_period") == lit(busi_month)) &
        (col("account_code").like(index_id + "%")) &
        (when(
            lit(summary) == lit(''),
            lit(True)
        ).otherwise(
            col("summary").like('%' + summary + '%')
        ))
    ).join(
        other=spark.table("ddw.t_cockpit_book").alias("a"),
        on=(col("t.book_code") == col("a.book_code")),
        how="left_anti"
    ).groupBy(
        col("t.book_code").alias("BOOK_ID")
    ).agg(
        (sum(col("local_credit_sum")) * cal_direction).alias("INDEX_VALUE")
    ).select(
        col("BOOK_ID"),
        col("INDEX_VALUE")
    )

    return df

