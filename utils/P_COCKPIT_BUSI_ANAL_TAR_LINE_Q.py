# -*- coding: utf-8 -*-

"""
经营分析-业务条线-经营目标完成情况-按季度
"""

import logging

from utils.date_utils import *
from utils.task_env import return_to_hive


def p_cockpit_busi_anal_tar_line_q(spark, busi_date):
    logging.info("p_cockpit_busi_anal_tar_line_q执行开始")

    v_busi_year = busi_date[:4]
    v_BUSI_QUARTER = get_quarter(busi_date)

    """
        考核指标：
    001：考核收入
    002：经纪业务手续费收入市占率
    003：考核日均权益
    004：日均权益市占率
    005：考核利润
    006：成交额
    007：成交额市占率
    008：成交量
    009：成交量市占率
    010：新增直接开发有效客户数量
    011：新增有效客户数量
    012：产品销售额
    """

    # 初始化数据

    df_q = spark.table("ddw.T_business_line").alias("t") \
        .filter(
        col("t.if_use") == "1"
    ).join(
        other=spark.table("ddw.T_BUSI_ANAL_TARGET_TYPE").alias("a"),
        on=None,
        how="inner"
    ).select(
        lit(v_busi_year).alias("BUSI_YEAR"),
        lit(v_BUSI_QUARTER).alias("BUSI_QUARTER"),
        col("t.business_line_id"),
        col("a.busi_type"),
        col("a.busi_type_name")
    )

    return_to_hive(
        spark=spark,
        df_result=df_q,
        target_table="ddw.T_COCKPIT_BUSI_ANAL_TAR_LINE_Q",
        insert_mode="overwrite",
        partition_column=["BUSI_YEAR", "BUSI_QUARTER"],
        partition_value=[v_busi_year, v_BUSI_QUARTER]
    )

    df_q = spark.table("ddw.T_COCKPIT_BUSI_ANAL_TAR_LINE_Q").filter(
        (col("BUSI_YEAR") == v_busi_year) &
        (col("BUSI_QUARTER") == v_BUSI_QUARTER)
    )

    """
    更新指标数据
    后续更新
    """
    # TODO: 后续更新
