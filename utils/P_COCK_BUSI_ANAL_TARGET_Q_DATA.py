# -*- coding: utf-8 -*-

"""
经营分析-业务单位-经营目标完成情况-按季度
"""
import logging

from pyspark.sql.functions import col, lit

from utils.date_utils import get_quarter
from utils.task_env import return_to_hive

logging.basicConfig(level=logging.INFO)


# TODO:  CF_BUSIMG.T_COCKPIT_BUSI_ANAL_TARGET_Q,分区字段,busi_year,busi_quarter

def p_cock_busi_anal_target_q_data(spark, busi_date):
    v_busi_year = busi_date[:4]
    v_busi_quarter = get_quarter()

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
    df_result = spark.table("ddw.T_OA_BRANCH").alias("t") \
        .filter(
        col("t.canceled").isNull()
    ).join(
        other=spark.table("ddw.T_BUSI_ANAL_TARGET_TYPE").alias("a"),
        on=None,
        how="inner"
    ).select(
        lit(v_busi_year).alias("BUSI_YEAR"),
        lit(v_busi_quarter).alias("BUSI_QUARTER"),
        col("t.departmentid"),
        col("a.busi_type"),
        col("a.busi_type_name"),
    )

    return_to_hive(
        spark=spark,
        df_result=df_result,
        target_table="ddw.T_COCKPIT_BUSI_ANAL_TARGET_Y",
        insert_mode="overwrite",
        partition_column=["busi_year", "busi_quarter"],
        partition_value=[v_busi_year, v_busi_quarter]
    )

    # TODO:  更新指标数据--待更新
    logging.info("p_cock_busi_anal_target_q_data执行完成")
    logging.info("本次任务为:经营分析-业务单位-经营目标完成情况-按季度")
