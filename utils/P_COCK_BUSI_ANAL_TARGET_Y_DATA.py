# -*- coding: utf-8 -*-

"""
经营分析-业务单位-经营目标完成情况-按年落地
"""
import logging

from pyspark.sql.functions import col, lit

from config import Config
from utils.task_env import return_to_hive

logger = Config().get_logger()


# TODO: CF_BUSIMG.T_COCKPIT_BUSI_ANAL_TARGET_Y,按年分区,分区字段,busi_year
def p_cock_busi_anal_target_y_data(spark, busi_date):
    v_busi_year = busi_date[:4]

    # 初始化数据
    df_result = spark.table("ddw.T_OA_BRANCH").alias("t") \
        .filter(
        col("t.canceled").isNull()
    ).crossJoin(
        other=spark.table("ddw.T_BUSI_ANAL_TARGET_TYPE").alias("a")
    ).select(
        lit(v_busi_year).alias("BUSI_YEAR"),
        col("t.departmentid"),
        col("a.busi_type"),
        col("a.busi_type_name"),
    )

    return_to_hive(
        spark=spark,
        df_result=df_result,
        target_table="ddw.T_COCKPIT_BUSI_ANAL_TARGET_Y",
        insert_mode="overwrite",
    )

    # TODO:  更新指标数据--待更新
    logger.info("p_cock_busi_anal_target_y_data执行完成")
    logger.info("本次任务为: 经营分析-业务单位-经营目标完成情况-按年落地")
