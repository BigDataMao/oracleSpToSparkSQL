# -*- coding: utf-8 -*-

"""
经营分析-业务单位-经营目标完成情况-按年落地
"""
import logging

from pyspark.sql.functions import col, lit

from utils.task_env import return_to_hive

logging.basicConfig(level=logging.INFO)


# TODO: CF_BUSIMG.T_COCKPIT_BUSI_ANAL_TARGET_Y,按年分区,分区字段,busi_year
def p_cock_busi_anal_target_y_data(spark, busi_date):
    v_busi_year = busi_date[:4]

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
        col("t.departmentid"),
        col("a.busi_type"),
        col("a.busi_type_name"),
    )

    return_to_hive(
        spark=spark,
        df_result=df_result,
        target_table="ddw.T_COCKPIT_BUSI_ANAL_TARGET_Y",
        insert_mode="overwrite",
        partition_column="busi_year",
        partition_value=v_busi_year
    )

    # TODO:  更新指标数据--待更新
    logging.info("p_cock_busi_anal_target_y_data执行完成")
