# -*- coding: utf-8 -*-
"""
经营目标责任书-数据生成
"""
import logging

from pyspark.sql.functions import col, lit

from utils.task_env import return_to_hive, update_dataframe


def p_cockpit_00136_data(spark, busi_date):
    """
    TODO:
    本方法其实就是oracle里面的merge into的功能
    理应有一个主表，一个副表，主表是要更新的表，副表是用来更新主表的表
    本方法的主要功能是将副表的数据更新到主表中
    应该有更加简洁的方法来实现这个功能
    """
    # 获取年份
    i_year_id = busi_date[:4]

    df_136_y = spark.table("ddw.T_COCKPIT_00135").alias("t") \
        .filter(
        col("t.year_id") == lit(i_year_id),
        col("t.check_result") == lit("1"),  # 校验结果(0：异常，1：正常)
        col("t.index_status") == lit("1"),  # 状态(0:待审核，1：审核通过，2：审核未通过)
    ).join(
        other=spark.table("ddw.T_COCKPIT_00136").alias("a"),
        on=(
            col("t.year_id") == col("a.year_id") &
            col("t.oa_branch_id") == col("a.oa_branch_id") &
            col("t.index_id") == col("a.index_id")
        ),
        how="left_anti"
    ).select(
        "t.year_id",
        "t.oa_branch_id",
        "t.oa_branch_name",
        "t.index_id",
        "t.index_asses_benchmark",
        "t.index_type",
        "t.index_name",
        "t.year_target_value",
        "t.quarter_target_1",
        "t.quarter_target_2",
        "t.quarter_target_3",
        "t.quarter_target_4",
        "t.weight_rate",
        "t.upper_limit_score",
    )
    
    return_to_hive(
        spark=spark,
        df_result=df_136_y,
        target_table="ddw.T_COCKPIT_00136",
        insert_mode="append",
    )

    logging.info("ddw.T_COCKPIT_00136[增量数据],写入完成")

    df_y = spark.table("ddw.T_COCKPIT_00135").alias("t") \
        .filter(
        col("t.year_id") == lit(i_year_id),
        col("t.check_result") == lit("1"),  # 校验结果(0：异常，1：正常)
        col("t.index_status") == lit("1"),  # 状态(0:待审核，1：审核通过，2：审核未通过)
    )

    df_136 = update_dataframe(
        df_to_update=spark.table("ddw.T_COCKPIT_00136"),
        df_use_me=df_y,
        join_columns=["year_id", "oa_branch_id", "index_id"],
        update_columns=[
            "year_target_value",
            "quarter_target_1",
            "quarter_target_2",
            "quarter_target_3",
            "quarter_target_4",
        ]
    )

    return_to_hive(
        spark=spark,
        df_result=df_136,
        target_table="ddw.T_COCKPIT_00136",
        insert_mode="overwrite",
    )

    logging.info("ddw.T_COCKPIT_00136[全量数据],更新完成")
