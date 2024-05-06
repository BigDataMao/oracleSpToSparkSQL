# -*- coding: utf-8 -*-
import logging

from pyspark.sql.functions import col

from utils.task_env import return_to_hive, log


@log
def p_cockpit_00110_1_data(spark, busi_date):
    """
    业务人员营业部维护
    """
    df_ds_crm_broker = spark.table("ods.t_ds_crm_broker")
    df_cockpit_00110 = spark.table("ddw.t_cockpit_00110")

    df_y = df_ds_crm_broker.alias("t") \
        .filter(
        col("t.broker_id").like("ZD")  # 驻点人员
    ).join(
        other=df_cockpit_00110.alias("a"),
        on=(
                col("t.broker_id") == col("a.broker_id")
        ),
        how="left_anti"
    ).select(
        col("t.broker_id"),
        col("t.broker_nam")
    )
    return_to_hive(
        spark=spark,
        df_result=df_y,
        target_table="ddw.t_cockpit_00110",
        insert_mode="overwrite"
    )
