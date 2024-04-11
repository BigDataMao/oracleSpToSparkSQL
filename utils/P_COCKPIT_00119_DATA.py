# -*- coding: utf-8 -*-
"""
收入分配表(最终呈现表) foF产品 明细数据 数据落地，到月份
"""
import logging

from pyspark.sql.functions import col, lit

from utils.task_env import return_to_hive


def p_cockpit_00119_data(spark, busi_date):
    df_cockpit_00118 = spark.table("ddw.t_cockpit_00118")
    df_cockpit_00117 = spark.table("ddw.t_cockpit_00117")

    i_busi_month = busi_date[:6]
    # 收入分配表(最终呈现表) 数据落地
    df_y = df_cockpit_00118.alias("t") \
        .filter(
        col("t.busi_month") == lit(i_busi_month)
    ).join(
        other=df_cockpit_00117.alias("a"),
        on=(
            col("t.busi_month") == col("a.busi_month"),
            col("t.filing_code") == col("a.filing_code")
        ),
        how="inner"
    ).select(
        col("t.busi_month"),
        col("t.filing_code"),
        col("t.product_name"),
        col("a.alloca_oa_branch_id"),
        col("a.alloca_oa_branch_name"),
        (col("t.compre_income") * col("a.alloca_reate")).alias("compre_income"),
        (col("t.manage_fee_income_extax") * col("a.alloca_reate")).alias("sales_incentives"),
        (col("t.manage_fee_income_extax") * col("a.alloca_reate")).alias("manage_fee_income_extax"),
        (col("t.clear_remain_transfee_extax") * col("a.alloca_reate")).alias("clear_remain_transfee_extax"),
        (col("t.market_reduct_extax") * col("a.alloca_reate")).alias("market_reduct_extax"),
        (col("t.interest_income") * col("a.alloca_reate")).alias("interest_income"),
        (col("t.client_ret_marreduct_extax") * col("a.alloca_reate")).alias("client_ret_marreduct_extax"),
        (col("t.client_ret_interest") * col("a.alloca_reate")).alias("client_ret_interest"),
        (col("t.total_futu_income") * col("a.alloca_reate")).alias("total_futu_income")
    )

    return_to_hive(
        spark=spark,
        df_result=df_y,
        target_table="ddw.t_cockpit_00119",
        insert_mode="overwrite"
    )

    logging.info("ddw.t_cockpit_00119写入完成")
