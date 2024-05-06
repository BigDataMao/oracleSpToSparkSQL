# -*- coding: utf-8 -*-
import logging

from utils.task_env import return_to_hive, log


@log
def p_cockpit_00121_data(spark, busi_date):
    i_busi_month = busi_date[:6]
    # 读取Hive表数据并创建DataFrame
    df_t = spark.table("ddw.T_COCKPIT_00120")
    df_a = spark.table("ddw.T_COCKPIT_00117")

    # 进行连接和筛选
    result_df = df_t.join(
        other=df_a,
        on=(df_t["busi_month"] == df_a["busi_month"]) & (df_t["filing_code"] == df_a["filing_code"]),
        how="inner"
    ).filter(
        df_t["busi_month"] == i_busi_month
    ).select(
        df_t["busi_month"],
        df_t["filing_code"],
        df_t["product_name"],
        df_a["alloca_oa_branch_id"],
        df_a["alloca_oa_branch_name"],
        df_a["alloca_broker_product_id"],
        df_a["alloca_broker_product_name"],
        (df_t["total_futu_income"] * df_a["alloca_reate"]).alias("total_futu_income"),
        (df_t["CLEAR_REMAIN_TRANSFEE_EXTAX"] * df_a["alloca_reate"]).alias("CLEAR_REMAIN_TRANSFEE_EXTAX"),
        (df_t["MARKET_REDUCT_EXTAX"] * df_a["alloca_reate"]).alias("MARKET_REDUCT_EXTAX"),
        (df_t["INTEREST_INCOME"] * df_a["alloca_reate"]).alias("INTEREST_INCOME"),
        (df_t["CLIENT_RET_MARREDUCT_EXTAX"] * df_a["alloca_reate"]).alias("CLIENT_RET_MARREDUCT_EXTAX"),
        (df_t["CLIENT_RET_INTEREST"] * df_a["alloca_reate"]).alias("CLIENT_RET_INTEREST"),
        (df_t["end_rights"] * df_a["alloca_reate"]).alias("end_rights"),
        (df_t["avg_rights"] * df_a["alloca_reate"]).alias("avg_rights"),
        (df_t["interest_base"] * df_a["alloca_reate"]).alias("interest_base"),
        (df_t["clear_remain_transfee"] * df_a["alloca_reate"]).alias("clear_remain_transfee"),
        (df_t["market_reduct"] * df_a["alloca_reate"]).alias("market_reduct"),
        (df_t["CLIENT_RET_MARREDUCT"] * df_a["alloca_reate"]).alias("CLIENT_RET_MARREDUCT")
    )

    # 写入Hive表
    return_to_hive(spark, result_df, "ddw.T_COCKPIT_00121", "overwrite")
