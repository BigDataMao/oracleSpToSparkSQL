# -*- coding: utf-8 -*-
"""
权益分配表-FOF产品（最终呈现表样式）
"""
from pyspark.sql import Window
from pyspark.sql.functions import col, min, max, count, sum, row_number, desc, when, lit, coalesce

from utils.task_env import return_to_hive, log


@log
def p_cockpit_00110_before(spark, busi_month):

    if busi_month.length() == 6:
        i_busi_month = busi_month
    else:
        raise Exception("busi_month参数格式错误！")

    df_date = spark.table("edw.t10_pub_date") \
        .filter(
        (col("busi_date").substr(1, 6) == i_busi_month) &
        (col("market_no") == "1") &
        (col("trade_flag") == "1")
    ).agg(
        min("busi_date").alias("v_begin_date"),
        max("busi_date").alias("v_end_date"),
        count("*").alias("v_trade_days")
    )

    v_begin_date = df_date.first()["v_begin_date"]
    v_end_date = df_date.first()["v_end_date"]
    v_trade_days = df_date.first()["v_trade_days"]

    # 客户证件号码，客户保有份额
    tmp = spark.table("ddw.T_COCKPIT_00096").alias("a") \
        .filter(
        (col("busi_month").between(v_begin_date, v_end_date))
    ).agg(
        sum("a.confirm_share").alias("client_confirm_share")
    ).select(
        "filing_code", "product_name", "client_name", "client_confirm_share"
    )

    # 产品总分配份额
    tmp1 = spark.table("ddw.T_COCKPIT_00095").alias("b") \
        .select(
        "filing_code", "product_name", "PRODUCT_TOTAL_SHARE",
        row_number().over(Window.partitionBy("filing_code").orderBy(desc("month_id"))).alias("rn")
    )

    tmp2 = tmp1.filter(col("rn") == 1).select(
        "filing_code", "PRODUCT_TOTAL_SHARE"
    )

    tmp3 = spark.table("ddw.T_COCKPIT_00097").alias("t") \
        .join(
        other=tmp.alias("a"),
        on=(col("t.filing_code") == col("a.filing_code")) & (col("t.client_name") == col("a.client_name")),
        how="left"
    ).join(
        other=tmp2.alias("b"),
        on=(col("t.filing_code") == col("b.filing_code")),
        how="left"
    ).select(
        "t.filing_code",
        "t.product_name",
        "b.product_total_share",
        "t.client_name",
        "a.id_no",
        "a.client_confirm_share",
        "t.oa_branch_id",
        "t.oa_branch_name",
        "t.oa_broker_name",
        "t.oa_broker_name_rate"
    )

    tmp4 = tmp3.alias("t") \
        .groupBy(
        "t.filing_code",
        "t.oa_branch_id",
        "t.oa_broker_name",
        "t.product_total_share"
    ).agg(
        sum("t.client_confirm_share").alias("client_confirm_share"),
        sum("t.client_confirm_share" * col("t.oa_broker_name_rate")).alias("oa_broker_product_share")
    ).select(
        "filing_code",
        "oa_branch_id",
        "oa_broker_name",
        "product_total_share",
        "client_confirm_share",
        "oa_broker_product_share"
    )

    df_110_6 = tmp4.alias("t") \
        .select(
        "t.filing_code",
        "t.oa_branch_id",
        "t.oa_broker_name",
        "t.product_total_share",
        "t.client_confirm_share",
        when(
            col("t.PRODUCT_TOTAL_SHARE") != lit(0),
            coalesce(col("t.oa_broker_product_share"), lit(0)) / col("t.PRODUCT_TOTAL_SHARE")
        ).otherwise(lit(0)).alias("oa_broker_name_prop")
    )

    return_to_hive(
        spark=spark,
        df_result=df_110_6,
        target_table="ddw.TMP_COCKPIT_00110_6",
        insert_mode="overwrite"
    )

    """
    CF_BUSIMG.TMP_COCKPIT_00110_7
    """

    tmp = spark.table("edw.h15_client_sett").alias("t") \
        .filter(
        (col("busi_date").between(v_begin_date, v_end_date))
    ).join(
        other=spark.table("ddw.T_COCKPIT_00095_1").alias("a"),
        on=(
                (col("t.fund_account_id") == col("a.fund_account_id")) &
                (col("a.month_id") == i_busi_month)
        ),
        how="inner"
    ).groupBy(
        "a.filing_code",
        "a.product_name"
    ).agg(
        sum(
            when(
                v_trade_days > 0,
                col("t.rights") / v_trade_days
            ).otherwise(0)
        ).alias("avg_rights")
    ).select(
        "a.filing_code",
        "a.product_name",
        "avg_rights"
    )

    tmp1 = spark.table("ddw.T_COCKPIT_00100") \
        .filter(col("busi_month") == i_busi_month).alias("t") \
        .join(
        other=tmp.alias("a"),
        on=(col("t.filing_code") == col("a.filing_code")),
        how="left"
    ).join(
        other=df_110_6.alias("b"),
        on=(
                (col("t.filing_code") == col("b.filing_code")) &
                (col("t.OA_BRANCH_ID") == col("b.oa_branch_id")) &
                (col("t.OA_BROKER_NAME") == col("b.oa_broker_name"))
        ),
        how="left"
    ).select(
        "t.filing_code",
        "t.product_name",
        "t.OA_BRANCH_ID",
        "oa_branch_name",
        "t.OA_BROKER_ID",
        "oa_broker_name",
        (coalesce(col("a.avg_rights"), lit(0))
         * (1 - col("t.OA_BRANCH_ADVISOR_RATE"))
         * coalesce(col("b.oa_broker_name_prop"), lit(0))).alias("sales_product_avg_rights"),  # 销售产品
        (coalesce(col("a.avg_rights"), lit(0))
         * col("t.OA_BRANCH_ADVISOR_RATE")
         * col("t.OA_BROKER_ADVISOR_RATE")).alias("advisor_avg_rights"),  # 推荐投顾
        (coalesce(col("b.client_confirm_share"), lit(0))).alias("sales_share"),  # 销售份额
        "t.ADVISOR_NAME",  # 推荐投顾名称
        (coalesce(col("a.avg_rights"), lit(0))).alias("fof_avg_rights")  # FOF产品落地经纪端本月日均权益
    )

    df_110_7 = tmp1.alias("t") \
        .select(
        "t.filing_code",
        "t.product_name",
        "t.OA_BRANCH_ID",
        "oa_branch_name",
        "t.OA_BROKER_ID",
        "t.oa_broker_name",
        "t.sales_product_avg_rights",
        "t.advisor_avg_rights",
        (col("t.SALES_PRODUCT_AVG_RIGHTS") + col("t.ADVISOR_AVG_RIGHTS")).alias("SUM_AVG_RIGHTS"),
        "t.sales_share",
        "t.ADVISOR_NAME",
        "t.fof_avg_rights"
    )

    return_to_hive(
        spark=spark,
        df_result=df_110_7,
        target_table="ddw.TMP_COCKPIT_00110_7",
        insert_mode="overwrite"
    )