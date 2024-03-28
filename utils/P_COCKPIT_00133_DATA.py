# -*- coding: utf-8 -*-
"""
经纪业务收入、权益情况-数据落地
"""
import logging

from pyspark.sql.functions import col, lit, min, max, count, sum, coalesce

from utils.task_env import return_to_hive, update_dataframe


def p_cockpit_00133_data(spark, busi_date):
    logging.basicConfig(level=logging.INFO)

    i_month_id = busi_date[:6]

    df_date_trade = spark.table("edw.t10_pub_date") \
        .filter(
        (col("busi_date").substr(1, 6) == i_month_id) &
        (col("market_no") == "1") &
        (col("trade_flag") == "1")
    ).agg(
        min("busi_date").alias("v_begin_trade_date"),
        max("busi_date").alias("v_end_trade_date"),
        count("*").alias("v_trade_days")
    )

    first_row_trade = df_date_trade.first()
    v_begin_trade_date = first_row_trade["v_begin_trade_date"]
    v_end_trade_date = first_row_trade["v_end_trade_date"]
    v_trade_days = first_row_trade["v_trade_days"]

    df_date = spark.table("edw.t10_pub_date") \
        .filter(
        (col("busi_date").substr(1, 6) == i_month_id) &
        (col("market_no") == "1")
    ).agg(
        min("busi_date").alias("v_begin_date"),
        max("busi_date").alias("v_end_date")
    )

    first_row = df_date.first()
    v_begin_date = first_row["v_begin_date"]
    v_end_date = first_row["v_end_date"]

    logging.info(
        u"""
        v_begin_trade_date: %s
        v_end_trade_date:   %s
        v_trade_days:       %s
        v_begin_date:       %s
        v_end_date:         %s
        参数处理完毕
        """,
        v_begin_trade_date, v_end_trade_date, v_trade_days, v_begin_date, v_end_date
    )

    # ------------------- 参数处理完毕 -------------------

    df_113_m = spark.table("ddw.v_oa_branch") \
        .withColumn("busi_month", lit(i_month_id))

    return_to_hive(
        spark=spark,
        df_result=df_113_m,
        target_table="ddw.T_COCKPIT_00133",
        insert_mode="overwrite",
        partition_column="busi_month",
        partition_value=i_month_id
    )

    df_113_m = spark.table("ddw.T_COCKPIT_00133") \
        .filter(col("busi_month") == i_month_id)

    """
    财务内核表-调整前 cf_busimg.t_cockpit_00127
    经纪业务收入-留存手续费收入 REMAIN_TRANSFEE_INCOME
    经纪业务收入-交易所减免净收入 MARKET_REDUCE_INCOME
    经纪业务收入-利息净收入 CLEAR_INTEREST_INCOME
    """

    df_127 = spark.table("ddw.t_cockpit_00127")
    df_b = spark.table("ddw.T_YY_BRANCH_OA_RELA")

    df_y = df_127.join(df_b, df_127["book_id"] == df_b["yy_book_id"]) \
        .filter(col("month_id") == i_month_id) \
        .groupby("oa_branch_id", "oa_branch_name") \
        .agg(sum("b6").alias("REMAIN_TRANSFEE_INCOME"),
             sum("b8").alias("MARKET_REDUCE_INCOME"),
             sum("b7").alias("CLEAR_INTEREST_INCOME"))

    df_113_m = update_dataframe(
        df_to_update=df_113_m,
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=["REMAIN_TRANSFEE_INCOME", "MARKET_REDUCE_INCOME", "CLEAR_INTEREST_INCOME"]
    )

    """
    经纪业务收入=留存手续费收入+交易所手续费减免净收入+利息净收入
    """

    df_113_m = df_113_m \
        .withColumn("FUTU_INCOME_new",
                    coalesce(col("REMAIN_TRANSFEE_INCOME"), lit(0)) +
                    coalesce(col("MARKET_REDUCE_INCOME"), lit(0)) +
                    coalesce(col("CLEAR_INTEREST_INCOME")), lit(0)
                    ) \
        .drop("FUTU_INCOME") \
        .withColumnRenamed("FUTU_INCOME_new", "FUTU_INCOME")

    """更新
    CF_BUSIMG.T_COCKPIT_00122  投资咨询基本信息维护参数表-主表
    CF_BUSIMG.T_COCKPIT_00122_1  投资咨询基本信息-内核分配比例-表1
    交易咨询-交易咨询收入   TRANS_CONSULT_INCOME  (投资咨询服务费总额：)
    交易咨询-销售收入  SALES_INCOME (投资咨询内核表——销售部门收入，业务单位作为销售部门时的收入)
    """


