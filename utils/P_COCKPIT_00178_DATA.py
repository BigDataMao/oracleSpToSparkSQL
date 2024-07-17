# -*- coding: utf-8 -*-
import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, greatest, lit, regexp_replace, round, when

from config import Config
from utils.date_utils import get_date_period_and_days
from utils.task_env import log, return_to_hive

logger = Config().get_logger()


@log
def p_cockpit_00178_data(spark: SparkSession, i_month_id):
    """
    IB协同收入统计汇总表-落地数据
    :param spark: SparkSession对象
    :param i_month_id: 月份ID,格式为"YYYYMM"
    :return:
    """

    days = get_date_period_and_days(
        spark=spark,
        busi_month=i_month_id,
        is_trade_day=True
    )

    v_begin_trade_date = days[0]
    v_end_trade_date = days[1]
    v_trade_days = days[2]

    df_tmp_109_1 = spark.table("ddw.T_COCKPIT_00107").alias("t") \
        .filter(
        (col("t.service_type") == "1")
    ).dropDuplicates(
        ["regist_date", "fund_account_id", "branch_id", "branch_name"]
    ).select(
        col("t.regist_date").alias("busi_date"),
        col("t.fund_account_id"),
        col("t.branch_id"),
        col("t.branch_name")
    ).alias("a").join(
        spark.table("edw.h15_client_sett").alias("t"),
        col("t.fund_account_id") == col("a.fund_account_id"),
        "inner"
    ).filter(
        (col("t.busi_date").between(v_begin_trade_date, v_end_trade_date))
    ).groupBy(
        "a.branch_id",
        "a.branch_name"
    ).agg(
        sum(
            when(
                col("t.busi_date") == v_begin_trade_date,
                col("t.yes_rights")
            ).otherwise(0)
        ).alias("yes_rights"),
        sum(
            when(
                col("t.busi_date") == v_end_trade_date,
                col("t.rights")
            ).otherwise(0)
        ).alias("end_rights"),
        sum(
            when(
                lit(v_trade_days) > 0,
                col("t.rights") / v_trade_days
            ).otherwise(0)
        ).alias("avg_rights"),
        sum(
            col("t.transfee") + col("t.delivery_transfee") + col("t.strikefee")
            - col("t.market_transfee") - col("t.market_delivery_transfee") - col("t.market_strikefee")
        ).alias("remain_transfee")
    )

    logger.info("开始计算-IB协同收入")

    df_tmp_109_2 = spark.table("ddw.T_COCKPIT_00107").alias("t") \
        .filter(
        (col("t.service_type") == "1")
    ).dropDuplicates(
        ["regist_date", "fund_account_id"]
    ).select(
        col("t.regist_date").alias("busi_date"),
        col("t.fund_account_id")
    ).alias("a").join(
        spark.table("edw.h15_client_sett").alias("t"),
        col("t.fund_account_id") == col("a.fund_account_id"),
        "inner"
    ).filter(
        (col("t.busi_date").between(v_begin_trade_date, v_end_trade_date))
    ).groupBy(
        "t.busi_date",
        "t.fund_account_id"
    ).agg(
        sum(
            col("t.transfee") + col("t.delivery_transfee") + col("t.strikefee")
            - col("t.market_transfee") - col("t.market_delivery_transfee") - col("t.market_strikefee")
        ).alias("remain_transfee")
    ).alias("a").join(
        spark.table("ddw.T_COCKPIT_00107").alias("t"),
        col("t.fund_account_id") == col("a.fund_account_id"),
        "inner"
    ).select(
        col("a.busi_date"),
        col("t.branch_id"),
        col("t.branch_name"),
        col("t.fund_account_id"),
        (col("a.remain_transfee") * col("t.coope_income_reate")).alias("coope_income")
    ).alias("t").groupBy(
        "t.branch_id",
        "t.branch_name"
    ).agg(
        sum("t.coope_income").alias("coope_income")
    )

    logger.info("开始插入数据")

    df_result = df_tmp_109_1.alias("t").join(
        df_tmp_109_2.alias("a"),
        col("t.branch_id") == col("a.branch_id"),
        "left"
    ).select(
        lit(i_month_id).alias("month_id"),
        col("t.branch_id"),
        col("t.yes_rights"),
        col("t.end_rights"),
        col("t.avg_rights"),
        col("t.remain_transfee"),
        col("a.coope_income")
    )

    return_to_hive(
        spark,
        df_result,
        "ddw.T_COCKPIT_00178",
        "overwrite"
    )

