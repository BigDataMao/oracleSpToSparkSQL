# -*- coding: utf-8 -*-
import logging
from pyspark.sql.functions import sum, desc, row_number
from pyspark.sql.window import Window

from utils.date_utils import get_date_period_and_days
from utils.task_env import *

logger = Config().get_logger()


@log
def p_cockpit_bu_anal_resp_top_pd(spark, busi_date):
    """
    经营分析-分管部门-成交品种排名落地
    :param spark: SparkSession对象
    :param busi_date: 业务日期,格式为"YYYYMMDD"
    :return: None
    """
    i_month_id = busi_date[:6]
    (
        v_being_date,
        v_end_date,
        _
    ) = get_date_period_and_days(
        spark=spark,
        begin_month=i_month_id,
        end_month=i_month_id,
        is_trade_day=True
    )
    df_hold_balance = spark.table("edw.h15_hold_balance")
    df_fund_account = spark.table("edw.h12_fund_account")
    df_ctp_branch_oa_rela = spark.table("ddw.t_ctp_branch_oa_rela")
    df_product = spark.table("edw.h13_product")
    df_oa_branch = spark.table("ddw.t_oa_branch")

    df_tmp = df_hold_balance.alias("t") \
        .join(
        other=df_fund_account.alias("b"),
        on=(
                col("t.fund_account_id") == col("b.fund_account_id")
        ),
        how="left"
    ).join(
        other=df_ctp_branch_oa_rela.alias("c"),
        on=(
                col("b.branch_id") == col("c.ctp_branch_id")
        ),
        how="inner"
    ).join(
        other=df_product.alias("d"),
        on=(
                (col("t.product_id") == col("d.product_id")) &
                (col("t.market_id") == col("d.market_id")) &
                (col("t.trade_type") == col("d.trade_type"))
        ),
        how="left"
    ).join(
        other=df_oa_branch.alias("e"),
        on=(
                col("c.oa_branch_id") == col("e.departmentid")
        ),
        how="left"
    ).filter(
        (col("t.busi_date").between(lit(v_being_date), lit(v_end_date))) &
        (col("c.oa_branch_id").isNotNull()) &
        (col("e.business_line_id").isNotNull())
    ).groupBy(
        col("t.product_id"),
        col("d.product_name"),
        col("e.respons_line_id")
    ).agg(
        sum(col("t.done_amt")).alias("done_amount"),
        sum(col("t.done_sum")).alias("done_money"),
    ).select(
        col("t.product_id"),
        col("d.product_name"),
        col("e.respons_line_id"),
        col("done_amount"),
        col("done_money")
    ).alias("df_tmp")

    df_tmp1 = df_tmp.alias("t") \
        .select(
        lit(i_month_id).alias("busi_month"),
        col("respons_line_id"),
        col("product_id"),
        col("product_name"),
        col("done_amount"),
        col("done_money")
    ).alias("df_tmp1")

    # 对聚合后的结果进行窗口函数排序
    window_spec1 = Window.partitionBy("respons_line_id").orderBy(desc("done_amount"))
    window_spec2 = Window.partitionBy("respons_line_id").orderBy(desc("done_money"))

    df_y = df_tmp1.withColumn(
        "rank_done_amount", row_number().over(window_spec1)
    ).withColumn(
        "rank_done_money", row_number().over(window_spec2)
    ).select(
        col("busi_month"),
        col("respons_line_id"),
        col("product_id"),
        col("product_name"),
        (col("done_amount") / 10000).alias("done_amount"),  # 万手
        (col("done_money") / 100000000).alias("done_money"),  # 亿元
        col("rank_done_amount"),
        col("rank_done_money")
    )

    return_to_hive(
        spark=spark,
        df_result=df_y,
        target_table="ddw.t_cockpit_bu_anal_resp_top_pd",
        insert_mode="overwrite"
    )
