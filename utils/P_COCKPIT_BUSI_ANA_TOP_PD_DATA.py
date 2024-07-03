# -*- coding: utf-8 -*-

from pyspark.sql import Window
from pyspark.sql.functions import sum, rank

from utils.date_utils import *
from utils.task_env import *

logger = Config().get_logger()


@log
def p_cockpit_busi_ana_top_pd_data(spark, busi_date):
    """
    经营分析-业务单位-成交品种排名落地数据
    :param spark: SparkSession对象
    :param busi_date: 业务日期,格式为"YYYYMMDD"
    :return: None
    """
    logger.info("p_cockpit_busi_ana_top_pd_data开始计算")

    (
        v_begin_date,
        v_end_date,
        _
    ) = get_date_period_and_days(
        spark=spark,
        busi_month=busi_date[:6],
        is_trade_day=True
    )

    tmp = spark.table("edw.h15_hold_balance").alias("t") \
        .filter(
        (col("t.busi_date").between(v_begin_date, v_end_date))
    ).join(
        other=spark.table("edw.h12_fund_account").alias("b"),
        on=col("t.fund_account_id") == col("b.fund_account_id"),
        how="left"
    ).join(
        other=spark.table("ddw.t_ctp_branch_oa_rela").alias("c"),
        on=col("b.branch_id") == col("c.ctp_branch_id"),
        how="inner"
    ).join(
        other=spark.table("edw.h13_product").alias("d"),
        on=(
            (col("t.product_id") == col("d.product_id")) &
            (col("t.market_id") == col("d.market_id")) &
            (col("t.trade_type") == col("d.trade_type"))
        ),
        how="left"
    ).filter(
        col("c.oa_branch_id").isNotNull()
    ).groupBy(
        "t.product_id", "d.product_name", "c.oa_branch_id"
    ).agg(
        sum("t.done_amt").alias("done_amount"),
        sum("t.done_sum").alias("done_money")
    ).select(
        col("t.product_id"),
        col("d.product_name"),
        col("c.oa_branch_id"),
        col("done_amount"),
        col("done_money")
    )

    df_result = tmp.alias("t") \
        .select(
        lit(busi_date[:6]).alias("BUSI_MONTH"),
        col("t.oa_branch_id"),
        col("t.product_id"),
        col("t.product_name"),
        (col("t.done_amount") / 10000).alias("done_amount"),  # 万手
        (col("t.done_money") / 100000000).alias("done_money"),  # 亿元,
        rank().over(
            Window.partitionBy("t.oa_branch_id").orderBy(col("t.done_amount").desc())
        ).alias("RANK_DONE_AMOUNT"),
        rank().over(
            Window.partitionBy("t.oa_branch_id").orderBy(col("t.done_money").desc())
        ).alias("RANK_DONE_MONEY")
    )

    return_to_hive(
        spark=spark,
        df_result=df_result,
        target_table="ddw.T_COCKPIT_BUSI_ANALYSE_TOP_PD",
        insert_mode="overwrite"
    )

    logger.info("p_cockpit_busi_ana_top_pd_data计算完成")
    logger.info("本次任务为: 经营分析-业务单位-成交品种排名落地数据")