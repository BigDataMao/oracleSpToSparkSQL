# -*- coding: utf-8 -*-
"""
客户分析落地表(客户分析-业务条线-)
"""
import logging
from datetime import datetime, timedelta

from pyspark.sql import Window
from pyspark.sql.functions import col, lit, sum, rank

from utils.date_utils import get_mon_sun_str, get_busi_week_int
from utils.task_env import return_to_hive, update_dataframe


def p_cockpit_anal_line_top_data(spark, busi_date):
    v_busi_year = busi_date[:4]
    v_rank_no = 9

    # 获取给定日期所在年的第几周
    v_busi_week = get_busi_week_int(busi_date)
    # 找到给定日期所在周的星期一和星期日
    v_begin_date, v_end_date = get_mon_sun_str(busi_date)

    # TODO: CF_BUSIMG.T_COCKPIT_CLIENT_ANAL_LINE_TOP做成分区表,并且分区字段为busi_year, busi_week

    """
    指标类型(1:入金前9名，2：出金前9名，3：盈利前9名，4：亏损前9名，5：收入贡献前9名，6：成交量前9名，7：成交额前9名)
    """

    df_sett = spark.table("edw.h15_client_sett") \
        .filter(
        (col("busi_date").between(v_begin_date, v_end_date))
    )
    df_hold_balance = spark.table("edw.h15_hold_balance") \
        .filter(
        (col("busi_date").between(v_begin_date, v_end_date))
    )
    df_fund_account = spark.table("edw.h12_fund_account")
    df_relo = spark.table("ddw.t_ctp_branch_oa_rela") \
        .filter(
        (col("oa_branch_id").isNotNull())
    )
    df_oa_branch = spark.table("ddw.t_oa_branch") \
        .filter(
        (col("BUSINESS_LINE_ID").isNotNull())
    )

    logging.info("第1-5个指标开始计算")
    tmp = df_sett.alias("t") \
        .join(df_fund_account.alias("b"), col("t.fund_account") == col("b.fund_account"), "left") \
        .join(df_relo.alias("c"), col("b.branch_id") == col("c.ctp_branch_id"), "inner") \
        .join(df_oa_branch.alias("d"), col("c.oa_branch_id") == col("d.oa_branch_id"), "inner") \
        .groupBy("t.fund_account_id", "b.client_name", "d.business_line_id") \
        .agg(
        sum("t.fund_in").alias("fund_in"),
        sum("t.fund_out").alias("fund_out"),
        sum("t.today_profit").alias("today_profit"),
        sum(
            col("t.transfee") + col("t.strikefee") + col("t.delivery_transfee")
            - col("t.market_transfee") - col("t.market_delivery_transfee") - col("t.market_strikefee")
        ).alias("remain_transfee")
    ) \
        .select(
        col("t.fund_account_id"),
        col("b.client_name"),
        col("d.business_line_id"),
        col("fund_in"),
        col("fund_out"),
        col("today_profit"),
        col("remain_transfee")
    )

    """
    1:入金前9名，2：出金前9名，3：盈利前9名，4：亏损前9名，5：收入贡献前9名
    这5项都需要用df_sett当主表
    """
    tmp_rank = tmp.alias("t") \
        .select(
        col("t.fund_account_id"),
        col("t.client_name"),
        col("t.business_line_id"),
        col("fund_in"),
        col("fund_out"),
        col("today_profit"),
        col("remain_transfee"),
        rank().over(Window.partitionBy("t.business_line_id").orderBy(col("t.fund_in").desc())).alias("rank_no_1"),
        rank().over(Window.partitionBy("t.business_line_id").orderBy(col("t.fund_out").desc())).alias("rank_no_2"),
        # TODO: 亏损前9名的计算逻辑待核实,而且我没筛选,后面再筛
        rank().over(Window.partitionBy("t.business_line_id").orderBy(col("t.today_profit").desc())).alias("rank_no_3"),
        rank().over(Window.partitionBy("t.business_line_id").orderBy(col("t.today_profit").asc())).alias("rank_no_4"),
        rank().over(Window.partitionBy("t.business_line_id").orderBy(col("t.remain_transfee").desc())).alias("rank_no_5")
    )

    def write_to_hive(num):
        num = str(num)
        tmp_result = tmp_rank.alias("t") \
            .select(
            lit(v_busi_week).alias("busi_week"),
            lit(v_busi_year).alias("busi_year"),
            col("t.business_line_id").alias("business_line_id"),
            lit(v_begin_date).alias("begin_date"),
            lit(v_end_date).alias("end_date"),
            col("t.fund_account_id").alias("client_id"),
            col("t.client_name").alias("client_name"),
            col("t.rank_no_" + num).alias("rank_no"),
            lit(num).alias("index_type")
        ).filter(
            col("t.rank_no") <= v_rank_no
        )

        return_to_hive(
            spark=spark,
            df_result=tmp_result,
            target_table="ddw.t_cockpit_client_analyse_top",
            insert_mode="append",
            partition_column=["busi_year", "busi_week"],
            partition_value=[v_busi_year, v_busi_week]
        )

        logging.info("本次任务共7个指标,已完成第{}个指标".format(num))

    for i in range(1, 6):  # 1-5
        write_to_hive(i)

    logging.info("第6-7个指标开始计算")

    """
    6：成交量前9名，7：成交额前9名
    这两项需要用df_hold_balance当主表
    """

    tmp = df_hold_balance.alias("t") \
        .join(df_fund_account.alias("b"), col("t.fund_account") == col("b.fund_account"), "left") \
        .join(df_relo.alias("c"), col("b.branch_id") == col("c.ctp_branch_id"), "inner") \
        .join(df_oa_branch.alias("d"), col("c.oa_branch_id") == col("d.oa_branch_id"), "inner") \
        .groupBy("t.fund_account_id", "b.client_name", "d.business_line_id") \
        .agg(
        sum("t.done_amt").alias("done_amt"),
        sum("t.done_sum").alias("done_sum")) \
        .select(
        col("t.fund_account_id"),
        col("b.client_name"),
        col("d.business_line_id"),
        col("done_amt"),
        col("done_sum"))

    tmp_rank = tmp.alias("t") \
        .select(
        col("t.fund_account_id"),
        col("t.client_name"),
        col("t.business_line_id"),
        col("done_amt"),
        col("done_sum"),
        rank().over(Window.partitionBy("t.business_line_id").orderBy(col("t.done_amt").desc())).alias("rank_no_6"),
        rank().over(Window.partitionBy("t.business_line_id").orderBy(col("t.done_sum").desc())).alias("rank_no_7"))

    for i in range(6, 8):  # 6-7
        write_to_hive(i)

    logging.info("p_cockpit_anal_line_top_data任务执行完成")
    logging.info("本次任务为:客户分析落地表(客户分析-业务条线-)")