# -*- coding: utf-8 -*-
"""
经营分析-业务单位-成交品种排名落地数据
"""
import logging
from datetime import datetime, timedelta

from pyspark.sql import Window
from pyspark.sql.functions import col, lit, sum, rank

from utils.date_utils import *
from utils.task_env import *


def p_cockpit_busi_ana_top_pd_data(spark, busi_date):
    logging.info("p_cockpit_busi_ana_top_pd_data开始计算")

    """
  select min(t.busi_date), max(t.busi_date)
    into v_begin_date, v_end_date
    from cf_sett.t_pub_date t
   where substr(t.busi_date,1,6)=I_MONTH_ID
     and t.market_no = '1'
     and t.trade_flag = '1';

  delete from CF_BUSIMG.T_COCKPIT_BUSI_ANALYSE_TOP_PD t
   where t.busi_month = I_MONTH_ID;
  commit;


  INSERT INTO CF_BUSIMG.T_COCKPIT_BUSI_ANALYSE_TOP_PD
    (BUSI_MONTH,
     OA_BRANCH_ID,
     PRODUCT_ID,
     PRODUCT_NAME,
     DONE_AMOUNT,
     DONE_MONEY,
     RANK_DONE_AMOUNT,
     RANK_DONE_MONEY)
    with tmp as
     (SELECT t.product_id,
             d.product_name,
             C.OA_BRANCH_ID,
             SUM(T.Done_Amt) AS Done_amount,
             SUM(t.done_sum) AS done_money
        FROM CF_SETT.t_Hold_Balance T
        LEFT JOIN CF_SETT.T_FUND_ACCOUNT B
          ON T.FUND_ACCOUNT_ID = B.FUND_ACCOUNT_ID
       INNER JOIN CF_BUSIMG.T_CTP_BRANCH_OA_RELA C
          ON B.BRANCH_ID = C.CTP_BRANCH_ID
        left join cf_sett.t_product d
          on t.product_id = d.product_id
         and t.market_id = d.market_id
         and t.trade_type = d.trade_type
       WHERE T.BUSI_DATE BETWEEN v_begin_date AND v_end_date
         AND C.OA_BRANCH_ID IS NOT NULL
       GROUP BY t.product_id, d.product_name, C.OA_BRANCH_ID)
    select I_MONTH_ID as busi_month,
           t.oa_branch_id,
           t.product_id,
           t.product_name,
           t.Done_amount,
           t.done_money,
           row_number() over(partition by t.oa_branch_id order by t.Done_amount desc) as RANK_DONE_AMOUNT,
           row_number() over(partition by t.oa_branch_id order by t.done_money desc) RANK_DONE_MONEY
      from tmp t;
    """

    (
        v_begin_date,
        v_end_date,
        _
    ) = get_date_period_and_days(
        spark=spark,
        busi_month=busi_date[:6],
        is_trade_day=True
    )

    # TODO: CF_BUSIMG.T_COCKPIT_BUSI_ANALYSE_TOP_PD,分区字段,busi_month

    tmp = spark.table("ddw.h15_hold_balance").alias("t") \
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
        other=spark.table("ddw.h13_product").alias("d"),
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
        col("t.done_amount"),
        col("t.done_money"),
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
        insert_mode="overwrite",
        partition_column="BUSI_MONTH",
        partition_value=busi_date[:6]
    )

    logging.info("p_cockpit_busi_ana_top_pd_data计算完成")
    logging.info("本次任务为: 经营分析-业务单位-成交品种排名落地数据")