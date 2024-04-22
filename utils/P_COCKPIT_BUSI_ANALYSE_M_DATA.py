# -*- coding: utf-8 -*-

from pyspark.sql.functions import sum

from utils.date_utils import *
from utils.task_env import *


@log
def p_cockpit_busi_analyse_m_data(spark, busi_date):
    """
    经营分析-业务单位-按月落地
    """

    v_begin_date = busi_date[:4] + "0101"
    v_end_date, v_now_begin_date, v_now_trade_days = get_date_period_and_days(
        spark=spark,
        busi_month=busi_date[:6],
        end_date=datetime.now().strftime("%Y%m%d"),
        is_trade_day=True
    )
    v_new_begin_date = busi_date[:6] + "01"
    v_new_end_date = v_end_date
    v_now_end_date = v_end_date

    # 初始化数据
    df_m = spark.table("ddw.t_oa_branch").alias("t") \
        .filter(
        col("t.canceled").isNull()
    ).select(
        lit(busi_date).alias("BUSI_MONTH"),
        col("t.departmentid").alias("OA_BRANCH_ID")
    )

    return_to_hive(
        spark=spark,
        df_result=df_m,
        target_table="ddw.T_COCKPIT_BUSI_ANALYSE_M",
        insert_mode="overwrite",
        partition_column="BUSI_MONTH",
        partition_value=busi_date[:6]
    )

    df_m = spark.table("ddw.T_COCKPIT_BUSI_ANALYSE_M").filter(
        col("BUSI_MONTH") == busi_date[:6]
    )

    # 财务指标 TODO
    # 收入结构 TODO
    # 业务指标-日均权益
    tmp_new = spark.table("ddw.h15_client_sett").alias("t") \
        .filter(
        (col("t.busi_date").between(v_begin_date, v_end_date))
    ).join(
        other=spark.table("ddw.h12_fund_account").alias("b"),
        on=col("t.fund_account") == col("b.fund_account"),
        how="left"
    ).join(
        other=spark.table("ddw.t_ctp_branch_oa_rela").alias("c"),
        on=col("b.branch_id") == col("c.ctp_branch_id"),
        how="inner"
    ).filter(
        col("c.oa_branch_id").isNotNull()
    ).groupBy(
        col("t.fund_account_id"),
        when(
            (col("b.open_date").between(v_new_begin_date, v_new_end_date)),
            lit(1)
        ).otherwise(lit(0)).alias("is_new_flag"),
        col("c.oa_branch_id")
    ).agg(
        sum("t.rights").alias("sum_rights")
    ).select(
        col("t.fund_account_id"),
        col("is_new_flag"),
        col("c.oa_branch_id"),
        col("sum_rights")
    )

    tmp_result = tmp_new.alias("t") \
        .select(
        col("t.oa_branch_id"),
        when(
            (col("t.is_new_flag") == 1) & (lit(v_now_trade_days) != 0),
            col("t.sum_rights") / v_now_trade_days
        ).otherwise(lit(0)).alias("AVG_RIGHTS_NEW"),
        when(
            (col("t.is_new_flag") == 0) & (lit(v_now_trade_days) != 0),
            col("t.sum_rights") / v_now_trade_days
        ).otherwise(lit(0)).alias("AVG_RIGHTS_STOCK"),
        when(
            (lit(v_now_trade_days) != 0),
            col("t.sum_rights") / v_now_trade_days
        ).otherwise(lit(0)).alias("sum_avg_rights")
    )

    df_y = tmp_result.alias("t") \
        .select(
        col("t.oa_branch_id"),
        col("t.AVG_RIGHTS_STOCK"),
        when(
            col("t.sum_avg_rights") != 0,
            col("t.AVG_RIGHTS_STOCK") / col("t.sum_avg_rights")
        ).otherwise(lit(0)).alias("AVG_RIGHTS_STOCK_PROP"),
        col("t.AVG_RIGHTS_NEW"),
        when(
            col("t.sum_avg_rights") != 0,
            col("t.AVG_RIGHTS_NEW") / col("t.sum_avg_rights")
        ).otherwise(lit(0)).alias("AVG_RIGHTS_NEW_PROP")
    )

    df_m = update_dataframe(
        df_to_update=df_m,
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=[
            "AVG_RIGHTS_STOCK",
            "AVG_RIGHTS_STOCK_PROP",
            "AVG_RIGHTS_NEW",
            "AVG_RIGHTS_NEW_PROP"
        ]
    )

    # 业务指标-成交量
    # 业务指标-成交额

    tmp_new = spark.table("ddw.h15_hold_balance").alias("t") \
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
    ).filter(
        col("c.oa_branch_id").isNotNull()
    ).groupBy(
        col("t.fund_account_id"),
        when(
            (col("b.open_date").between(v_new_begin_date, v_new_end_date)),
            lit(1)
        ).otherwise(lit(0)).alias("is_new_flag"),
        col("c.oa_branch_id")
    ).agg(
        sum("t.done_amt").alias("done_amount"),
        sum("t.done_sum").alias("done_money")
    ).select(
        col("t.fund_account_id"),
        col("is_new_flag"),
        col("c.oa_branch_id"),
        col("done_amount"),
        col("done_money")
    )

    tmp_result = tmp_new.alias("t") \
        .select(
        col("t.oa_branch_id"),
        when(
            (col("t.is_new_flag") == 1) & (lit(v_now_trade_days) != 0),
            col("t.done_amount")
        ).otherwise(lit(0)).alias("DONE_AMOUNT_NEW"),
        when(
            (col("t.is_new_flag") == 0) & (lit(v_now_trade_days) != 0),
            col("t.done_amount")
        ).otherwise(lit(0)).alias("DONE_AMOUNT_STOCK"),
        when(
            (col("t.is_new_flag") == 1) & (lit(v_now_trade_days) != 0),
            col("t.done_money")
        ).otherwise(lit(0)).alias("DONE_MONEY_NEW"),
        when(
            (col("t.is_new_flag") == 0) & (lit(v_now_trade_days) != 0),
            col("t.done_money")
        ).otherwise(lit(0)).alias("DONE_MONEY_STOCK"),
        col("t.done_amount").alias("SUM_DONE_AMOUNT"),
        col("t.done_money").alias("SUM_DONE_MONEY")
    )

    df_y = tmp_result.alias("t") \
        .select(
        col("t.oa_branch_id"),
        col("t.DONE_AMOUNT_STOCK"),
        when(
            col("t.SUM_DONE_AMOUNT") != 0,
            col("t.DONE_AMOUNT_STOCK") / col("t.SUM_DONE_AMOUNT")
        ).otherwise(lit(0)).alias("DONE_AMOUNT_STOCK_PROP"),
        col("t.DONE_AMOUNT_NEW"),
        when(
            col("t.SUM_DONE_AMOUNT") != 0,
            col("t.DONE_AMOUNT_NEW") / col("t.SUM_DONE_AMOUNT")
        ).otherwise(lit(0)).alias("DONE_AMOUNT_NEW_PROP"),
        col("t.DONE_MONEY_STOCK"),
        when(
            col("t.SUM_DONE_MONEY") != 0,
            col("t.DONE_MONEY_STOCK") / col("t.SUM_DONE_MONEY")
        ).otherwise(lit(0)).alias("DONE_MONEY_STOCK_PROP"),
        col("t.DONE_MONEY_NEW"),
        when(
            col("t.SUM_DONE_MONEY") != 0,
            col("t.DONE_MONEY_NEW") / col("t.SUM_DONE_MONEY")
        ).otherwise(lit(0)).alias("DONE_MONEY_NEW_PROP")
    )

    df_m = update_dataframe(
        df_to_update=df_m,
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=[
            "DONE_AMOUNT_STOCK",
            "DONE_AMOUNT_STOCK_PROP",
            "DONE_AMOUNT_NEW",
            "DONE_AMOUNT_NEW_PROP",
            "DONE_MONEY_STOCK",
            "DONE_MONEY_STOCK_PROP",
            "DONE_MONEY_NEW",
            "DONE_MONEY_NEW_PROP"
        ]
    )

    # 市场成交量，成交额
    first_row = spark.table(
        "ddw.t_cockpit_industry_trad"
    ).filter(
        col("etl_month") == busi_date[:6]
    ).select(
        (coalesce(sum(col("trad_num")), lit(0)) * 2).alias("total_done_amount"),
        (coalesce(sum(col("trad_amt")), lit(0)) * 2 * 100000000).alias("total_done_money")
    ).first()

    v_total_done_amount = first_row["total_done_amount"]
    v_total_done_money = first_row["total_done_money"]

    # 市场客户权益-日均
    tmp = spark.table("ddw.t_cockpit_industry_manage").alias("t") \
        .filter(
        (col("t.etl_month") == busi_date[:6]) & (col("t.index_name") == "客户权益")
    ).select(
        (coalesce(sum(col("t.index_value")), lit(0)) * 100000000).alias("rights")
    )

    v_total_rights = tmp.alias("t").select(
        when(
            lit(v_now_trade_days) != 0,
            col("t.rights") / v_now_trade_days
        ).otherwise(lit(0)).alias("total_rights")
    ).first()["total_rights"]

    # 市场手续费收入
    v_total_index_value = spark.table("ddw.t_cockpit_industry_manage").alias("t") \
        .filter(
        (col("t.etl_month") == busi_date[:6]) &
        (col("t.index_name") == "手续费收入")
    ).select(
        (coalesce(sum(col("t.index_value")), lit(0)) * 100000000).alias("total_index_value")
    ).first()["total_index_value"]

    # 市场地位 --日均权益，手续费

    tmp = spark.table("ddw.h15_client_sett").alias("t") \
        .filter(
        (col("t.busi_date").between(v_now_begin_date, v_now_end_date))
    ).join(
        other=spark.table("ddw.h12_fund_account").alias("b"),
        on=col("t.fund_account") == col("b.fund_account"),
        how="left"
    ).join(
        other=spark.table("ddw.t_ctp_branch_oa_rela").alias("c"),
        on=col("b.branch_id") == col("c.ctp_branch_id"),
        how="inner"
    ).filter(
        col("c.oa_branch_id").isNotNull()
    ).groupBy(
        col("t.fund_account_id"),
        col("c.oa_branch_id")
    ).agg(
        sum("t.rights").alias("rights"),
        sum(
            coalesce(col("t.transfee"), lit(0)) +
            coalesce(col("t.delivery_transfee"), lit(0)) +
            coalesce(col("t.strikefee"), lit(0))
        ).alias("transfee")
    ).select(
        col("t.fund_account_id"),
        col("c.oa_branch_id"),
        col("rights"),
        col("transfee")
    )

    tmp1 = tmp.alias("t") \
        .groupBy(
        col("t.oa_branch_id")
    ).agg(
        sum(
            when(
                lit(v_now_trade_days) != 0,
                col("t.rights") / v_now_trade_days
            ).otherwise(lit(0))
        ).alias("AVG_RIGHTS"),
        sum(col("t.transfee")).alias("transfee")
    ).select(
        col("t.oa_branch_id"),
        col("AVG_RIGHTS"),
        col("transfee")
    )

    df_y = tmp1.alias("t") \
        .select(
        col("t.oa_branch_id"),
        when(
            col("t.AVG_RIGHTS") != 0,
            col("t.AVG_RIGHTS") / v_total_rights
        ).otherwise(lit(0)).alias("AVG_RIGHTS_MARKET_RATE"),
        when(
            col("v_total_index_value") != 0,
            col("t.transfee") / v_total_index_value
        ).otherwise(lit(0)).alias("FUTU_TRANS_INCOME_MARKET_RATE")
    )

    df_m = update_dataframe(
        df_to_update=df_m,
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=[
            "AVG_RIGHTS_MARKET_RATE",
            "FUTU_TRANS_INCOME_MARKET_RATE"
        ]
    )

    # 市场地位 --日均权益，手续费

    tmp = spark.table("ddw.h15_hold_balance").alias("t") \
        .filter(
        (col("t.busi_date").between(v_now_begin_date, v_now_end_date))
    ).join(
        other=spark.table("ddw.h12_fund_account").alias("b"),
        on=col("t.fund_account_id") == col("b.fund_account_id"),
        how="left"
    ).join(
        other=spark.table("ddw.t_ctp_branch_oa_rela").alias("c"),
        on=col("b.branch_id") == col("c.ctp_branch_id"),
        how="inner"
    ).filter(
        col("c.oa_branch_id").isNotNull()
    ).groupBy(
        col("t.fund_account_id"),
        col("c.oa_branch_id")
    ).agg(
        sum("t.done_amt").alias("done_amount"),
        sum("t.done_sum").alias("done_money")
    ).select(
        col("t.fund_account_id"),
        col("c.oa_branch_id"),
        col("done_amount"),
        col("done_money")
    )

    tmp1 = tmp.alias("t") \
        .groupBy(
        col("t.oa_branch_id")
    ).agg(
        sum(col("t.done_amount")).alias("done_amount"),
        sum(col("t.done_money")).alias("done_money")
    ).select(
        col("t.oa_branch_id"),
        col("done_amount"),
        col("done_money")
    )

    df_y = tmp1.alias("t") \
        .select(
        col("t.oa_branch_id"),
        when(
            v_total_done_amount != 0,
            col("t.done_amount") / v_total_done_amount
        ).otherwise(lit(0)).alias("DONE_AMOUNT_MARKET_RATE"),
        when(
            v_total_done_money != 0,
            col("t.done_money") / v_total_done_money
        ).otherwise(lit(0)).alias("DONE_MONEY_MAREKT_RATE")
    )

    df_m = update_dataframe(
        df_to_update=df_m,
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=[
            "DONE_MONEY_MAREKT_RATE",
            "DONE_AMOUNT_MARKET_RATE"
        ]
    )

    return_to_hive(
        spark=spark,
        df_result=df_m,
        target_table="ddw.T_COCKPIT_BUSI_ANALYSE_M",
        insert_mode="overwrite",
        partition_column="BUSI_MONTH",
        partition_value=busi_date[:6]
    )