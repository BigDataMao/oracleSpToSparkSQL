# -*- coding: utf-8 -*-

from pyspark.sql.functions import sum, concat, substring, length, regexp_extract

from utils.date_utils import *
from utils.task_env import *


@log
def p_brp_06010_d_data(spark, busi_date):
    """
    交易统计表-日和阶段
    """

    v_begin_date = busi_date
    v_end_date = busi_date

    v_dratio = spark.table("ods.t_Cspmg_Csperson_Fee").alias("a") \
        .filter(
        (col("a.fee_type") == "1003") &
        (col("a.use_status") == "0") &
        (col("a.data_level") == "1")
    ).agg(
        max("a.fee").alias("fee")
    ).select(
        when(col("fee").isNull(), 5.5).otherwise(col("fee")) / 100000000
    ).first()[0]

    (
        v_begin_trade_date,
        v_end_trade_date,
        _
    ) = get_date_period_and_days(
        spark=spark,
        begin_date=v_begin_date,
        end_date=v_end_date,
        is_trade_day=True
    )

    df_tmp_brp_06010_hold = spark.table("edw.h15_hold_detail").alias("a") \
        .filter(
        col("a.busi_date") == v_end_trade_date
    ).join(
        spark.table("edw.h12_fund_account").alias("b"),
        col("a.fund_account_id") == col("b.fund_account_id"),
        "inner"
    ).join(
        spark.table("edw.h17_security").alias("d"),
        col("a.security_id") == col("d.security_id"),
        "inner"
    ).groupBy(
        col("a.busi_date").alias("busi_date_during"),
        col("a.client_id"),
        col("a.branch_id"),
        col("a.FUND_ACCOUNT_ID"),
        col("a.money_type"),
        col("a.trade_type"),
        col("a.trade_prop"),
        col("d.market_id"),
        col("d.product_id"),
        col("a.security_id"),
        col("d.delivery_date")
    ).agg(
        sum(
            when(col("a.busi_date") == v_end_trade_date, col("a.amount")).otherwise(0)
        ).alias("hold_amount"),
        sum(
            when(col("a.busi_date") == v_end_trade_date,
                 col("a.amount") * col("a.sett_price") * col("a.hand_amount")).otherwise(0)
        ).alias("hold_money"),
        sum(
            when(col("a.busi_date") == v_end_trade_date, col("a.amount")).otherwise(0)
        ).alias("hold_amount_avg"),
        sum(
            when(col("a.busi_date") == v_end_trade_date, col("a.hold_profit_bydate")).otherwise(0)
        ).alias("hold_profit_bydate"),
        sum(
            when(col("a.busi_date") == v_end_trade_date, col("a.hold_profit_bytrade")).otherwise(0)
        ).alias("hold_profit_bytrade"),
        sum(
            when(
                (col("a.busi_date") == v_end_trade_date) &
                (col("a.trade_type").isin("12", "21")) &
                (col("a.bs_direction") == "0"),
                col("a.amount") * col("a.sett_price") * col("a.hand_amount")
            ).otherwise(0)
        ).alias("buy_hold_money_qq"),
        sum(
            when(
                (col("a.busi_date") == v_end_trade_date) &
                (col("a.trade_type").isin("12", "21")) &
                (col("a.bs_direction") == "1"),
                col("a.amount") * col("a.sett_price") * col("a.hand_amount")
            ).otherwise(0)
        ).alias("sell_hold_money_qq"),
        sum(
            when(col("a.busi_date") == v_end_trade_date, col("a.margin")).otherwise(0)
        ).alias("margin"),
        sum(
            when(col("a.busi_date") == v_end_trade_date, col("a.market_margin")).otherwise(0)
        ).alias("market_margin")
    )

    return_to_hive(
        spark=spark,
        df_result=df_tmp_brp_06010_hold,
        target_table="cf_busimg.tmp_brp_06010_hold",
        insert_mode="overwrite"
    )

    df_tmp_brp_06010_hold_fee = spark.table("edw.h15_hold_balance").alias("a") \
        .filter(
        col("a.busi_date").between(v_begin_trade_date, v_end_trade_date)
    ).join(
        spark.table("edw.h12_fund_account").alias("b"),
        col("a.fund_account_id") == col("b.fund_account_id"),
        "inner"
    ).join(
        spark.table("edw.h17_security").alias("d"),
        col("a.security_id") == col("d.security_id"),
        "inner"
    ).fillna(0).groupBy(
        col("a.busi_date").alias("busi_date_during"),
        col("a.client_id"),
        col("a.branch_id"),
        col("a.FUND_ACCOUNT_ID"),
        col("a.money_type"),
        col("a.trade_type"),
        col("a.trade_prop"),
        col("d.market_id"),
        col("d.product_id"),
        col("a.security_id"),
        col("d.delivery_date")
    ).agg(
        sum(
            col("a.transfee") + col("a.settfee") + col("a.holdmovefee")
        ).alias("transfee"),
        sum(
            col("a.market_transfee") + col("a.market_settfee") + col("a.market_holdmovefee")
        ).alias("market_transfee"),
        sum(col("a.margin")).alias("margin"),
        sum(col("a.market_margin")).alias("market_margin"),
        sum(col("a.today_profit")).alias("total_profit"),
        sum(col("a.close_profit_bydate")).alias("close_profit_bydate"),
        sum(col("a.close_profit_bytrade")).alias("close_profit_bytrade"),
        sum(col("a.hold_profit_bydate")).alias("hold_profit_bydate"),
        sum(col("a.hold_profit_bytrade")).alias("hold_profit_bytrade"),
        sum(
            when(
                (col("a.trade_type").isin("12", "21")) &
                (col("a.bs_direction") == "0"),
                col("a.market_value")
            ).otherwise(0)
        ).alias("buy_hold_money_qq"),
        sum(
            when(
                (col("a.trade_type").isin("12", "21")) &
                (col("a.bs_direction") == "1"),
                col("a.market_value")
            ).otherwise(0)
        ).alias("sell_hold_money_qq"),
        sum(
            when(
                col("a.trade_prop") == "1",
                col("a.margin")
            ).otherwise(0)
        ).alias("margin_tj"),
        sum(
            when(
                col("a.trade_prop") == "2",
                col("a.margin")
            ).otherwise(0)
        ).alias("margin_tl"),
        sum(
            when(
                col("a.trade_prop") == "3",
                col("a.margin")
            ).otherwise(0)
        ).alias("margin_tb"),
        sum(
            when(
                col("a.bs_direction") == "0",
                col("a.hold_amount")
            ).otherwise(0)
        ).alias("hold_amount_buy"),
        sum(
            when(
                col("a.bs_direction") == "1",
                col("a.hold_amount")
            ).otherwise(0)
        ).alias("hold_amount_sell"),
        sum(col("a.hold_amount")).alias("hold_amount"),
        sum(col("a.hold_money")).alias("hold_money"),
        sum(col("a.hold_amount")).alias("hold_amount_avg"),
        sum(col("a.close_money_today")).alias("close_money_today"),
        sum(col("a.close_amount_today")).alias("close_amount_today"),
        sum(col("a.done_sum")).alias("done_sum"),
        sum(col("a.done_amt")).alias("done_amt"),
        sum(col("a.done_sum")).alias("done_money_avg"),
        sum(col("a.done_amt")).alias("done_amount_avg")
    )

    return_to_hive(
        spark=spark,
        df_result=df_tmp_brp_06010_hold_fee,
        target_table="ddw.tmp_brp_06010_hold_fee",
        insert_mode="overwrite"
    )

    df_tmp_brp_06010_done = spark.table("edw.h14_done").alias("a") \
        .filter(
        col("a.busi_date").between(v_begin_trade_date, v_end_trade_date)
    ).join(
        other=spark.table("edw.h12_fund_account").alias("b"),
        on=col("a.fund_account_id") == col("b.fund_account_id"),
        how="inner"
    ).join(
        other=spark.table("edw.h17_security").alias("d"),
        on=col("a.security_id") == col("d.security_id"),
        how="inner"
    ).fillna().groupBy(
        col("a.busi_date").alias("busi_date_during"),
        col("a.client_id"),
        col("a.branch_id"),
        col("a.FUND_ACCOUNT_ID"),
        col("a.money_type"),
        col("a.trade_type"),
        col("a.trade_prop"),
        col("d.market_id"),
        col("d.product_id"),
        col("a.security_id"),
        col("d.delivery_date")
    ).agg(
        countDistinct(col("a.busi_date")).alias("have_trade_num"),
        sum(col("a.transfee")).alias("transfee"),
        sum(col("a.market_transfee")).alias("market_transfee"),
        sum(col("a.transfee") - col("a.market_transfee")).alias("remain_transfee"),
        sum(
            when(col("a.trade_order") == "3", col("a.transfee")).otherwise(0)
        ).alias("transfee_pj"),
        sum(
            when(col("a.trade_order") == "3", col("a.market_transfee")).otherwise(0)
        ).alias("market_transfee_pj"),
        sum(
            when(
                col("a.trade_order") == "3",
                col("a.transfee") - col("a.market_transfee")
            ).otherwise(0)
        ).alias("remain_transfee_pj"),
        sum(col("a.done_amount")).alias("done_amount"),
        sum(
            when(col("a.trade_order") == "3", col("a.done_amount")).otherwise(0)
        ).alias("done_amount_pj"),
        sum(col("a.done_money")).alias("done_money"),
        sum(
            when(col("a.trade_order") == "3", col("a.done_money")).otherwise(0)
        ).alias("done_money_pj"),
        sum(col("a.done_amount")).alias("done_amount_avg"),
        sum(col("a.done_money")).alias("done_money_avg"),
        sum(col("a.SOCRT_OPENFEE")).alias("JINGSHOUFEI"),
        sum(col("a.SETTLEMENTFEE")).alias("JIESUANFEI"),
        sum(
            when(
                (col("a.bs_direction") == "0") & (col("a.trade_type") != "11"),
                col("a.done_money")
            ).otherwise(0)
        ).alias("OPT_PREMIUM_PAY"),
        sum(
            when(
                (col("a.bs_direction") == "1") & (col("a.trade_type") != "11"),
                col("a.done_money")
            ).otherwise(0)
        ).alias("OPT_PREMIUM_INCOME"),
        sum(col("a.add_fee1")).alias("add_fee1"),
        sum(col("a.add_fee2")).alias("add_fee2"),
        sum(
            when(col("a.force_flag") == "1", col("a.done_amount")).otherwise(0)
        ).alias("done_amount_qp"),
        sum(
            when(col("a.force_flag") == "1", col("a.done_money")).otherwise(0)
        ).alias("done_money_qp"),
        sum(
            when(col("a.force_flag") == "1", 1).otherwise(0)
        ).alias("force_number")
    )

    return_to_hive(
        spark=spark,
        df_result=df_tmp_brp_06010_done,
        target_table="ddw.tmp_brp_06010_done",
        insert_mode="overwrite"
    )

    v_count = spark.table("edw.h14_delivery").alias("t") \
        .filter(
        (col("t.busi_date").between(v_begin_date, v_end_date)) &
        (col("t.data_source") == "CTP2")
    ).count()

    df_tmp_brp_06010_deliv = spark.table("edw.h14_delivery").alias("a") \
        .filter(
        when(
            v_count > 0,
            (col("a.sett_date").between(v_begin_date, v_end_date))
        ).otherwise(
            (col("a.busi_date").between(v_begin_date, v_end_date))
        )
    ).join(
        other=spark.table("edw.h12_fund_account").alias("b"),
        on=col("a.fund_account_id") == col("b.fund_account_id"),
        how="inner"
    ).join(
        other=spark.table("edw.h17_security").alias("d"),
        on=col("a.security_id") == col("d.security_id"),
        how="inner"
    ).fillna(0).groupBy(
        when(v_count > 0, col("a.sett_date")).otherwise(col("a.busi_date")).alias("busi_date_during"),
        col("a.client_id"),
        col("a.branch_id"),
        col("a.FUND_ACCOUNT_ID"),
        col("a.money_type"),
        col("a.trade_type"),
        col("a.trade_prop"),
        col("d.market_id"),
        col("d.product_id"),
        col("a.security_id"),
        col("d.delivery_date")
    ).agg(
        sum(col("a.transfee")).alias("transfee"),
        sum(col("a.market_transfee")).alias("market_transfee"),
        sum(col("a.transfee") - col("a.market_transfee")).alias("remain_transfee"),
        sum(col("a.pay_amount")).alias("delivery_amount"),
        sum(col("a.done_amt")).alias("done_amt"),
        sum(
            when(col("a.busi_date") == v_end_trade_date, col("a.margin")).otherwise(0)
        ).alias("margin"),
        sum(
            when(col("a.busi_date") == v_end_trade_date, col("a.market_margin")).otherwise(0)
        ).alias("market_margin"),
        sum(
            when(
                (col("a.busi_date") == v_end_trade_date) & (col("a.trade_prop") == "1"),
                col("a.margin")
            ).otherwise(0)
        ).alias("margin_tj"),
        sum(
            when(
                (col("a.busi_date") == v_end_trade_date) & (col("a.trade_prop") == "2"),
                col("a.margin")
            ).otherwise(0)
        ).alias("margin_tl"),
        sum(
            when(
                (col("a.busi_date") == v_end_trade_date) & (col("a.trade_prop") == "3"),
                col("a.margin")
            ).otherwise(0)
        ).alias("margin_tb")
    )

    return_to_hive(
        spark=spark,
        df_result=df_tmp_brp_06010_deliv,
        target_table="ddw.tmp_brp_06010_deliv",
        insert_mode="overwrite"
    )

    df_tmp_brp_06010_execute = spark.table("edw.h14_delivery").alias("a") \
        .filter(
        col("a.busi_date").between(v_begin_trade_date, v_end_trade_date) &
        (col("a.trade_type") == "12") &
        (col("a.strike_type") == "0")
    ).join(
        other=spark.table("edw.h12_fund_account").alias("b"),
        on=col("a.fund_account_id") == col("b.fund_account_id"),
        how="inner"
    ).join(
        other=spark.table("edw.h17_security").alias("d"),
        on=col("a.security_id") == col("d.security_id"),
        how="inner"
    ).fillna(0).groupBy(
        col("a.busi_date").alias("busi_date_during"),
        col("a.client_id"),
        col("a.branch_id"),
        col("a.FUND_ACCOUNT_ID"),
        col("a.money_type"),
        col("a.trade_type"),
        col("a.trade_prop"),
        col("d.market_id"),
        col("d.product_id"),
        col("a.security_id"),
        col("d.delivery_date")
    ).agg(
        sum(col("a.strikefee")).alias("transfee"),
        sum(col("a.market_strikefee")).alias("market_transfee"),
        sum(col("a.strikefee") - col("a.market_strikefee")).alias("remain_transfee"),
        sum(col("a.execute_transfee")).alias("transfee_xq"),
        sum(col("a.market_execute_transfee")).alias("market_transfee_xq"),
        sum(col("a.performfee")).alias("transfee_ly"),
        sum(col("a.market_performfee")).alias("market_transfee_ly"),
        sum(
            when(
                (col("a.bs_direction") == "0"),
                col("a.strike_qty")
            ).otherwise(0)
        ).alias("strike_qty_xq"),
        sum(
            when(
                (col("a.bs_direction") == "1"),
                col("a.strike_qty")
            ).otherwise(0)
        ).alias("strike_qty_ly"),
        sum(col("a.optstrike_profit")).alias("optstrike_profit"),
        sum(
            when(
                (col("a.busi_date") == col("d.delivery_date")),
                col("a.strike_qty")
            ).otherwise(0)
        ).alias("strike_qty_dq"),
        sum(
            when(
                (col("a.busi_date") == col("d.delivery_date")),
                col("a.strike_qty") * col("a.strike_price") * col("a.hand_amount")
            ).otherwise(0)
        ).alias("strike_money_dq")
    )

    return_to_hive(
        spark=spark,
        df_result=df_tmp_brp_06010_execute,
        target_table="ddw.tmp_brp_06010_execute",
        insert_mode="overwrite"
    )

    df_tmp_brp_06010_execute_01 = spark.table("edw.h14_delivery").alias("a") \
        .filter(
        col("a.busi_date").between(v_begin_trade_date, v_end_trade_date) &
        (col("a.trade_type") == "21")
    ).join(
        other=spark.table("edw.h12_fund_account").alias("b"),
        on=col("a.fund_account_id") == col("b.fund_account_id"),
        how="inner"
    ).join(
        other=spark.table("edw.h17_security").alias("d"),
        on=col("a.security_id") == col("d.security_id"),
        how="inner"
    ).fillna(0).groupBy(
        col("a.busi_date").alias("busi_date_during"),
        col("a.client_id"),
        col("a.branch_id"),
        col("a.FUND_ACCOUNT_ID"),
        col("a.money_type"),
        col("a.trade_type"),
        col("a.trade_prop"),
        col("d.market_id"),
        col("d.product_id"),
        col("a.security_id"),
        col("d.delivery_date")
    ).agg(
        sum(col("a.strikefee")).alias("transfee"),
        sum(col("a.market_strikefee")).alias("market_transfee"),
        sum(col("a.strikefee") - col("a.market_strikefee")).alias("remain_transfee"),
        sum(
            when(
                col("a.bs_direction") == "0",
                col("a.strikefee")
            ).otherwise(0)
        ) + sum(
            when(
                (col("a.bs_direction") == "1") & (col("a.market_id") == "CFFEX"),
                col("a.strikefee")
            ).otherwise(0)
        ).alias("transfee_xq"),
        sum(
            when(
                (col("a.bs_direction") == "0"),
                col("a.market_strikefee")
            ).otherwise(0)
        ) + sum(
            when(
                (col("a.bs_direction") == "1") & (col("a.market_id") == "CFFEX"),
                col("a.market_strikefee")
            ).otherwise(0)
        ).alias("market_transfee_xq"),
        sum(
            when(
                (col("a.bs_direction") == "1") & (col("a.market_id") != "CFFEX"),
                col("a.strikefee")
            ).otherwise(0)
        ).alias("transfee_ly"),
        sum(
            when(
                (col("a.bs_direction") == "1") & (col("a.market_id") != "CFFEX"),
                col("a.market_strikefee")
            ).otherwise(0)
        ).alias("market_transfee_ly"),
        sum(
            when(
                (col("a.bs_direction") == "0"),
                col("a.strike_qty")
            ).otherwise(0)
        ) + sum(
            when(
                (col("a.bs_direction") == "1") & (col("a.market_id") == "CFFEX"),
                col("a.strike_qty")
            ).otherwise(0)
        ).alias("strike_qty_xq"),
        sum(
            when(
                (col("a.bs_direction") == "1") & (col("a.market_id") != "CFFEX"),
                col("a.strike_qty")
            ).otherwise(0)
        ).alias("strike_qty_ly"),
        sum(col("a.optstrike_profit")).alias("optstrike_profit"),
        lit(0).alias("strikefrozenmargin"),
        lit(0).alias("exchstrikefrozenmargin")
    )

    df_tmp_brp_06010_execute_02 = spark.table("edw.h14_delivery").alias("a") \
        .filter(
        col("a.busi_date").between(v_begin_trade_date, v_end_trade_date) &
        (col("a.trade_type") == "21")
    ).join(
        other=spark.table("edw.h12_fund_account").alias("b"),
        on=col("a.fund_account_id") == col("b.fund_account_id"),
        how="inner"
    ).join(
        other=spark.table("edw.h17_security").alias("d"),
        on=col("a.security_id") == col("d.security_id"),
        how="inner"
    ).fillna(0).groupBy(
        col("a.busi_date").alias("busi_date_during"),
        col("a.client_id"),
        col("a.branch_id"),
        col("a.FUND_ACCOUNT_ID"),
        col("a.money_type"),
        col("a.trade_type"),
        col("a.trade_prop"),
        col("d.market_id"),
        col("d.product_id"),
        col("a.security_id"),
        col("d.delivery_date")
    ).agg(
        lit(0).alias("transfee"),
        lit(0).alias("market_transfee"),
        lit(0).alias("remain_transfee"),
        lit(0).alias("transfee_xq"),
        lit(0).alias("market_transfee_xq"),
        lit(0).alias("transfee_ly"),
        lit(0).alias("market_transfee_ly"),
        lit(0).alias("strike_qty_xq"),
        lit(0).alias("strike_qty_ly"),
        lit(0).alias("optstrike_profit"),
        sum(
            when(
                (col("a.busi_date") == v_end_trade_date),
                col("a.strikefrozenmargin")
            ).otherwise(0)
        ).alias("strikefrozenmargin"),
        sum(
            when(
                (col("a.busi_date") == v_end_trade_date),
                col("a.exchstrikefrozenmargin")
            ).otherwise(0)
        ).alias("exchstrikefrozenmargin")
    )

    df_tmp_brp_06010_execute = df_tmp_brp_06010_execute_01.unionall(df_tmp_brp_06010_execute_02)

    return_to_hive(
        spark=spark,
        df_result=df_tmp_brp_06010_execute,
        target_table="ddw.tmp_brp_06010_execute",
        insert_mode="overwrite"
    )

    df_x = df_tmp_brp_06010_hold.alias("w1").select(
        col("w1.busi_date_during"),
        col("w1.client_id"),
        col("w1.branch_id"),
        col("w1.FUND_ACCOUNT_ID"),
        col("w1.money_type"),
        col("w1.trade_type"),
        col("w1.trade_prop"),
        col("w1.market_id"),
        col("w1.product_id"),
        col("w1.security_id"),
        col("w1.delivery_date")
    ).distinct().union(
        df_tmp_brp_06010_hold_fee.alias("w11").select(
            col("w11.busi_date_during"),
            col("w11.client_id"),
            col("w11.branch_id"),
            col("w11.FUND_ACCOUNT_ID"),
            col("w11.money_type"),
            col("w11.trade_type"),
            col("w11.trade_prop"),
            col("w11.market_id"),
            col("w11.product_id"),
            col("w11.security_id"),
            col("w11.delivery_date")
        ).distinct()
    ).union(
        df_tmp_brp_06010_done.alias("w2").select(
            col("w2.busi_date_during"),
            col("w2.client_id"),
            col("w2.branch_id"),
            col("w2.FUND_ACCOUNT_ID"),
            col("w2.money_type"),
            col("w2.trade_type"),
            col("w2.trade_prop"),
            col("w2.market_id"),
            col("w2.product_id"),
            col("w2.security_id"),
            col("w2.delivery_date")
        ).distinct()
    ).union(
        df_tmp_brp_06010_deliv.alias("w3").select(
            col("w3.busi_date_during"),
            col("w3.client_id"),
            col("w3.branch_id"),
            col("w3.FUND_ACCOUNT_ID"),
            col("w3.money_type"),
            col("w3.trade_type"),
            col("w3.trade_prop"),
            col("w3.market_id"),
            col("w3.product_id"),
            col("w3.security_id"),
            col("w3.delivery_date")
        ).distinct()
    ).union(
        df_tmp_brp_06010_execute.alias("w4").select(
            col("w4.busi_date_during"),
            col("w4.client_id"),
            col("w4.branch_id"),
            col("w4.FUND_ACCOUNT_ID"),
            col("w4.money_type"),
            col("w4.trade_type"),
            col("w4.trade_prop"),
            col("w4.market_id"),
            col("w4.product_id"),
            col("w4.security_id"),
            col("w4.delivery_date")
        ).distinct()
    )

    df_tmp_brp_06010 = df_x.alias("x").join(
        other=df_tmp_brp_06010_hold.alias("a"),
        on=[
            (col("x.client_id") == col("a.client_id")),
            (col("x.FUND_ACCOUNT_ID") == col("a.FUND_ACCOUNT_ID")),
            (col("x.busi_date_during") == col("a.busi_date_during")),
            (col("x.security_id") == col("a.security_id")),
            (col("x.money_type") == col("a.money_type")),
            (col("x.trade_type") == col("a.trade_type")),
            (col("x.trade_prop") == col("a.trade_prop")),
            (col("x.branch_id") == col("a.branch_id"))
        ],
        how="left"
    ).join(
        other=df_tmp_brp_06010_hold_fee.alias("a1"),
        on=[
            (col("x.client_id") == col("a1.client_id")),
            (col("x.FUND_ACCOUNT_ID") == col("a1.FUND_ACCOUNT_ID")),
            (col("x.busi_date_during") == col("a1.busi_date_during")),
            (col("x.security_id") == col("a1.security_id")),
            (col("x.money_type") == col("a1.money_type")),
            (col("x.trade_type") == col("a1.trade_type")),
            (col("x.trade_prop") == col("a1.trade_prop")),
            (col("x.branch_id") == col("a1.branch_id"))
        ],
        how="left"
    ).join(
        other=df_tmp_brp_06010_done.alias("b"),
        on=[
            (col("x.client_id") == col("b.client_id")),
            (col("x.FUND_ACCOUNT_ID") == col("b.FUND_ACCOUNT_ID")),
            (col("x.busi_date_during") == col("b.busi_date_during")),
            (col("x.security_id") == col("b.security_id")),
            (col("x.money_type") == col("b.money_type")),
            (col("x.trade_type") == col("b.trade_type")),
            (col("x.trade_prop") == col("b.trade_prop")),
            (col("x.branch_id") == col("b.branch_id"))
        ],
        how="left"
    ).join(
        other=df_tmp_brp_06010_deliv.alias("c"),
        on=[
            (col("x.client_id") == col("c.client_id")),
            (col("x.FUND_ACCOUNT_ID") == col("c.FUND_ACCOUNT_ID")),
            (col("x.busi_date_during") == col("c.busi_date_during")),
            (col("x.security_id") == col("c.security_id")),
            (col("x.money_type") == col("c.money_type")),
            (col("x.trade_type") == col("c.trade_type")),
            (col("x.trade_prop") == col("c.trade_prop")),
            (col("x.branch_id") == col("c.branch_id"))
        ],
        how="left"
    ).join(
        other=df_tmp_brp_06010_execute.alias("d"),
        on=[
            (col("x.client_id") == col("d.client_id")),
            (col("x.FUND_ACCOUNT_ID") == col("d.FUND_ACCOUNT_ID")),
            (col("x.busi_date_during") == col("d.busi_date_during")),
            (col("x.security_id") == col("d.security_id")),
            (col("x.money_type") == col("d.money_type")),
            (col("x.trade_type") == col("d.trade_type")),
            (col("x.trade_prop") == col("d.trade_prop")),
            (col("x.branch_id") == col("d.branch_id"))
        ],
        how="left"
    ).fillna().select(
        col("x.busi_date_during"),
        col("x.client_id"),
        col("x.FUND_ACCOUNT_ID"),
        col("x.branch_id"),
        col("x.money_type"),
        col("x.trade_type"),
        col("x.trade_prop"),
        col("x.product_id"),
        col("x.security_id"),
        when(
            col("x.trade_type") == "11",
            concat(lit("1"), substring(col("x.security_id"), -3, 3))
        ).when(
            col("x.trade_type") != "11",
            when(
                col("x.market_id") == "CZCE",
                concat(lit("1"), substring(col("x.security_id"), 3, 3))
            ).when(
                col("x.market_id") == "DCE",
                substring(col("x.security_id"), 2, 4)
            ).when(
                col("x.market_id") == "SSE",
                substring(col("x.security_id"), 8, 4)
            )
        ).alias("DELIV_DATE"),
        col("x.market_id"),
        col("a.hold_amount"),
        col("a.hold_money"),
        col("a.hold_amount_avg"),
        col("a.hold_profit_bydate"),
        col("a.hold_profit_bytrade"),
        when(
            col("x.trade_type") == "21",
            col("a1.buy_hold_money_qq")
        ).when(
            col("x.trade_type") == "12",
            col("a.buy_hold_money_qq")
        ).otherwise(0).alias("BUY_HOLD_MONEY_QQ"),
        when(
            col("x.trade_type") == "21",
            col("a1.sell_hold_money_qq")
        ).when(
            col("x.trade_type") == "12",
            col("a.sell_hold_money_qq")
        ).otherwise(0).alias("SELL_HOLD_MONEY_QQ"),
        (col("a.margin") + col("c.margin")).alias("MARGIN"),
        (col("a.market_margin") + col("c.market_margin")).alias("MARKET_MARGIN"),
        col("b.have_trade_num"),
        col("b.transfee_pj"),
        col("b.market_transfee_pj"),
        col("b.remain_transfee_pj"),
        col("a1.done_amt"),
        col("a1.close_amount_today"),
        col("a1.done_sum"),
        col("a1.close_money_today"),
        col("a1.done_amount_avg"),
        col("a1.done_money_avg"),
        col("c.transfee").alias("DELIV_TRANSFEE"),
        col("c.market_transfee").alias("DELIV_MARKET_TRANSFEE"),
        col("c.remain_transfee").alias("DELIV_REMAIN_TRANSFEE"),
        col("c.delivery_amount"),
        col("c.done_amt"),
        col("d.transfee_xq"),
        col("d.transfee_ly"),
        col("d.market_transfee_xq"),
        col("d.market_transfee_ly"),
        col("d.strike_qty_xq"),
        col("d.strike_qty_ly"),
        col("a1.close_profit_bydate"),
        col("a1.close_profit_bytrade"),
        (col("a1.transfee") + col("c.transfee") + col("d.transfee")).alias("TOTAL_TRANSFEE"),
        (col("a1.market_transfee") + col("c.market_transfee") + col("d.market_transfee")).alias(
            "TOTAL_MARKET_TRANSFEE"),
        (col("a1.transfee") - col("a1.market_transfee") + col("c.remain_transfee") + col("d.remain_transfee")).alias(
            "TOTAL_REMAIN_TRANSFEE"),
        col("a1.total_profit").alias("TOTAL_PROFIT"),
        col("b.SOCRT_OPENFEE").alias("JINGSHOUFEI"),
        col("b.SETTLEMENTFEE").alias("JIESUANFEI"),
        col("b.opt_premium_income"),
        col("b.opt_premium_pay"),
        col("b.add_fee1"),
        col("b.add_fee2"),
        (col("b.opt_premium_income") - col("b.opt_premium_pay")).alias("OPT_PREMIUM_INCOME_PAY"),
        col("d.optstrike_profit"),
        (col("a1.margin_tj") + col("c.margin_tj")).alias("MARGIN_TJ"),
        (col("a1.margin_tl") + col("c.margin_tl")).alias("MARGIN_TL"),
        (col("a1.margin_tb") + col("c.margin_tb")).alias("MARGIN_TB"),
        (col("a1.hold_profit_bytrade") + col("a1.close_profit_bytrade")).alias("FLOAT_PROFIT"),
        col("a1.hold_amount_buy").alias("HOLD_AMOUNT_BUY"),
        col("a1.hold_amount_sell").alias("HOLD_AMOUNT_SELL"),
        col("d.strike_qty_dq").alias("STRIKE_QTY_DQ"),
        col("d.strike_money_dq").alias("STRIKE_MONEY_DQ"),
        col("a1.transfee").alias("TRADE_TRANSFEE"),
        col("a1.market_transfee").alias("MARKET_TRADE_TRANSFEE"),
        (col("a1.transfee") - col("a1.market_transfee")).alias("TRADE_REMAIN_TRANSFEE"),
        col("c.margin").alias("DELIV_MARGIN"),
        col("c.market_margin").alias("DELIV_MARKET_MARGIN"),
        (col("d.transfee_xq") - col("d.market_transfee_xq")).alias("REMAIN_TRANSFEE_XQ"),
        col("b.done_amount_qp").alias("DONE_AMOUNT_QP"),
        col("b.done_money_qp").alias("DONE_MONEY_QP"),
        col("b.force_number").alias("FORCE_NUMBER")
    )

    return_to_hive(
        spark=spark,
        df_result=df_tmp_brp_06010,
        target_table="ddw.tmp_brp_06010",
        insert_mode="overwrite"
    )

    cust_done = df_tmp_brp_06010.alias("a") \
        .join(
        other=spark.table("edw.h12_fund_account").alias("c"),
        on=col("a.fund_account_id") == col("c.fund_account_id"),
        how="left"
    ).fillna().groupBy(
        col("a.busi_date_during").alias("busi_date"),
        col("a.client_id"),
        col("c.branch_id"),
        col("c.client_type"),
        col("c.frozen_status"),
        col("a.money_type"),
        col("a.trade_type"),
        col("a.trade_prop"),
        col("a.product_id"),
        col("a.security_id"),
        col("a.delivery_date"),
        col("c.open_date"),
        col("a.market_id"),
        col("a.fund_account_id")
    ).agg(
        sum(col("a.hold_amount")).alias("hold_amount"),
        sum(col("a.hold_money")).alias("hold_money"),
        sum(col("a.hold_amount_avg")).alias("hold_amount_avg"),
        sum(col("a.hold_profit_bydate")).alias("hold_profit_bydate"),
        sum(col("a.hold_profit_bytrade")).alias("hold_profit_bytrade"),
        sum(col("a.buy_hold_money_qq")).alias("buy_hold_money_qq"),
        sum(col("a.sell_hold_money_qq")).alias("sell_hold_money_qq"),
        sum(col("a.margin")).alias("margin"),
        sum(col("a.market_margin")).alias("market_margin"),
        sum(col("a.have_trade_num")).alias("have_trade_num"),
        sum(col("a.transfee_pj")).alias("transfee_pj"),
        sum(col("a.market_transfee_pj")).alias("market_transfee_pj"),
        sum(col("a.remain_transfee_pj")).alias("remain_transfee_pj"),
        sum(col("a.done_amount")).alias("done_amount"),
        sum(col("a.done_amount_pj")).alias("done_amount_pj"),
        sum(col("a.done_money")).alias("done_money"),
        sum(col("a.done_money_pj")).alias("done_money_pj"),
        sum(col("a.done_amount_avg")).alias("done_amount_avg"),
        sum(col("a.done_money_avg")).alias("done_money_avg"),
        sum(col("a.deliv_transfee")).alias("deliv_transfee"),
        sum(col("a.deliv_market_transfee")).alias("deliv_market_transfee"),
        sum(col("a.deliv_remain_transfee")).alias("deliv_remain_transfee"),
        sum(col("a.delivery_amount")).alias("delivery_amount"),
        sum(col("a.done_amt")).alias("done_amt"),
        sum(col("a.transfee_xq")).alias("transfee_xq"),
        sum(col("a.transfee_ly")).alias("transfee_ly"),
        sum(col("a.market_transfee_xq")).alias("market_transfee_xq"),
        sum(col("a.market_transfee_ly")).alias("market_transfee_ly"),
        sum(col("a.strike_qty_xq")).alias("strike_qty_xq"),
        sum(col("a.strike_qty_ly")).alias("strike_qty_ly"),
        sum(col("a.close_profit_bydate")).alias("close_profit_bydate"),
        sum(col("a.close_profit_bytrade")).alias("close_profit_bytrade"),
        sum(col("a.total_transfee")).alias("total_transfee"),
        sum(col("a.total_market_transfee")).alias("total_market_transfee"),
        sum(col("a.total_remain_transfee")).alias("total_remain_transfee"),
        sum(col("a.total_profit")).alias("total_profit"),
        sum(col("a.jingshoufei")).alias("jingshoufei"),
        sum(col("a.jiesuanfei")).alias("jiesuanfei"),
        sum(col("a.opt_premium_income")).alias("opt_premium_income"),
        sum(col("a.opt_premium_pay")).alias("opt_premium_pay"),
        sum(col("a.add_fee1")).alias("add_fee1"),
        sum(col("a.add_fee2")).alias("add_fee2"),
        sum(col("a.opt_premium_income_pay")).alias("opt_premium_income_pay"),
        sum(col("a.optstrike_profit")).alias("optstrike_profit"),
        sum(col("a.margin_tj")).alias("margin_tj"),
        sum(col("a.margin_tl")).alias("margin_tl"),
        sum(col("a.margin_tb")).alias("margin_tb"),
        sum(col("a.float_profit")).alias("float_profit"),
        sum(col("a.hold_amount_buy")).alias("hold_amount_buy"),
        sum(col("a.hold_amount_sell")).alias("hold_amount_sell"),
        sum(col("a.strike_qty_dq")).alias("strike_qty_dq"),
        sum(col("a.strike_money_dq")).alias("strike_money_dq"),
        sum(col("a.DONE_MONEY") * v_dratio).alias("bzjj"),
        sum(col("a.trade_transfee")).alias("trade_transfee"),
        sum(col("a.market_trade_transfee")).alias("market_trade_transfee"),
        sum(col("a.trade_remain_transfee")).alias("trade_remain_transfee"),
        sum(col("a.deliv_margin")).alias("deliv_margin"),
        sum(col("a.deliv_market_margin")).alias("deliv_market_margin"),
        sum(col("a.remain_transfee_xq")).alias("remain_transfee_xq"),
        sum(col("a.done_amount_qp")).alias("done_amount_qp"),
        sum(col("a.done_money_qp")).alias("done_money_qp"),
        sum(col("a.force_number")).alias("force_number"),
        (sum(col("a.total_profit")) - sum(col("a.total_transfee"))).alias("clear_profit")
    )

    df_t_rpt_06010_d = cust_done.alias("t").join(
        other=spark.table("edw.h17_security").alias("e"),
        on=col("t.security_id") == col("e.security_id"),
        how="left"
    ).join(
        other=spark.table("edw.h16_product").alias("f"),
        on=(
                (col("e.product_id") == col("f.product_id")) &
                (col("e.trade_type") == col("f.trade_type")) &
                (col("e.market_id") == col("f.market_id"))
        ),
        how="left"
    ).select(
        col("t.busi_date"),
        col("t.client_id"),
        col("t.branch_id"),
        col("t.client_type"),
        col("t.frozen_status"),
        col("t.money_type"),
        col("t.trade_type"),
        col("t.trade_prop"),
        col("t.product_id"),
        col("t.security_id"),
        when(
            (length(col("e.delivery_year")) == 4),
            concat(substring(col("e.delivery_year"), 3, 2), col("e.delivery_month"))
        ).when(
            (substring(col("e.delivery_date"), 3, 4) != substring(col("e.end_delivery_date"), 3, 4)),
            substring(col("e.end_delivery_date"), 3, 4)
        ).when(
            (coalesce(col("e.delivery_date"), col("e.end_delivery_date"))).isNotNull(),
            substring(coalesce(col("e.delivery_date"), col("e.end_delivery_date")), 3, 4)
        ).otherwise(
            regexp_extract(col("e.security_id"), "[0-9]+", 1)
        ).alias("DELIV_DATE"),
        col("t.open_date"),
        col("t.market_id"),
        col("t.fund_account_id"),
        col("t.hold_amount"),
        col("t.hold_money"),
        col("t.hold_amount_avg"),
        col("t.hold_profit_bydate"),
        col("t.hold_profit_bytrade"),
        col("t.buy_hold_money_qq"),
        col("t.sell_hold_money_qq"),
        col("t.margin"),
        col("t.market_margin"),
        col("t.have_trade_num"),
        col("t.transfee_pj"),
        col("t.market_transfee_pj"),
        col("t.remain_transfee_pj"),
        col("t.done_amount"),
        col("t.done_amount_pj"),
        col("t.done_money"),
        col("t.done_money_pj"),
        col("t.done_amount_avg"),
        col("t.done_money_avg"),
        col("t.deliv_transfee"),
        col("t.deliv_market_transfee"),
        col("t.deliv_remain_transfee"),
        col("t.delivery_amount"),
        col("t.done_amt"),
        col("t.transfee_xq"),
        col("t.transfee_ly"),
        col("t.market_transfee_xq"),
        col("t.market_transfee_ly"),
        col("t.strike_qty_xq"),
        col("t.strike_qty_ly"),
        col("t.close_profit_bydate"),
        col("t.close_profit_bytrade"),
        col("t.total_transfee"),
        col("t.total_market_transfee"),
        col("t.total_remain_transfee"),
        col("t.total_profit"),
        col("t.jingshoufei"),
        col("t.jiesuanfei"),
        col("t.opt_premium_income"),
        col("t.opt_premium_pay"),
        col("t.add_fee1"),
        col("t.add_fee2"),
        col("t.opt_premium_income_pay"),
        col("t.optstrike_profit"),
        col("t.margin_tj"),
        col("t.margin_tl"),
        col("t.margin_tb"),
        col("t.float_profit"),
        col("t.hold_amount_buy"),
        col("t.hold_amount_sell"),
        col("t.strike_qty_dq"),
        col("t.strike_money_dq"),
        col("t.bzjj"),
        col("t.trade_transfee"),
        col("t.market_trade_transfee"),
        col("t.trade_remain_transfee"),
        col("t.deliv_margin"),
        col("t.deliv_market_margin"),
        col("t.remain_transfee_xq"),
        col("t.done_amount_qp"),
        col("t.done_money_qp"),
        col("t.force_number"),
        col("t.clear_profit")
    )

    df_y = spark.table("edw.h17_security").alias("d").select(
        col("d.security_id"),
        when(
            (length(col("d.delivery_year")) == 4),
            concat(substring(col("d.delivery_year"), 3, 2), col("d.delivery_month"))
        ).when(
            (substring(col("d.delivery_date"), 3, 4) != substring(col("d.end_delivery_date"), 3, 4)),
            substring(col("d.end_delivery_date"), 3, 4)
        ).when(
            (coalesce(col("d.delivery_date"), col("d.end_delivery_date"))).isNotNull(),
            substring(coalesce(col("d.delivery_date"), col("d.end_delivery_date")), 3, 4)
        ).otherwise(
            regexp_extract(col("d.security_id"), "[0-9]+", 1)
        ).alias("DELIV_DATE")
    )

    df_t_rpt_06010_d = update_dataframe(
        df_to_update=df_t_rpt_06010_d,
        df_use_me=df_y,
        join_columns=["security_id"],
        update_columns=["DELIV_DATE"]
    )

    return_to_hive(
        spark=spark,
        df_result=df_t_rpt_06010_d,
        target_table="cf_stat.t_rpt_06010",
        insert_mode="overwrite",
        partition_column="busi_date",
        partition_value=busi_date
    )
