# -*- coding: utf-8 -*-

from pyspark.sql.functions import sum, substring, length, regexp_extract, udf
from pyspark.sql.types import StringType

from utils.date_utils import *
from utils.task_env import *


@log
def p_brp_06010_data(spark, list_pub_date, i_begin_date, i_end_date):
    """
    交易统计表-月度汇总数据生成
    """

    # 注册UDF
    get_trade_date_udf = udf(lambda x, n: get_trade_date(list_pub_date, x, n), StringType())

    v_begin_date = i_begin_date
    v_end_date = i_end_date

    v_begin_trade_date, v_end_trade_date, v_trade_day_count = \
        get_date_period_and_days(
            spark=spark,
            begin_date= i_begin_date,
            end_date= i_end_date,
            is_trade_day= True,
        )

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

    df_tmp_06010_security = spark.table("edw.h17_security").alias("d") \
        .select(
        col("d.security_id"),
        col("d.market_id"),
        col("d.security_name"),
        col("d.product_id"),
        col("d.delivery_date"),
        col("d.trade_type")
    )

    return_to_hive(
        spark=spark,
        df_result=df_tmp_06010_security,
        target_table="ddw.TMP_06010_SECURITY",
        insert_mode="overwrite"
    )

    df_tmp_06010_hold = spark.table("edw.h15_hold_detail").alias("a") \
        .filter(
        (col("a.busi_date") == v_end_trade_date)
    ).join(
        other=df_tmp_06010_security,
        on=(col("a.security_id") == col("d.security_id")),
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
        sum(
            when(
                col("a.busi_date") == v_end_trade_date,
                col("a.amount")
            ).otherwise(0)
        ).alias("hold_amount"),
        sum(
            when(
                col("a.busi_date") == v_end_trade_date,
                col("a.amount") * col("a.sett_price") * col("a.hand_amount")
            ).otherwise(0)
        ).alias("hold_money"),
        sum(
            when(
                col("a.busi_date") == v_end_trade_date,
                col("a.amount")
            ).otherwise(0)
        ) / v_trade_day_count.alias("hold_amount_avg"),
        sum(
            when(
                col("a.busi_date") == v_end_trade_date,
                col("a.hold_profit_bydate")
            ).otherwise(0)
        ).alias("hold_profit_bydate"),
        sum(
            when(
                col("a.busi_date") == v_end_trade_date,
                col("a.hold_profit_bytrade")
            ).otherwise(0)
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
            when(
                col("a.busi_date") == v_end_trade_date,
                col("a.margin")
            ).otherwise(0)
        ).alias("margin"),
        sum(
            when(
                col("a.busi_date") == v_end_trade_date,
                col("a.market_margin")
            ).otherwise(0)
        ).alias("market_margin")
    )

    return_to_hive(
        spark=spark,
        df_result=df_tmp_06010_hold,
        target_table="ddw.TMP_06010_HOLD",
        insert_mode="overwrite"
    )

    df_tmp_06010_hold_fee = spark.table("edw.h15_hold_balance").alias("a") \
        .filter(
        (col("a.busi_date").between(v_begin_date, v_end_date))
    ).join(
        other=df_tmp_06010_security,
        on=(col("a.security_id") == col("d.security_id")),
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
        sum(
            col("a.transfee") + col("a.settfee") + col("a.holdmovefee")
        ).alias("transfee"),
        sum(
            col("a.market_transfee") + col("a.market_settfee") + col("a.market_holdmovefee")
        ).alias("market_transfee"),
        sum(
            when(
                col("a.busi_date") == v_end_trade_date,
                col("a.margin")
            ).otherwise(0)
        ).alias("margin"),
        sum(
            when(
                col("a.busi_date") == v_end_trade_date,
                col("a.market_margin")
            ).otherwise(0)
        ).alias("market_margin"),
        sum(col("a.today_profit")),
        sum(col("a.close_profit_bydate")).alias("close_profit_bydate"),
        sum(
            when(
                col("a.busi_date") == v_end_trade_date,
                col("a.close_profit_bytrade")
            ).otherwise(0)
        ).alias("close_profit_bytrade"),
        sum(col("a.hold_profit_bydate")).alias("hold_profit_bydate"),
        sum(
            when(
                col("a.busi_date") == v_end_trade_date,
                col("a.hold_profit_bytrade")
            ).otherwise(0)
        ).alias("hold_profit_bytrade"),
        sum(
            when(
                (col("a.busi_date") == v_end_trade_date) &
                (col("a.trade_type").isin("12", "21")) &
                (col("a.bs_direction") == "0"),
                col("a.market_value")
            ).otherwise(0)
        ).alias("buy_hold_money_qq"),
        sum(
            when(
                (col("a.busi_date") == v_end_trade_date) &
                (col("a.trade_type").isin("12", "21")) &
                (col("a.bs_direction") == "1"),
                col("a.market_value")
            ).otherwise(0)
        ).alias("sell_hold_money_qq"),
        sum(
            when(
                (col("a.busi_date") == v_end_trade_date) &
                (col("a.trade_prop") == "1"),
                col("a.margin")
            ).otherwise(0)
        ).alias("margin_tj"),
        sum(
            when(
                (col("a.busi_date") == v_end_trade_date) &
                (col("a.trade_prop") == "2"),
                col("a.margin")
            ).otherwise(0)
        ).alias("margin_tl"),
        sum(
            when(
                (col("a.busi_date") == v_end_trade_date) &
                (col("a.trade_prop") == "3"),
                col("a.margin")
            ).otherwise(0)
        ).alias("margin_tb"),
        sum(
            when(
                (col("a.busi_date") == v_end_trade_date) &
                (col("a.bs_direction") == "0"),
                col("a.hold_amount")
            ).otherwise(0)
        ).alias("hold_amount_buy"),
        sum(
            when(
                (col("a.busi_date") == v_end_trade_date) &
                (col("a.bs_direction") == "1"),
                col("a.hold_amount")
            ).otherwise(0)
        ).alias("hold_amount_sell"),
        sum(
            when(
                col("a.busi_date") == v_end_trade_date,
                col("a.hold_amount")
            ).otherwise(0)
        ).alias("hold_amount"),
        sum(
            when(
                col("a.busi_date") == v_end_trade_date,
                col("a.hold_money")
            ).otherwise(0)
        ).alias("hold_money"),
        sum(col("a.hold_amount") / v_trade_day_count).alias("hold_amount_avg"),
        sum(col("a.close_money_today")).alias("close_money_today"),
        sum(col("a.close_amount_today")).alias("close_amount_today"),
        sum(col("a.done_sum")).alias("done_sum"),
        sum(col("a.done_amt")).alias("done_amt"),
        sum(col("a.done_sum") / v_trade_day_count).alias("done_money_avg"),
        sum(col("a.done_amt") / v_trade_day_count).alias("done_amount_avg"),
        sum(col("a.hold_amount")).alias("total_hold_amount")
    )

    return_to_hive(
        spark=spark,
        df_result=df_tmp_06010_hold_fee,
        target_table="ddw.TMP_06010_HOLD_FEE",
        insert_mode="overwrite"
    )

    df_tmp_06010_done = spark.table("edw.h14_done").alias("a") \
        .filter(
        (col("a.busi_date").between(v_begin_date, v_end_date))
    ).join(
        other=df_tmp_06010_security,
        on=(col("a.security_id") == col("d.security_id")),
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
            when(
                col("a.trade_order") == "3",
                col("a.transfee")
            ).otherwise(0)
        ).alias("transfee_pj"),
        sum(
            when(
                col("a.trade_order") == "3",
                col("a.market_transfee")
            ).otherwise(0)
        ).alias("market_transfee_pj"),
        sum(
            when(
                col("a.trade_order") == "3",
                col("a.transfee") - col("a.market_transfee")
            ).otherwise(0)
        ).alias("remain_transfee_pj"),
        sum(col("a.done_amount")).alias("done_amount"),
        sum(
            when(
                col("a.trade_order") == "3",
                col("a.done_amount")
            ).otherwise(0)
        ).alias("done_amount_pj"),
        sum(col("a.done_money")).alias("done_money"),
        sum(
            when(
                col("a.trade_order") == "3",
                col("a.done_money")
            ).otherwise(0)
        ).alias("done_money_pj"),
        sum(col("a.done_amount") / v_trade_day_count).alias("done_amount_avg"),
        sum(col("a.done_money") / v_trade_day_count).alias("done_money_avg"),
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
            when(
                col("a.force_flag") == "1",
                col("a.done_amount")
            ).otherwise(0)
        ).alias("done_amount_qp"),
        sum(
            when(
                col("a.force_flag") == "1",
                col("a.done_money")
            ).otherwise(0)
        ).alias("done_money_qp"),
        sum(
            when(
                col("a.force_flag") == "1",
                1
            ).otherwise(0)
        ).alias("force_number")
    )

    return_to_hive(
        spark=spark,
        df_result=df_tmp_06010_done,
        target_table="ddw.TMP_06010_DONE",
        insert_mode="overwrite"
    )

    df_tmp_06010_deliv = spark.table("edw.h14_delivery").alias("a") \
        .filter(
        (col("a.sett_date").between(v_begin_date, v_end_date))
    ).join(
        other=df_tmp_06010_security,
        on=(col("a.security_id") == col("d.security_id")),
        how="inner"
    ).fillna().groupBy(
        col("a.sett_date").alias("busi_date_during"),
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
            when(
                col("a.sett_date") == v_end_trade_date,
                col("a.margin")
            ).otherwise(0)
        ).alias("margin"),
        sum(
            when(
                col("a.sett_date") == v_end_trade_date,
                col("a.market_margin")
            ).otherwise(0)
        ).alias("market_margin"),
        sum(
            when(
                (col("a.sett_date") == v_end_trade_date) &
                (col("a.trade_prop") == "1"),
                col("a.margin")
            ).otherwise(0)
        ).alias("margin_tj"),
        sum(
            when(
                (col("a.sett_date") == v_end_trade_date) &
                (col("a.trade_prop") == "2"),
                col("a.margin")
            ).otherwise(0)
        ).alias("margin_tl"),
        sum(
            when(
                (col("a.sett_date") == v_end_trade_date) &
                (col("a.trade_prop") == "3"),
                col("a.margin")
            ).otherwise(0)
        ).alias("margin_tb")
    )

    return_to_hive(
        spark=spark,
        df_result=df_tmp_06010_deliv,
        target_table="ddw.TMP_06010_DELIV",
        insert_mode="overwrite"
    )

    df_tmp_06010_execute = spark.table("edw.h14_execute_result").alias("a") \
        .filter(
        (col("a.busi_date").between(v_begin_date, v_end_date)) &
        (col("a.trade_type") == "12") &
        (col("a.strike_type") == "0")
    ).join(
        other=df_tmp_06010_security,
        on=(col("a.security_id") == col("d.security_id")),
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
        sum(col("a.strikefee")).alias("transfee"),
        sum(col("a.market_strikefee")).alias("market_transfee"),
        sum(col("a.strikefee") - col("a.market_strikefee")).alias("remain_transfee"),
        sum(col("a.execute_transfee")).alias("transfee_xq"),
        sum(col("a.market_execute_transfee")).alias("market_transfee_xq"),
        sum(col("a.performfee")).alias("transfee_ly"),
        sum(col("a.market_performfee")).alias("market_transfee_ly"),
        sum(
            when(
                col("a.bs_direction") == "0",
                col("a.strike_qty")
            ).otherwise(0)
        ).alias("strike_qty_xq"),
        sum(
            when(
                col("a.bs_direction") == "1",
                col("a.strike_qty")
            ).otherwise(0)
        ).alias("strike_qty_ly"),
        sum(col("a.optstrike_profit")).alias("optstrike_profit"),
        sum(
            when(
                col("a.busi_date") == col("d.delivery_date"),
                col("a.strike_qty")
            ).otherwise(0)
        ).alias("strike_qty_dq"),
        sum(
            when(
                col("a.busi_date") == col("d.delivery_date"),
                col("a.strike_qty") * col("a.strike_price") * col("a.hand_amount")
            ).otherwise(0)
        ).alias("strike_money_dq")
    )

    return_to_hive(
        spark=spark,
        df_result=df_tmp_06010_execute,
        target_table="ddw.TMP_06010_EXECUTE",
        insert_mode="overwrite"
    )

    total_01 = spark.table("edw.h14_execute_result").alias("a") \
        .filter(
        (get_trade_date_udf(col("a.busi_date"), 1).between(v_begin_date, v_end_date)) &
        (col("a.trade_type") == "21")
    ).join(
        other=df_tmp_06010_security,
        on=(col("a.security_id") == col("d.security_id")),
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
        sum(col("a.strikefee")).alias("transfee"),
        sum(col("a.market_strikefee")).alias("market_transfee"),
        sum(col("a.strikefee") - col("a.market_strikefee")).alias("remain_transfee"),
        sum(
            when(
                col("a.bs_direction") == "0",
                col("a.strikefee")
            ).otherwise(0)
        ).alias("transfee_xq"),
        sum(
            when(
                col("a.bs_direction") == "0",
                col("a.market_strikefee")
            ).otherwise(0)
        ).alias("market_transfee_xq"),
        sum(
            when(
                col("a.bs_direction") == "1",
                col("a.strikefee")
            ).otherwise(0)
        ).alias("transfee_ly"),
        sum(
            when(
                col("a.bs_direction") == "1",
                col("a.market_strikefee")
            ).otherwise(0)
        ).alias("market_transfee_ly"),
        sum(
            when(
                col("a.bs_direction") == "0",
                col("a.strike_qty")
            ).otherwise(0)
        ).alias("strike_qty_xq"),
        sum(
            when(
                col("a.bs_direction") == "1",
                col("a.strike_qty")
            ).otherwise(0)
        ).alias("strike_qty_ly"),
        sum(col("a.optstrike_profit")).alias("optstrike_profit"),
        lit(0).alias("STRIKEFROZENMARGIN"),
        lit(0).alias("EXCHSTRIKEFROZENMARGIN")
    )

    total_02 = spark.table("edw.h14_execute_result").alias("a") \
        .filter(
        (col("a.busi_date").between(v_begin_date, v_end_date)) &
        (col("a.trade_type") == "21")
    ).join(
        other=df_tmp_06010_security,
        on=(col("a.security_id") == col("d.security_id")),
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
        sum(
            when(
                col("a.busi_date") == v_end_trade_date,
                col("a.strikefrozenmargin")
            ).otherwise(0)
        ).alias("strikefrozenmargin"),
        sum(
            when(
                col("a.busi_date") == v_end_trade_date,
                col("a.exchstrikefrozenmargin")
            ).otherwise(0)
        ).alias("exchstrikefrozenmargin")
    ).select(
        col("busi_date_during"),
        col("a.client_id"),
        col("a.branch_id"),
        col("a.FUND_ACCOUNT_ID"),
        col("a.money_type"),
        col("a.trade_type"),
        col("a.trade_prop"),
        col("d.market_id"),
        col("d.product_id"),
        col("a.security_id"),
        col("d.delivery_date"),
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
        col("strikefrozenmargin"),
        col("exchstrikefrozenmargin")
    )

    df_tmp_brp_06010_execute = total_01.union(total_02).groupBy(
        col("busi_date_during"),
        col("client_id"),
        col("branch_id"),
        col("FUND_ACCOUNT_ID"),
        col("money_type"),
        col("trade_type"),
        col("trade_prop"),
        col("market_id"),
        col("product_id"),
        col("security_id"),
        col("delivery_date")
    ).agg(
        sum(col("transfee")),
        sum(col("market_transfee")),
        sum(col("remain_transfee")),
        sum(col("transfee_xq")),
        sum(col("market_transfee_xq")),
        sum(col("transfee_ly")),
        sum(col("market_transfee_ly")),
        sum(col("strike_qty_xq")),
        sum(col("strike_qty_ly")),
        sum(col("optstrike_profit")),
        sum(col("STRIKEFROZENMARGIN")),
        sum(col("EXCHSTRIKEFROZENMARGIN"))
    )

    return_to_hive(
        spark=spark,
        df_result=df_tmp_brp_06010_execute,
        target_table="ddw.TMP_06010_EXECUTE",
        insert_mode="overwrite"
    )

    df_tmp_brp_06010 = df_tmp_06010_hold.alias("w1").select(
        col("w1.busi_date_during"),
        col("w1.client_id"),
        col("w1.FUND_ACCOUNT_ID"),
        col("w1.branch_id"),
        col("w1.money_type"),
        col("w1.trade_type"),
        col("w1.trade_prop"),
        col("w1.product_id"),
        col("w1.security_id"),
        col("d.delivery_date")
    ).distinct().union(
        df_tmp_06010_hold_fee.alias("w11").select(
            col("w11.busi_date_during"),
            col("w11.client_id"),
            col("w11.FUND_ACCOUNT_ID"),
            col("w11.branch_id"),
            col("w11.money_type"),
            col("w11.trade_type"),
            col("w11.trade_prop"),
            col("w11.product_id"),
            col("w11.security_id"),
            col("d.delivery_date")
        ).distinct()
    ).union(
        df_tmp_06010_done.alias("w2").select(
            col("w2.busi_date_during"),
            col("w2.client_id"),
            col("w2.FUND_ACCOUNT_ID"),
            col("w2.branch_id"),
            col("w2.money_type"),
            col("w2.trade_type"),
            col("w2.trade_prop"),
            col("w2.product_id"),
            col("w2.security_id"),
            col("d.delivery_date")
        ).distinct()
    ).union(
        df_tmp_06010_deliv.alias("w3").select(
            col("w3.busi_date_during"),
            col("w3.client_id"),
            col("w3.FUND_ACCOUNT_ID"),
            col("w3.branch_id"),
            col("w3.money_type"),
            col("w3.trade_type"),
            col("w3.trade_prop"),
            col("w3.product_id"),
            col("w3.security_id"),
            col("d.delivery_date")
        ).distinct()
    ).union(
        df_tmp_brp_06010_execute.alias("w4").select(
            col("w4.busi_date_during"),
            col("w4.client_id"),
            col("w4.FUND_ACCOUNT_ID"),
            col("w4.branch_id"),
            col("w4.money_type"),
            col("w4.trade_type"),
            col("w4.trade_prop"),
            col("w4.product_id"),
            col("w4.security_id"),
            col("d.delivery_date")
        ).distinct()
    ).alias("x").join(
        other=df_tmp_06010_hold.alias("a"),
        on=(
            (col("x.client_id") == col("a.client_id")) &
            (col("x.FUND_ACCOUNT_ID") == col("a.FUND_ACCOUNT_ID")) &
            (col("x.busi_date_during") == col("a.busi_date_during")) &
            (col("x.security_id") == col("a.security_id")) &
            (col("x.money_type") == col("a.money_type")) &
            (col("x.trade_type") == col("a.trade_type")) &
            (col("x.trade_prop") == col("a.trade_prop")) &
            (col("x.branch_id") == col("a.branch_id"))
        ),
        how="left"
    ).join(
        other=df_tmp_06010_hold_fee.alias("a1"),
        on=(
            (col("x.client_id") == col("a1.client_id")) &
            (col("x.FUND_ACCOUNT_ID") == col("a1.FUND_ACCOUNT_ID")) &
            (col("x.busi_date_during") == col("a1.busi_date_during")) &
            (col("x.security_id") == col("a1.security_id")) &
            (col("x.money_type") == col("a1.money_type")) &
            (col("x.trade_type") == col("a1.trade_type")) &
            (col("x.trade_prop") == col("a1.trade_prop")) &
            (col("x.branch_id") == col("a1.branch_id"))
        ),
        how="left"
    ).join(
        other=df_tmp_06010_done.alias("b"),
        on=(
            (col("x.client_id") == col("b.client_id")) &
            (col("x.FUND_ACCOUNT_ID") == col("b.FUND_ACCOUNT_ID")) &
            (col("x.busi_date_during") == col("b.busi_date_during")) &
            (col("x.security_id") == col("b.security_id")) &
            (col("x.money_type") == col("b.money_type")) &
            (col("x.trade_type") == col("b.trade_type")) &
            (col("x.trade_prop") == col("b.trade_prop")) &
            (col("x.branch_id") == col("b.branch_id"))
        ),
        how="left"
    ).join(
        other=df_tmp_06010_deliv.alias("c"),
        on=(
            (col("x.client_id") == col("c.client_id")) &
            (col("x.FUND_ACCOUNT_ID") == col("c.FUND_ACCOUNT_ID")) &
            (col("x.busi_date_during") == col("c.busi_date_during")) &
            (col("x.security_id") == col("c.security_id")) &
            (col("x.money_type") == col("c.money_type")) &
            (col("x.trade_type") == col("c.trade_type")) &
            (col("x.trade_prop") == col("c.trade_prop")) &
            (col("x.branch_id") == col("c.branch_id"))
        ),
        how="left"
    ).join(
        other=df_tmp_brp_06010_execute.alias("d"),
        on=(
            (col("x.client_id") == col("d.client_id")) &
            (col("x.FUND_ACCOUNT_ID") == col("d.FUND_ACCOUNT_ID")) &
            (col("x.busi_date_during") == col("d.busi_date_during")) &
            (col("x.security_id") == col("d.security_id")) &
            (col("x.money_type") == col("d.money_type")) &
            (col("x.trade_type") == col("d.trade_type")) &
            (col("x.trade_prop") == col("d.trade_prop")) &
            (col("x.branch_id") == col("d.branch_id"))
        ),
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
        lit(None).alias("DELIV_DATE"),
        col("x.market_id"),
        col("a.hold_amount"),
        col("a.hold_money"),
        col("a.hold_amount_avg"),
        col("a.hold_profit_bydate"),
        col("a.hold_profit_bytrade"),
        when(
            col("x.trade_type") == "21",
            col("a.buy_hold_money_qq")
        ).otherwise(
            when(
                col("x.trade_type") == "12",
                col("a1.buy_hold_money_qq")
            ).otherwise(0)
        ).alias("BUY_HOLD_MONEY_QQ"),
        when(
            col("x.trade_type") == "21",
            col("a.sell_hold_money_qq")
        ).otherwise(
            when(
                col("x.trade_type") == "12",
                col("a1.sell_hold_money_qq")
            ).otherwise(0)
        ).alias("SELL_HOLD_MONEY_QQ"),
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
        (col("a1.market_transfee") + col("c.market_transfee") + col("d.market_transfee")).alias("TOTAL_MARKET_TRANSFEE"),
        (col("a1.transfee") - col("a1.market_transfee") + col("c.remain_transfee") + col("d.remain_transfee")).alias("TOTAL_REMAIN_TRANSFEE"),
        col("a1.total_profit").alias("TOTAL_PROFIT"),
        col("b.SOCRT_OPENFEE").alias("JINGSHOUFEI"),
        col("b.SETTLEMENTFEE").alias("JIESUANFEI"),
        col("b.opt_premium_income").alias("OPT_PREMIUM_INCOME"),
        col("b.opt_premium_pay").alias("OPT_PREMIUM_PAY"),
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
        col("a1.total_hold_amount").alias("TOTAL_HOLD_AMOUNT"),
        col("a1.transfee").alias("TRADE_TRANSFEE"),
        col("a1.market_transfee").alias("MARKET_TRADE_TRANSFEE"),
        (col("a1.transfee") - col("a1.market_transfee")).alias("TRADE_REMAIN_TRANSFEE"),
        col("c.margin").alias("DELIV_MARGIN"),
        col("c.market_margin").alias("DELIV_MARKET_MARGIN"),
        (col("d.transfee_xq") - col("d.market_transfee_xq")).alias("REMAIN_TRANSFEE_XQ"),
        col("b.done_amount_qp"),
        col("b.done_money_qp"),
        col("b.force_number")
    )

    return_to_hive(
        spark=spark,
        df_result=df_tmp_brp_06010,
        target_table="ddw.TMP_BRP_06010",
        insert_mode="overwrite"
    )

    cust_done = df_tmp_brp_06010.alias("a").groupBy(
        lit(i_begin_date[0:6]).alias("BUSI_DATE_DURING"),
        col("a.client_id"),
        col("a.branch_id"),
        col("a.money_type"),
        col("a.trade_type"),
        col("a.trade_prop"),
        col("a.product_id"),
        col("a.security_id"),
        col("a.delivery_date"),
        col("a.market_id"),
        col("a.FUND_ACCOUNT_ID")
    ).agg(
        sum(col("a.hold_amount")).alias("HOLD_AMOUNT"),
        sum(col("a.hold_money")).alias("HOLD_MONEY"),
        sum(col("a.hold_amount_avg")).alias("HOLD_AMOUNT_AVG"),
        sum(col("a.hold_profit_bydate")).alias("HOLD_PROFIT_BYDATE"),
        sum(col("a.hold_profit_bytrade")).alias("HOLD_PROFIT_BYTRADE"),
        sum(col("a.BUY_HOLD_MONEY_QQ")).alias("BUY_HOLD_MONEY_QQ"),
        sum(col("a.SELL_HOLD_MONEY_QQ")).alias("SELL_HOLD_MONEY_QQ"),
        sum(col("a.MARGIN")).alias("MARGIN"),
        sum(col("a.MARKET_MARGIN")).alias("MARKET_MARGIN"),
        sum(col("a.have_trade_num")).alias("HAVE_TRADE_NUM"),
        sum(col("a.transfee_pj")).alias("TRANSFEE_PJ"),
        sum(col("a.market_transfee_pj")).alias("MARKET_TRANSFEE_PJ"),
        sum(col("a.remain_transfee_pj")).alias("REMAIN_TRANSFEE_PJ"),
        sum(col("a.done_amt")).alias("DONE_AMOUNT"),
        sum(col("a.done_amount_pj")).alias("DONE_AMOUNT_PJ"),
        sum(col("a.done_money")).alias("DONE_MONEY"),
        sum(col("a.done_money_pj")).alias("DONE_MONEY_PJ"),
        sum(col("a.done_amount_avg")).alias("DONE_AMOUNT_AVG"),
        sum(col("a.done_money_avg")).alias("DONE_MONEY_AVG"),
        sum(col("a.DELIV_TRANSFEE")).alias("DELIV_TRANSFEE"),
        sum(col("a.DELIV_MARKET_TRANSFEE")).alias("DELIV_MARKET_TRANSFEE"),
        sum(col("a.DELIV_REMAIN_TRANSFEE")).alias("DELIV_REMAIN_TRANSFEE"),
        sum(col("a.delivery_amount")).alias("DELIVERY_AMOUNT"),
        sum(col("a.done_amt")).alias("DONE_AMT"),
        sum(col("a.transfee_xq")).alias("TRANSFEE_XQ"),
        sum(col("a.transfee_ly")).alias("TRANSFEE_LY"),
        sum(col("a.market_transfee_xq")).alias("MARKET_TRANSFEE_XQ"),
        sum(col("a.market_transfee_ly")).alias("MARKET_TRANSFEE_LY"),
        sum(col("a.strike_qty_xq")).alias("STRIKE_QTY_XQ"),
        sum(col("a.strike_qty_ly")).alias("STRIKE_QTY_LY"),
        sum(col("a.close_profit_bydate")).alias("CLOSE_PROFIT_BYDATE"),
        sum(col("a.close_profit_bytrade")).alias("CLOSE_PROFIT_BYTRADE"),
        sum(col("a.TOTAL_TRANSFEE")).alias("TOTAL_TRANSFEE"),
        sum(col("a.TOTAL_MARKET_TRANSFEE")).alias("TOTAL_MARKET_TRANSFEE"),
        sum(col("a.TOTAL_REMAIN_TRANSFEE")).alias("TOTAL_REMAIN_TRANSFEE"),
        sum(col("a.TOTAL_PROFIT")).alias("TOTAL_PROFIT"),
        sum(col("a.JINGSHOUFEI")).alias("JINGSHOUFEI"),
        sum(col("a.JIESUANFEI")).alias("JIESUANFEI"),
        sum(col("a.OPT_PREMIUM_INCOME")).alias("OPT_PREMIUM_INCOME"),
        sum(col("a.OPT_PREMIUM_PAY")).alias("OPT_PREMIUM_PAY"),
        sum(col("a.add_fee1")).alias("ADD_FEE1"),
        sum(col("a.add_fee2")).alias("ADD_FEE2"),
        sum(col("a.OPT_PREMIUM_INCOME_PAY")).alias("OPT_PREMIUM_INCOME_PAY"),
        sum(col("a.optstrike_profit")).alias("OPTSTRIKE_PROFIT"),
        sum(col("a.MARGIN_TJ")).alias("MARGIN_TJ"),
        sum(col("a.MARGIN_TL")).alias("MARGIN_TL"),
        sum(col("a.MARGIN_TB")).alias("MARGIN_TB"),
        sum(col("a.FLOAT_PROFIT")).alias("FLOAT_PROFIT"),
        sum(col("a.HOLD_AMOUNT_BUY")).alias("HOLD_AMOUNT_BUY"),
        sum(col("a.HOLD_AMOUNT_SELL")).alias("HOLD_AMOUNT_SELL"),
        sum(col("a.STRIKE_QTY_DQ")).alias("STRIKE_QTY_DQ"),
        sum(col("a.STRIKE_MONEY_DQ")).alias("STRIKE_MONEY_DQ"),
        sum(col("a.total_hold_amount")).alias("total_hold_amount"),
        sum(col("a.BZJJ") * v_dratio).alias("BZJJ"),
        sum(col("a.TRADE_TRANSFEE")).alias("TRADE_TRANSFEE"),
        sum(col("a.MARKET_TRADE_TRANSFEE")).alias("MARKET_TRADE_TRANSFEE"),
        sum(col("a.TRADE_REMAIN_TRANSFEE")).alias("TRADE_REMAIN_TRANSFEE"),
        sum(col("a.DELIV_MARGIN")).alias("DELIV_MARGIN"),
        sum(col("a.DELIV_MARKET_MARGIN")).alias("DELIV_MARKET_MARGIN"),
        sum(col("a.REMAIN_TRANSFEE_XQ")).alias("REMAIN_TRANSFEE_XQ"),
        sum(col("a.DONE_AMOUNT_QP")).alias("DONE_AMOUNT_QP"),
        sum(col("a.DONE_MONEY_QP")).alias("DONE_MONEY_QP"),
        sum(col("a.FORCE_NUMBER")).alias("FORCE_NUMBER"),
        sum(col("a.TOTAL_PROFIT") - col("a.TOTAL_TRANSFEE")).alias("CLEAR_PROFIT")
    )

    df_t_trade_sum_data = cust_done.alias("t").join(
        other=spark.table("edw.h17_security").alias("e"),
        on=(col("t.security_id") == col("e.security_id")),
        how="left"
    ).join(
        other=spark.table("edw.h13_product").alias("f"),
        on=(
            (col("t.product_id") == col("f.product_id")) &
            (col("t.trade_type") == col("f.trade_type")) &
            (col("t.market_id") == col("f.market_id"))
        ),
        how="left"
    ).select(
        col("t.BUSI_DATE_DURING"),
        col("t.CLIENT_ID"),
        col("t.BRANCH_ID"),
        col("t.MONEY_TYPE"),
        col("t.TRADE_TYPE"),
        col("t.TRADE_PROP"),
        col("t.PRODUCT_ID"),
        col("t.SECURITY_ID"),
        when(
            (length(col("e.delivery_year")) == 4),
            substring(col("e.delivery_year"), 3, 2) + col("e.delivery_month")
        ).when(
            (substring(col("e.delivery_date"), 3, 4) != substring(col("e.end_delivery_date"), 3, 4)),
            substring(col("e.end_delivery_date"), 3, 4)
        ).when(
            (col("e.delivery_date") != col("e.end_delivery_date")),
            substring(col("e.end_delivery_date"), 3, 4)
        ).otherwise(
            regexp_extract(col("e.security_id"), "[0-9]+", 1)
        ).alias("DELIV_DATE"),
        col("t.MARKET_ID"),
        col("t.FUND_ACCOUNT_ID"),
        col("t.HOLD_AMOUNT"),
        col("t.HOLD_MONEY"),
        col("t.HOLD_AMOUNT_AVG"),
        col("t.HOLD_PROFIT_BYDATE"),
        col("t.HOLD_PROFIT_BYTRADE"),
        col("t.BUY_HOLD_MONEY_QQ"),
        col("t.SELL_HOLD_MONEY_QQ"),
        col("t.MARGIN"),
        col("t.MARKET_MARGIN"),
        col("t.HAVE_TRADE_NUM"),
        col("t.TRANSFEE_PJ"),
        col("t.MARKET_TRANSFEE_PJ"),
        col("t.REMAIN_TRANSFEE_PJ"),
        col("t.DONE_AMOUNT"),
        col("t.DONE_AMOUNT_PJ"),
        col("t.DONE_MONEY"),
        col("t.DONE_MONEY_PJ"),
        col("t.DONE_AMOUNT_AVG"),
        col("t.DONE_MONEY_AVG"),
        col("t.DELIV_TRANSFEE"),
        col("t.DELIV_MARKET_TRANSFEE"),
        col("t.DELIV_REMAIN_TRANSFEE"),
        col("t.DELIVERY_AMOUNT"),
        col("t.DONE_AMT"),
        col("t.TRANSFEE_XQ"),
        col("t.TRANSFEE_LY"),
        col("t.MARKET_TRANSFEE_XQ"),
        col("t.MARKET_TRANSFEE_LY"),
        col("t.STRIKE_QTY_XQ"),
        col("t.STRIKE_QTY_LY"),
        col("t.CLOSE_PROFIT_BYDATE"),
        col("t.CLOSE_PROFIT_BYTRADE"),
        col("t.TOTAL_TRANSFEE"),
        col("t.TOTAL_MARKET_TRANSFEE"),
        col("t.TOTAL_REMAIN_TRANSFEE"),
        col("t.TOTAL_PROFIT"),
        col("t.JINGSHOUFEI"),
        col("t.JIESUANFEI"),
        col("t.OPT_PREMIUM_INCOME"),
        col("t.OPT_PREMIUM_PAY"),
        col("t.ADD_FEE1"),
        col("t.ADD_FEE2"),
        col("t.OPT_PREMIUM_INCOME_PAY"),
        col("t.OPTSTRIKE_PROFIT"),
        col("t.MARGIN_TJ"),
        col("t.MARGIN_TL"),
        col("t.MARGIN_TB"),
        col("t.FLOAT_PROFIT"),
        col("t.HOLD_AMOUNT_BUY"),
        col("t.HOLD_AMOUNT_SELL"),
        col("t.STRIKE_QTY_DQ"),
        col("t.STRIKE_MONEY_DQ"),
        col("t.total_hold_amount"),
        col("t.BZJJ"),
        col("t.TRADE_TRANSFEE"),
        col("t.MARKET_TRADE_TRANSFEE"),
        col("t.TRADE_REMAIN_TRANSFEE"),
        col("t.DELIV_MARGIN"),
        col("t.DELIV_MARKET_MARGIN"),
        col("t.REMAIN_TRANSFEE_XQ"),
        col("t.DONE_AMOUNT_QP"),
        col("t.DONE_MONEY_QP"),
        col("t.FORCE_NUMBER"),
        col("t.CLEAR_PROFIT")
    )

    return_to_hive(
        spark=spark,
        df_result=df_t_trade_sum_data,
        target_table="cf_stat.T_TRADE_SUM_DATA",
        insert_mode="overwrite",
        partition_column=["BUSI_DATE_DURING"],
        partition_value=i_begin_date[0:6]
    )
