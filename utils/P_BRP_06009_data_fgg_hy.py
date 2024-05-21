# -*- coding: utf-8 -*-

from pyspark.sql.functions import sum, udf
from pyspark.sql.types import StringType

from utils.date_utils import *
from utils.task_env import *

config = Config()


@log
def p_brp_06009_data_fgg_hy(spark, list_pub_date, i_begin_date, i_end_date):
    """
    资金对账表-月度汇总
    """
    logger = config.get_logger()
    logger.info("开始执行p_brp_06009_data_fgg_hy")

    # 注册函数
    get_trade_date_udf = udf(lambda x, n: get_trade_date(list_pub_date, x, n), StringType())

    if not is_trade_day_check(list_pub_date, i_begin_date):
        v_begin_date = get_trade_date(list_pub_date, i_begin_date, 1)
        v_begin_date_before = get_trade_date(list_pub_date, v_begin_date, -1)
    else:
        v_begin_date = i_begin_date
        v_begin_date_before = v_begin_date

    if not is_trade_day_check(list_pub_date, i_end_date):
        v_end_date = get_trade_date(list_pub_date, i_end_date, 0)
    else:
        v_end_date = i_end_date

    v_begin_trade_date = v_begin_date
    v_end_trade_date = v_end_date

    (
        v_begin_trade_date,
        v_end_trade_date,
        _
    ) = get_date_period_and_days(
        spark=spark,
        begin_date=i_begin_date,
        end_date=i_end_date,
        is_trade_day=True
    )

    v_trade_day_count = get_date_period_and_days(
        spark=spark,
        begin_date=i_begin_date,
        end_date=i_end_date,
        is_trade_day=False
    )[2]

    df_tmp_trade_date = spark.table("edw.t10_pub_date").alias("c") \
        .filter(
        (col("c.market_no") == "1") &
        (col("c.busi_date").between(i_begin_date, i_end_date))
    ).select(
        col("c.busi_date").alias("n_busi_date"),
        get_trade_date_udf(col("c.busi_date"), lit(0)).alias("trade_date"),
        col("c.trade_flag").cast("int").alias("trade_flag")
    )

    return_to_hive(
        spark=spark,
        df_result=df_tmp_trade_date,
        target_table="ddw.tmp_trade_date",
        insert_mode="overwrite"
    )

    logger.info("cf_busimg.tmp_trade_date写入完成")

    # 取投资者保障基金比例

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

    # 取成交量，成交金额,平今成交量，平今成交额

    df_tmp_brp_06008_done = spark.table("edw.h15_hold_balance").alias("a") \
        .filter(
        (col("a.busi_date").between(v_begin_trade_date, v_end_trade_date))
    ).groupBy(
        col("a.busi_date"),
        col("a.client_id"),
        col("a.fund_account_id"),
        col("a.money_type")
    ).agg(
        sum("a.done_amt").alias("done_amount"),
        sum("a.done_sum").alias("done_money"),
        sum("a.close_amount_today").alias("done_amount_pj"),
        sum("a.close_money_today").alias("done_money_pj")
    )

    return_to_hive(
        spark=spark,
        df_result=df_tmp_brp_06008_done,
        target_table="ddw.tmp_brp_06008_done",
        insert_mode="overwrite"
    )

    # 净流程手续费取德索数据

    df_tmp_brp_06008_clear = spark.table("ddw.t_rpt_06008").alias("a") \
        .filter(
        (col("a.n_busi_date").between(v_begin_date, v_end_date))
    ).groupBy(
        col("a.n_busi_date"),
        col("a.fund_account_id")
    ).agg(
        sum("a.CLEAR_REMAIN_TRANSFEE").alias("clear_remain_transfee")
    ).select(
        col("n_busi_date").alias("busi_date"),
        col("fund_account_id"),
        col("clear_remain_transfee")
    )

    return_to_hive(
        spark=spark,
        df_result=df_tmp_brp_06008_clear,
        target_table="ddw.tmp_brp_06008_clear",
        insert_mode="overwrite"
    )

    logger.info("ddw.tmp_brp_06008_clear写入完成")

    # 清除分区,单月分区可不清除,直接覆盖

    t_N_cust_rights = spark.table("edw.h15_client_sett").fillna(0).alias("a") \
        .filter(
        (col("a.busi_date").between(v_begin_date_before, v_end_date))
    ).join(
        other=df_tmp_trade_date.fillna(0).alias("b"),
        on=(col("a.busi_date") == col("b.trade_date")),
        how="inner"
    ).join(
        other=spark.table("edw.h12_fund_account").alias("c"),
        on=(col("a.fund_account_id") == col("c.fund_account_id")),
        how="inner"
    ).join(
        other=df_tmp_brp_06008_done.fillna(0).alias("d"),
        on=(
                (col("a.busi_date") == col("d.busi_date")) &
                (col("a.client_id") == col("d.client_id")) &
                (col("a.fund_account_id") == col("d.fund_account_id")) &
                (col("a.money_type") == col("d.money_type"))
        ),
        how="left"
    ).join(
        other=df_tmp_brp_06008_clear.fillna(0).alias("e"),
        on=(
                (col("a.busi_date") == col("e.busi_date")) &
                (col("a.fund_account_id") == col("e.fund_account_id"))
        ),
        how="left"
    ).groupBy(
        col("b.N_busi_date"),
        col("b.trade_flag"),
        col("a.busi_date"),
        col("c.branch_id"),
        col("a.client_id"),
        col("a.fund_account_id"),
        col("a.sett_type"),
        col("a.MONEY_TYPE")
    ).agg(
        sum(col("a.yes_rights") * col("b.trade_flag")).alias("yes_rights"),
        sum(col("a.fund_in") * col("b.trade_flag")).alias("fund_in"),
        sum(col("a.strikeactreceivsum") * col("b.trade_flag")).alias("strikeactreceivsum"),
        sum(col("a.fund_out") * col("b.trade_flag")).alias("fund_out"),
        sum(col("a.strikeactpaysum") * col("b.trade_flag")).alias("strikeactpaysum"),
        sum(col("a.fund_impawn_in") * col("b.trade_flag")).alias("fund_impawn_in"),
        sum(col("a.fund_impawn_out") * col("b.trade_flag")).alias("fund_impawn_out"),
        sum(col("a.lastfundmortgagein") * col("b.trade_flag")).alias("lastfundmortgagein"),
        sum(col("a.lastfundmortgageout") * col("b.trade_flag")).alias("lastfundmortgageout"),
        sum(col("a.impawn_money")).alias("impawn_money"),
        sum(col("a.yes_impawn_money") * col("b.trade_flag")).alias("yes_impawn_money"),
        sum(col("a.opt_premium_income") * col("b.trade_flag")).alias("opt_premium_income"),
        sum(col("a.opt_premium_pay") * col("b.trade_flag")).alias("opt_premium_pay"),
        sum(col("a.transfee") * col("b.trade_flag")).alias("transfee"),
        sum(col("a.delivery_transfee") * col("b.trade_flag")).alias("delivery_transfee"),
        sum(col("a.strikefee") * col("b.trade_flag")).alias("strikefee"),
        sum(col("a.performfee") * col("b.trade_flag")).alias("performfee"),
        sum(col("a.market_transfee") * col("b.trade_flag")).alias("market_transfee"),
        sum(col("a.market_delivery_transfee") * col("b.trade_flag")).alias("market_delivery_transfee"),
        sum(col("a.market_strikefee") * col("b.trade_flag")).alias("market_strikefee"),
        sum(col("a.market_performfee") * col("b.trade_flag")).alias("market_performfee"),
        sum(col("a.today_profit") * col("b.trade_flag")).alias("today_profit"),
        sum(col("a.hold_profit") * col("b.trade_flag")).alias("hold_profit"),
        sum(col("a.hold_profit_f") * col("b.trade_flag")).alias("hold_profit_f"),
        sum(col("a.close_profit") * col("b.trade_flag")).alias("close_profit"),
        sum(col("a.drop_profit_f") * col("b.trade_flag")).alias("drop_profit_f"),
        sum(col("a.rights")).alias("rights"),
        sum(col("a.rights") * col("b.trade_flag")).alias("end_rights"),
        sum(col("a.fund_impawn_margin") * col("b.trade_flag")).alias("fund_impawn_margin"),
        sum(col("a.open_pre_money")).alias("open_pre_money"),
        sum(col("a.socrt_openfee") * col("b.trade_flag")).alias("socrt_openfee"),
        sum(col("a.SETTLEMENTFEE") * col("b.trade_flag")).alias("SETTLEMENTFEE"),
        sum(col("a.buy_opt_market_value") * col("b.trade_flag")).alias("buy_opt_market_value"),
        sum(col("a.sell_opt_market_value") * col("b.trade_flag")).alias("sell_opt_market_value"),
        sum(col("a.market_deposit") * col("b.trade_flag")).alias("market_deposit"),
        sum(col("a.optstrike_profit") * col("b.trade_flag")).alias("optstrike_profit"),
        sum(col("a.margin") * col("b.trade_flag")).alias("margin"),
        sum(col("a.market_margin") * col("b.trade_flag")).alias("market_margin"),
        sum(col("a.strike_credit") * col("b.trade_flag")).alias("strike_credit"),
        sum(col("a.credit") * col("b.trade_flag")).alias("credit"),
        sum(col("a.EXECUTE_TRANSFEE") * col("b.trade_flag")).alias("EXECUTE_TRANSFEE"),
        sum(col("a.MARKET_EXECUTE_TRANSFEE") * col("b.trade_flag")).alias("MARKET_EXECUTE_TRANSFEE"),
        sum(col("a.transfee") * col("b.trade_flag")).alias("TRADE_TRANSFEE"),
        sum(col("a.market_transfee") * col("b.trade_flag")).alias("MARKET_TRADE_TRANSFEE"),
        sum((col("a.transfee") - col("a.market_transfee")) * col("b.trade_flag")).alias("REMAIN_TRADE_TRANSFEE"),
        sum(col("d.done_amount") * col("b.trade_flag")).alias("DONE_AMOUNT"),
        sum(col("d.done_money") * col("b.trade_flag")).alias("DONE_MONEY"),
        sum(col("d.done_amount_pj") * col("b.trade_flag")).alias("DONE_AMOUNT_PJ"),
        sum(col("d.done_money_pj") * col("b.trade_flag")).alias("DONE_MONEY_PJ"),
        sum(col("a.add_fee1") * col("b.trade_flag")).alias("ADD_FEE1"),
        sum(col("a.add_fee2") * col("b.trade_flag")).alias("ADD_FEE2"),
        sum(col("e.clear_remain_transfee") * col("b.trade_flag")).alias("clear_remain_transfee")
    ).select(
        col("b.N_busi_date"),
        col("b.trade_flag"),
        col("a.busi_date"),
        col("c.branch_id"),
        col("a.client_id"),
        col("a.fund_account_id"),
        col("a.sett_type"),
        col("a.MONEY_TYPE"),
        col("yes_rights"),
        col("fund_in"),
        col("strikeactreceivsum"),
        col("fund_out"),
        col("strikeactpaysum"),
        col("fund_impawn_in"),
        col("fund_impawn_out"),
        col("lastfundmortgagein"),
        col("lastfundmortgageout"),
        col("impawn_money"),
        col("yes_impawn_money"),
        col("opt_premium_income"),
        col("opt_premium_pay"),
        col("transfee"),
        col("delivery_transfee"),
        col("strikefee"),
        col("performfee"),
        col("market_transfee"),
        col("market_delivery_transfee"),
        col("market_strikefee"),
        col("market_performfee"),
        col("today_profit"),
        col("hold_profit"),
        col("hold_profit_f"),
        col("close_profit"),
        col("drop_profit_f"),
        col("rights"),
        col("end_rights"),
        col("fund_impawn_margin"),
        col("open_pre_money"),
        col("socrt_openfee"),
        col("SETTLEMENTFEE"),
        col("buy_opt_market_value"),
        col("sell_opt_market_value"),
        col("market_deposit"),
        col("optstrike_profit"),
        col("margin"),
        col("market_margin"),
        col("strike_credit"),
        col("credit"),
        col("EXECUTE_TRANSFEE"),
        col("MARKET_EXECUTE_TRANSFEE"),
        col("TRADE_TRANSFEE"),
        col("MARKET_TRADE_TRANSFEE"),
        col("REMAIN_TRADE_TRANSFEE"),
        col("DONE_AMOUNT"),
        col("DONE_MONEY"),
        col("DONE_AMOUNT_PJ"),
        col("DONE_MONEY_PJ"),
        col("ADD_FEE1"),
        col("ADD_FEE2"),
        col("clear_remain_transfee")
    )

    t_cust_baisc_data = t_N_cust_rights.alias("a") \
        .groupBy(
        lit(i_begin_date[:6]).alias("busi_date_during"),
        col("a.branch_id"),
        col("a.client_id"),
        col("a.fund_account_id"),
        col("a.MONEY_TYPE"),
        col("a.sett_type")
    ).agg(
        sum(
            when(col("a.busi_date") == v_begin_date, col("a.yes_rights")).otherwise(0)
        ).alias("yes_rights"),
        sum(
            col("a.fund_in") + col("a.strikeactreceivsum")
        ).alias("fund_in"),
        sum(
            col("a.fund_out") + col("a.strikeactpaysum")
        ).alias("fund_out"),
        sum(
            when(
                (col("a.busi_date") == v_end_date) & (col("a.trade_flag") == 1),
                col("a.fund_impawn_in")
            ).otherwise(0)
        ).alias("fund_impawn_in"),
        sum(
            when(
                (col("a.busi_date") == v_end_date) & (col("a.trade_flag") == 1),
                col("a.fund_impawn_out")
            ).otherwise(0)
        ).alias("fund_impawn_out"),
        sum(
            when(
                (col("a.busi_date") == v_begin_date) & (col("a.trade_flag") == 1),
                col("a.lastfundmortgagein")
            ).otherwise(0)
        ).alias("FUND_IMPAWN_in_YES"),
        sum(
            when(
                (col("a.busi_date") == v_begin_date) & (col("a.trade_flag") == 1),
                col("a.lastfundmortgageout")
            ).otherwise(0)
        ).alias("FUND_IMPAWN_OUT_YES"),
        (sum(
            when(
                col("a.busi_date") == v_end_date,
                col("a.impawn_money")
            ).otherwise(0)
        ) - sum(
            when(
                col("a.busi_date") == v_begin_date,
                col("a.yes_impawn_money")
            ).otherwise(0)
        )).alias("fund_impawn_chg"),
        sum(col("a.opt_premium_income")).alias("opt_premium_income"),
        sum(col("a.opt_premium_pay")).alias("opt_premium_pay"),
        sum(col("a.transfee") + col("a.delivery_transfee") + col("a.strikefee")).alias("transfee"),
        sum(col("a.market_transfee") + col("a.market_delivery_transfee") + col("a.market_strikefee")).alias(
            "market_transfee"),
        (sum(col("a.transfee") + col("a.delivery_transfee") + col("a.strikefee"))
         - sum(col("a.market_transfee") + col("a.market_delivery_transfee") + col("a.market_strikefee"))).alias(
            "remain_transfee"),
        sum(col("a.DONE_AMOUNT") * lit(v_dratio)).alias("bzjj"),
        sum(col("a.today_profit")).alias("today_profit"),
        sum(col("a.hold_profit")).alias("hold_profit_d"),
        sum(col("a.hold_profit_f")).alias("hold_profit_f"),
        sum(col("a.close_profit")).alias("close_profit"),
        sum(col("a.drop_profit_f")).alias("drop_profit_f"),
        sum(
            when(col("a.busi_date") == v_end_date, col("a.end_rights")).otherwise(0)
        ).alias("end_rights"),
        max(col("a.rights")).alias("max_rights"),
        sum(col("a.rights") * col("trade_flag")).alias("avg_trade_rights"),
        sum(col("a.rights")).alias("avg_nature_rights"),
        sum(col("a.fund_impawn_margin")).alias("fund_impawn_margin"),
        sum(
            when(
                (col("a.busi_date") == v_end_date) & (col("a.trade_flag") == 1),
                col("a.open_pre_money")
            ).otherwise(0)
        ).alias("end_open_pre_money"),
        sum(
            when(
                (col("a.busi_date") == v_end_date) & (col("a.trade_flag") == 1),
                col("a.impawn_money")
            ).otherwise(0)
        ).alias("end_impawn_money"),
        (sum(
            when(
                (col("a.busi_date") == v_end_date) & (col("a.trade_flag") == 1),
                col("a.open_pre_money")
            ).otherwise(0)
        ) - sum(
            when(
                (col("a.busi_date") == v_end_date) & (col("a.trade_flag") == 1),
                col("a.impawn_money")
            ).otherwise(0)
        )).alias("end_pre_impawn"),
        sum(col("a.delivery_transfee")).alias("delivery_transfee"),
        sum(col("a.market_delivery_transfee")).alias("market_delivery_transfee"),
        sum(col("a.socrt_openfee")).alias("jingshoufei"),
        sum(col("a.SETTLEMENTFEE")).alias("jiesuanfei"),
        sum(
            when(
                (col("a.busi_date") == v_end_date) & (col("a.trade_flag") == 1),
                col("a.buy_opt_market_value") - col("a.sell_opt_market_value")
            ).otherwise(0)
        ).alias("end_opt_market_value"),
        sum(
            when(
                (col("a.busi_date") == v_end_date) & (col("a.trade_flag") == 1),
                col("a.market_deposit")
            ).otherwise(0)
        ).alias("end_market_deposit"),
        sum(col("a.market_deposit")).alias("avg_trade_market_deposit"),
        sum(
            when(
                (col("a.busi_date") == v_end_date) & (col("a.trade_flag") == 1),
                col("a.buy_opt_market_value")
            ).otherwise(0)
        ).alias("buy_opt_market_value"),
        sum(
            when(
                (col("a.busi_date") == v_end_date) & (col("a.trade_flag") == 1),
                col("a.sell_opt_market_value")
            ).otherwise(0)
        ).alias("sell_opt_market_value"),
        sum(col("a.optstrike_profit")).alias("optstrike_profit"),
        sum(
            when(
                (col("a.busi_date") == v_end_date) & (col("a.trade_flag") == 1),
                col("a.margin")
            ).otherwise(0)
        ).alias("margin"),
        sum(
            when(
                (col("a.busi_date") == v_end_date) & (col("a.trade_flag") == 1),
                col("a.market_margin")
            ).otherwise(0)
        ).alias("market_margin"),
        sum(col("a.open_pre_money") * col("trade_flag")).alias("open_pre_money"),
        sum(col("a.impawn_money") * col("trade_flag")).alias("impawn_money"),
        (sum(col("a.open_pre_money") * col("trade_flag"))
         - sum(col("a.impawn_money") * col("trade_flag"))).alias("pre_impawn"),
        sum(
            when(
                (col("a.busi_date") == v_end_date) & (col("a.trade_flag") == 1),
                col("a.strike_credit")
            ).otherwise(0)
        ).alias("strike_credit"),
        sum(
            when(
                (col("a.busi_date") == v_end_date) & (col("a.trade_flag") == 1),
                col("a.credit")
            ).otherwise(0)
        ).alias("credit"),
        sum(col("a.open_pre_money")).alias("TOTAL_OPEN_PRE_MONEY"),
        sum(col("a.impawn_money")).alias("TOTAL_IMPAWN_MONEY"),
        (sum(col("a.open_pre_money")) - sum(col("a.impawn_money"))).alias("TOTAL_PRE_IMPAWN"),
        sum(col("a.strikefee")).alias("strikefee"),
        sum(col("a.market_strikefee")).alias("market_strikefee"),
        sum(col("a.performfee")).alias("performfee"),
        sum(col("a.market_performfee")).alias("market_performfee"),
        sum(col("a.EXECUTE_TRANSFEE")).alias("EXECUTE_TRANSFEE"),
        sum(col("a.MARKET_EXECUTE_TRANSFEE")).alias("MARKET_EXECUTE_TRANSFEE"),
        sum(col("a.TRADE_TRANSFEE")).alias("TRADE_TRANSFEE"),
        sum(col("a.MARKET_TRADE_TRANSFEE")).alias("MARKET_TRADE_TRANSFEE"),
        sum(col("a.REMAIN_TRADE_TRANSFEE")).alias("REMAIN_TRADE_TRANSFEE"),
        sum(col("a.DONE_AMOUNT")).alias("DONE_AMOUNT"),
        sum(col("a.DONE_MONEY")).alias("DONE_MONEY"),
        sum(col("a.DONE_AMOUNT_PJ")).alias("DONE_AMOUNT_PJ"),
        sum(col("a.DONE_MONEY_PJ")).alias("DONE_MONEY_PJ"),
        sum(col("a.ADD_FEE1")).alias("ADD_FEE1"),
        sum(col("a.ADD_FEE2")).alias("ADD_FEE2"),
        sum(
            when(col("a.busi_date") == v_begin_date, col("a.yes_impawn_money")).otherwise(0)
        ).alias("begin_yes_impawn_money"),
        sum(col("a.clear_remain_transfee")).alias("clear_remain_transfee")
    ).select(
        col("busi_date_during"),
        col("branch_id"),
        col("client_id"),
        col("fund_account_id"),
        col("MONEY_TYPE"),
        col('sett_type'),
        col("yes_rights"),
        col("fund_in"),
        col("fund_out"),
        col("fund_impawn_in"),
        col("fund_impawn_out"),
        col("FUND_IMPAWN_in_YES"),
        col("FUND_IMPAWN_OUT_YES"),
        col("fund_impawn_chg"),
        col("opt_premium_income"),
        col("opt_premium_pay"),
        col("transfee"),
        col("market_transfee"),
        col("remain_transfee"),
        col("bzjj"),
        col("today_profit"),
        col("hold_profit_d"),
        col("hold_profit_f"),
        col("close_profit"),
        col("drop_profit_f"),
        col("end_rights"),
        col("max_rights"),
        col("avg_trade_rights"),
        col("avg_nature_rights"),
        col("fund_impawn_margin"),
        col("end_open_pre_money"),
        col("end_impawn_money"),
        col("end_pre_impawn"),
        col("delivery_transfee"),
        col("market_delivery_transfee"),
        col("jingshoufei"),
        col("jiesuanfei"),
        col("end_opt_market_value"),
        col("end_market_deposit"),
        col("avg_trade_market_deposit"),
        col("buy_opt_market_value"),
        col("sell_opt_market_value"),
        col("optstrike_profit"),
        col("margin"),
        col("market_margin"),
        col("open_pre_money"),
        col("impawn_money"),
        col("pre_impawn"),
        col("strike_credit"),
        col("credit"),
        col("TOTAL_OPEN_PRE_MONEY"),
        col("TOTAL_IMPAWN_MONEY"),
        col("TOTAL_PRE_IMPAWN"),
        col("strikefee"),
        col("market_strikefee"),
        col("performfee"),
        col("market_performfee"),
        col("EXECUTE_TRANSFEE"),
        col("MARKET_EXECUTE_TRANSFEE"),
        col("TRADE_TRANSFEE"),
        col("MARKET_TRADE_TRANSFEE"),
        col("REMAIN_TRADE_TRANSFEE"),
        col("DONE_AMOUNT"),
        col("DONE_MONEY"),
        col("DONE_AMOUNT_PJ"),
        col("DONE_MONEY_PJ"),
        col("ADD_FEE1"),
        col("ADD_FEE2"),
        col("begin_yes_impawn_money"),
        col("clear_remain_transfee")
    )

    df_t_client_sett_data_m = t_cust_baisc_data.alias("a") \
        .select(
        col("a.busi_date_during"),
        col("a.client_id"),
        col("a.branch_id"),
        col("a.MONEY_TYPE"),
        col("a.YES_RIGHTS"),
        col("a.FUND_IN"),
        col("a.FUND_OUT"),
        col("a.FUND_IMPAWN_IN"),
        col("a.FUND_IMPAWN_OUT"),
        col("a.FUND_IMPAWN_CHG"),
        col("a.OPT_PREMIUM_INCOME"),
        col("a.OPT_PREMIUM_PAY"),
        col("a.TRANSFEE"),
        col("a.MARKET_TRANSFEE"),
        col("a.REMAIN_TRANSFEE"),
        col("a.BZJJ"),
        col("a.TODAY_PROFIT"),
        col("a.HOLD_PROFIT_D"),
        col("a.HOLD_PROFIT_F"),
        col("a.CLOSE_PROFIT"),
        col("a.DROP_PROFIT_F"),
        col("a.END_RIGHTS"),
        col("a.MAX_RIGHTS"),
        col("a.AVG_TRADE_RIGHTS"),
        col("a.AVG_NATURE_RIGHTS"),
        when(
            col("a.end_rights") != 0,
            col("a.margin") / col("a.end_rights")
        ).otherwise(0).alias("RISK_DEGREE1"),
        when(
            col("a.end_rights") != 0,
            col("a.market_margin") / col("a.end_rights")
        ).otherwise(0).alias("RISK_DEGREE2"),
        col("a.FUND_IMPAWN_MARGIN"),
        col("a.END_OPEN_PRE_MONEY"),
        col("a.END_IMPAWN_MONEY"),
        col("a.END_PRE_IMPAWN"),
        col("a.DELIVERY_TRANSFEE"),
        col("a.MARKET_DELIVERY_TRANSFEE"),
        col("a.JINGSHOUFEI"),
        col("a.JIESUANFEI"),
        col("a.END_OPT_MARKET_VALUE"),
        col("a.END_MARKET_DEPOSIT"),
        col("a.AVG_TRADE_MARKET_DEPOSIT"),
        col("a.BUY_OPT_MARKET_VALUE"),
        col("a.SELL_OPT_MARKET_VALUE"),
        col("a.OPTSTRIKE_PROFIT"),
        col("a.MARGIN"),
        col("a.MARKET_MARGIN"),
        col("a.OPEN_PRE_MONEY"),
        col("a.IMPAWN_MONEY"),
        col("a.PRE_IMPAWN"),
        col("a.strikefee").alias("STRIKEFEE_XQ"),
        col("a.MARKET_STRIKEFEE").alias("MARKET_STRIKEFEE_XQ"),
        col("a.performfee").alias("STRIKEFEE_LY"),
        col("a.MARKET_PERFORMFEE").alias("MARKET_STRIKEFEE_LY"),
        col("a.TOTAL_OPEN_PRE_MONEY"),
        col("a.TOTAL_IMPAWN_MONEY"),
        col("a.TOTAL_PRE_IMPAWN"),
        col("a.FUND_IMPAWN_IN_YES"),
        col("a.FUND_IMPAWN_OUT_YES"),
        col("a.STRIKE_CREDIT"),
        col("a.CREDIT"),
        col("a.FUND_ACCOUNT_ID"),
        col("a.SETT_TYPE"),
        col("a.EXECUTE_TRANSFEE"),
        col("a.MARKET_EXECUTE_TRANSFEE"),
        col("a.TRADE_TRANSFEE"),
        col("a.MARKET_TRADE_TRANSFEE"),
        col("a.REMAIN_TRADE_TRANSFEE"),
        col("a.CLEAR_REMAIN_TRANSFEE"),
        col("a.DONE_AMOUNT"),
        col("a.DONE_MONEY"),
        col("a.DONE_AMOUNT_PJ"),
        col("a.DONE_MONEY_PJ"),
        col("a.ADD_FEE1"),
        col("a.ADD_FEE2"),
        col("a.begin_yes_impawn_money")
    )

    return_to_hive(
        spark=spark,
        df_result=df_t_client_sett_data_m,
        target_table="ddw.t_client_sett_data",
        insert_mode="overwrite",
    )

    logger.info("ddw.t_client_sett_data写入成功！")
