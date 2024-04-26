# -*- coding: utf-8 -*-

from pyspark.sql.functions import col, lit, sum, when, substring, regexp_replace, round, udf, date_format, date_add, \
    to_date
from pyspark.sql.types import StringType, BooleanType

from utils.date_utils import *
from utils.task_env import *


@log
def p_brp_06008_fgg_data_hy(spark, list_pub_date, i_begin_date, i_end_date):
    """
    资金对账表日和落地数据
    """
    # 注册函数
    # is_trade_day_check_udf = udf(lambda x: is_trade_day_check(list_pub_date, x), BooleanType())
    get_trade_date_udf = udf(lambda x, n: get_trade_date(list_pub_date, x, n), StringType())

    # 日期处理
    if not is_trade_day_check(spark, i_begin_date):
        v_begin_date = get_trade_date(spark, i_begin_date, 1)
        v_begin_date_before = get_trade_date(spark, i_begin_date, -1)
    else:
        v_begin_date = i_begin_date
        v_begin_date_before = v_begin_date

    v_end_date = i_end_date

    if not is_trade_day_check(spark, i_end_date):
        v_end_date = get_trade_date(spark, v_end_date, 0)

    tmp_trade_date = spark.table("ddw.tmp_trade_date").alias("c") \
        .filter(
        (col("c.busi_date").between(i_begin_date, i_end_date)) &
        (col("c.market_no") == "1")
    ).select(
        col("c.busi_date").alias("n_busi_date"),
        get_trade_date_udf(col("c.busi_date"), lit(0)).alias("trade_date"),
        col("c.trade_flag").cast("int").alias("trade_flag")
    )

    return_to_hive(
        spark=spark,
        df_result=tmp_trade_date,
        target_table="ddw.tmp_trade_date",
        insert_mode="overwrite"
    )

    logging.info("ddw.tmp_trade_date写入完成")

    """
    取客户权限范围内的客户
    """

    # 添加保障基金，成交量，成交金额

    # 取投资者保障基金比例

    v_dratio = spark.table("ods.t_cspmg_csperson_fee").alias("a") \
        .filter(
        (col("a.fee_type") == "1003") &
        (col("a.use_status") == "0") &
        (col("a.data_level") == "1")
    ).agg(
        max(col("a.fee")).alias("fee")
    ).select(
        when(col("fee").isNull(), 5.5).otherwise(col("fee")).alias("fee")
    ).collect()[0][0] / 100000000

    # 成交量，成交金额,平今成交量，平今成交额
    df_hold_balance = spark.table("cf_sett.t_hold_balance").alias("a") \
        .filter(
        (col("a.busi_date").between(v_begin_date, v_end_date))
    ).groupBy(
        col("a.busi_date"),
        col("a.client_id"),
        col("a.fund_account_id"),
        col("a.money_type")
    ).agg(
        round(sum(col("a.done_amt")), 2).alias("done_amount"),
        round(sum(col("a.done_sum")), 2).alias("done_money"),
        round(sum(col("a.close_amount_today")), 2).alias("done_amount_pj"),
        round(sum(col("a.close_money_today")), 2).alias("done_money_pj")
    ).select(
        col("a.busi_date"),
        col("a.client_id"),
        col("a.fund_account_id"),
        col("a.money_type"),
        col("done_amount"),
        col("done_money"),
        col("done_amount_pj"),
        col("done_money_pj")
    )

    return_to_hive(
        spark=spark,
        df_result=df_hold_balance,
        target_table="ddw.tmp_brp_06008_done",
        insert_mode="overwrite"
    )

    # 净留存手续费取德索数据

    df_clear_remain_transfee = spark.table("ods.T_DS_ADM$CUST_01").alias("a") \
        .filter(
        (regexp_replace(col("a.tx_dt"), pattern="-", replacement="").between(v_begin_date, v_end_date))
    ).fillna(0).groupBy(
        regexp_replace(col("a.tx_dt"), pattern="-", replacement="").alias("busi_date"),
        col("a.investor_id").alias("fund_account_id")
    ).agg(
        round(
            sum(
                col("a.subsistence_fee_amt")
                - col("a.fd_amt")
                - col("a.return_amt")
                - col("a.soft_amt")),
            2
        ).alias("clear_remain_transfee")
    ).select(
        col("busi_date"),
        col("fund_account_id"),
        col("clear_remain_transfee")
    )

    return_to_hive(
        spark=spark,
        df_result=df_clear_remain_transfee,
        target_table="ddw.tmp_brp_06008_clear",
        insert_mode="overwrite"
    )

    v_trade_day_count = get_date_period_and_days(
        spark=spark,
        begin_date=i_begin_date,
        end_date=i_end_date,
        is_trade_day=True
    )[2]

    v_nature_day_count = get_date_period_and_days(
        spark=spark,
        begin_date=i_begin_date,
        end_date=i_end_date,
        is_trade_day=False
    )[2]

    t_1 = spark.table("edw.h15_client_sett").alias("a") \
        .filter(
        (col("a.busi_date").between(v_begin_date_before, v_end_date))
    )

    tmp = t_1.alias("a") \
        .join(
        other=tmp_trade_date.alias("b"),
        on=(
                col("a.busi_date") == col("b.trade_date")
        ),
        how="inner"
    ).join(
        other=df_hold_balance.alias("d"),
        on=(
                col("a.busi_date") == col("d.busi_date") &
                col("a.fund_account_id") == col("d.fund_account_id") &
                col("a.money_type") == col("d.money_type")
        ),
        how="left"
    ).join(
        other=df_clear_remain_transfee.alias("e"),
        on=(
                col("a.busi_date") == col("e.busi_date") &
                col("a.fund_account_id") == col("e.fund_account_id")
        ),
        how="left"
    ).fillna(0) \
        .select(
        col("b.n_busi_date"),
        col("b.trade_flag"),
        col("a.busi_date"),
        col("a.branch_id"),
        col("a.client_id"),
        col("a.fund_account_id"),
        col("a.sett_type"),
        col("a.money_type"),
        col("a.yes_rights") * col("b.trade_flag").alias("yes_rights"),
        col("a.fund_in") * col("b.trade_flag").alias("fund_in"),
        col("a.strikeactreceivsum") * col("b.trade_flag").alias("strikeactreceivsum"),
        col("a.fund_out") * col("b.trade_flag").alias("fund_out"),
        col("a.strikeactpaysum") * col("b.trade_flag").alias("strikeactpaysum"),
        col("a.fund_impawn_in") * col("b.trade_flag").alias("fund_impawn_in"),
        col("a.fund_impawn_out") * col("b.trade_flag").alias("fund_impawn_out"),
        col("a.lastfundmortgagein") * col("b.trade_flag").alias("lastfundmortgagein"),
        col("a.lastfundmortgageout") * col("b.trade_flag").alias("lastfundmortgageout"),
        col("a.impawn_money"),
        col("a.impawn_money") * col("b.trade_flag").alias("end_impawn_money"),
        col("a.yes_impawn_money") * col("b.trade_flag").alias("yes_impawn_money"),
        col("a.opt_premium_income") * col("b.trade_flag").alias("opt_premium_income"),
        col("a.opt_premium_pay") * col("b.trade_flag").alias("opt_premium_pay"),
        col("a.transfee") * col("b.trade_flag").alias("transfee"),
        col("a.delivery_transfee") * col("b.trade_flag").alias("delivery_transfee"),
        col("a.strikefee") * col("b.trade_flag").alias("strikefee"),
        col("a.performfee") * col("b.trade_flag").alias("performfee"),
        col("a.market_transfee") * col("b.trade_flag").alias("market_transfee"),
        col("a.market_delivery_transfee") * col("b.trade_flag").alias("market_delivery_transfee"),
        col("a.market_strikefee") * col("b.trade_flag").alias("market_strikefee"),
        col("a.market_performfee") * col("b.trade_flag").alias("market_performfee"),
        col("a.today_profit") * col("b.trade_flag").alias("today_profit"),
        col("a.hold_profit") * col("b.trade_flag").alias("hold_profit"),
        col("a.hold_profit_f") * col("b.trade_flag").alias("hold_profit_f"),
        col("a.close_profit") * col("b.trade_flag").alias("close_profit"),
        col("a.drop_profit_f") * col("b.trade_flag").alias("drop_profit_f"),
        col("a.rights"),
        col("a.rights") * col("b.trade_flag").alias("end_rights"),
        col("a.fund_impawn_margin") * col("b.trade_flag").alias("fund_impawn_margin"),
        col("a.open_pre_money"),
        col("a.open_pre_money") * col("b.trade_flag").alias("end_open_pre_money"),
        col("a.socrt_openfee") * col("b.trade_flag").alias("socrt_openfee"),
        col("a.settlementfee") * col("b.trade_flag").alias("settlementfee"),
        col("a.buy_opt_market_value") * col("b.trade_flag").alias("buy_opt_market_value"),
        col("a.sell_opt_market_value") * col("b.trade_flag").alias("sell_opt_market_value"),
        col("a.market_deposit") * col("b.trade_flag").alias("market_deposit"),
        col("a.optstrike_profit") * col("b.trade_flag").alias("optstrike_profit"),
        col("a.margin") * col("b.trade_flag").alias("margin"),
        col("a.market_margin") * col("b.trade_flag").alias("market_margin"),
        col("a.strike_credit") * col("b.trade_flag").alias("strike_credit"),
        col("a.credit") * col("b.trade_flag").alias("credit"),
        col("d.done_amount") * col("b.trade_flag").alias("done_amount"),
        col("d.done_money") * col("b.trade_flag").alias("done_money"),
        col("a.execute_transfee") * col("b.trade_flag").alias("execute_transfee"),
        col("a.market_execute_transfee") * col("b.trade_flag").alias("market_execute_transfee"),
        (col("a.rights") - col("a.market_margin")) * col("b.trade_flag").alias("market_open_pre_money"),
        col("d.done_amount_pj") * col("b.trade_flag").alias("done_amount_pj"),
        col("d.done_money_pj") * col("b.trade_flag").alias("done_money_pj"),
        col("a.add_fee1") * col("b.trade_flag").alias("add_fee1"),
        col("a.add_fee2") * col("b.trade_flag").alias("add_fee2"),
        col("e.clear_remain_transfee") * col("b.trade_flag").alias("clear_remain_transfee")
    )

    df_tmp_brp_06008_rpt1 = tmp.alias("a") \
        .groupBy(
        col("a.n_busi_date"),
        col("a.trade_flag"),
        col("a.busi_date"),
        col("a.branch_id"),
        col("a.client_id"),
        col("a.fund_account_id"),
        col("a.sett_type"),
        col("a.money_type")
    ).agg(
        sum(col("a.yes_rights")).alias("yes_rights"),
        sum(col("a.fund_in")).alias("fund_in"),
        sum(col("a.strikeactreceivsum")).alias("strikeactreceivsum"),
        sum(col("a.fund_out")).alias("fund_out"),
        sum(col("a.strikeactpaysum")).alias("strikeactpaysum"),
        sum(col("a.fund_impawn_in")).alias("fund_impawn_in"),
        sum(col("a.fund_impawn_out")).alias("fund_impawn_out"),
        sum(col("a.lastfundmortgagein")).alias("lastfundmortgagein"),
        sum(col("a.lastfundmortgageout")).alias("lastfundmortgageout"),
        sum(col("a.impawn_money")).alias("impawn_money"),
        sum(col("a.end_impawn_money")).alias("end_impawn_money"),
        sum(col("a.yes_impawn_money")).alias("yes_impawn_money"),
        sum(col("a.opt_premium_income")).alias("opt_premium_income"),
        sum(col("a.opt_premium_pay")).alias("opt_premium_pay"),
        sum(col("a.transfee")).alias("transfee"),
        sum(col("a.delivery_transfee")).alias("delivery_transfee"),
        sum(col("a.strikefee")).alias("strikefee"),
        sum(col("a.performfee")).alias("performfee"),
        sum(col("a.market_transfee")).alias("market_transfee"),
        sum(col("a.market_delivery_transfee")).alias("market_delivery_transfee"),
        sum(col("a.market_strikefee")).alias("market_strikefee"),
        sum(col("a.market_performfee")).alias("market_performfee"),
        sum(col("a.today_profit")).alias("today_profit"),
        sum(col("a.hold_profit")).alias("hold_profit"),
        sum(col("a.hold_profit_f")).alias("hold_profit_f"),
        sum(col("a.close_profit")).alias("close_profit"),
        sum(col("a.drop_profit_f")).alias("drop_profit_f"),
        sum(col("a.rights")).alias("rights"),
        sum(col("a.end_rights")).alias("end_rights"),
        sum(col("a.fund_impawn_margin")).alias("fund_impawn_margin"),
        sum(col("a.open_pre_money")).alias("open_pre_money"),
        sum(col("a.end_open_pre_money")).alias("end_open_pre_money"),
        sum(col("a.socrt_openfee")).alias("socrt_openfee"),
        sum(col("a.settlementfee")).alias("settlementfee"),
        sum(col("a.buy_opt_market_value")).alias("buy_opt_market_value"),
        sum(col("a.sell_opt_market_value")).alias("sell_opt_market_value"),
        sum(col("a.market_deposit")).alias("market_deposit"),
        sum(col("a.optstrike_profit")).alias("optstrike_profit"),
        sum(col("a.margin")).alias("margin"),
        sum(col("a.market_margin")).alias("market_margin"),
        sum(col("a.strike_credit")).alias("strike_credit"),
        sum(col("a.credit")).alias("credit"),
        sum(col("a.done_amount")).alias("done_amount"),
        sum(col("a.done_money")).alias("done_money"),
        sum(col("a.execute_transfee")).alias("execute_transfee"),
        sum(col("a.market_execute_transfee")).alias("market_execute_transfee"),
        sum(col("a.market_open_pre_money")).alias("market_open_pre_money"),
        sum(col("a.done_amount_pj")).alias("done_amount_pj"),
        sum(col("a.done_money_pj")).alias("done_money_pj"),
        sum(col("a.add_fee1")).alias("add_fee1"),
        sum(col("a.add_fee2")).alias("add_fee2"),
        sum(col("a.clear_remain_transfee")).alias("clear_remain_transfee")
    )

    return_to_hive(
        spark=spark,
        df_result=df_tmp_brp_06008_rpt1,
        target_table="ddw.tmp_brp_06008_rpt1",
        insert_mode="overwrite"
    )

    # 获取i_begin_date和i_end_date之间的所有的日期列表
    list_date = get_date_list(i_begin_date, i_end_date)

    for partition_date in list_date:
        spark.sql(
            "ALTER TABLE ddw.T_RPT_06008 DROP IF EXISTS PARTITION (n_busi_date=%s)" % partition_date
        )

    df_T_RPT_06008_days = df_tmp_brp_06008_rpt1.alias("a") \
        .groupBy(
        col("a.n_busi_date"),
        col("a.branch_id"),
        col("a.client_id"),
        col("a.fund_account_id"),
        col("a.money_type"),
        col("a.sett_type"),
        col("a.trade_flag")
    ).agg(
        when(
            sum(col("a.end_rights")) > 0, 0
        ).when(
            sum(col("a.end_rights")) == 0, 1
        ).otherwise(2).alias("rights_flag"),
        sum(col("a.rights")).alias("rights"),
        sum(col("a.yes_rights")).alias("yes_rights"),
        (sum(col("a.fund_in")) + sum(col("a.strikeactreceivsum"))).alias("fund_in"),
        (sum(col("a.fund_out")) + sum(col("a.strikeactpaysum"))).alias("fund_out"),
        sum(col("a.fund_impawn_in")).alias("fund_impawn_in"),
        sum(col("a.fund_impawn_out")).alias("fund_impawn_out"),
        sum(col("a.lastfundmortgagein")).alias("FUND_IMPAWN_in_YES"),
        sum(col("a.lastfundmortgageout")).alias("FUND_IMPAWN_OUT_YES"),
        sum(col("a.opt_premium_income")).alias("opt_premium_income"),
        sum(col("a.opt_premium_pay")).alias("opt_premium_pay"),
        sum(col("a.transfee") + col("a.delivery_transfee") + col("a.strikefee")).alias("transfee"),
        sum(col("a.market_transfee") + col("a.market_delivery_transfee") + col("a.market_strikefee")).alias(
            "market_transfee"),
        (sum(col("a.transfee") + col("a.delivery_transfee") + col("a.strikefee")) -
         sum(col("a.market_transfee") + col("a.market_delivery_transfee") + col("a.market_strikefee"))).alias(
            "remain_transfee"),
        sum(col("a.done_money") * lit(v_dratio)).alias("bzjj"),
        sum(col("a.today_profit")).alias("today_profit"),
        sum(col("a.hold_profit")).alias("hold_profit_d"),
        sum(col("a.hold_profit_f")).alias("hold_profit_f"),
        sum(col("a.close_profit")).alias("close_profit_d"),
        sum(col("a.drop_profit_f")).alias("close_profit_f"),
        sum(when(col("a.end_rights") == 0, 0).otherwise(col("a.margin") / col("a.end_rights"))).alias("risk_degree1"),
        sum(when(col("a.end_rights") == 0, 0).otherwise(col("a.market_margin") / col("a.end_rights"))).alias(
            "risk_degree2"),
        sum(col("a.fund_impawn_margin")).alias("fund_impawn_margin"),
        sum(col("a.delivery_transfee")).alias("delivery_transfee"),
        sum(col("a.market_delivery_transfee")).alias("market_delivery_transfee"),
        sum(col("a.jingshoufei")).alias("jingshoufei"),
        sum(col("a.settlementfee")).alias("jiesuanfei"),
        sum(col("a.buy_opt_market_value")).alias("buy_opt_market_value"),
        sum(col("a.sell_opt_market_value")).alias("sell_opt_market_value"),
        sum(col("a.optstrike_profit")).alias("optstrike_profit"),
        sum(col("a.margin")).alias("margin"),
        sum(col("a.market_margin")).alias("market_margin"),
        sum(col("a.open_pre_money")).alias("open_pre_money"),
        sum(col("a.market_open_pre_money")).alias("market_open_pre_money"),
        sum(col("a.impawn_money")).alias("impawn_money"),
        sum(col("a.yes_impawn_money")).alias("yes_impawn_money"),
        sum(col("a.open_pre_money") - col("a.impawn_money")).alias("pre_impawn"),
        sum(col("a.strike_credit")).alias("strike_credit"),
        sum(col("a.credit")).alias("credit"),
        sum(col("a.strikefee")).alias("strikefee"),
        sum(col("a.market_strikefee")).alias("market_strikefee"),
        sum(col("a.performfee")).alias("performfee"),
        sum(col("a.market_performfee")).alias("market_performfee"),
        sum(col("a.done_amount")).alias("done_amount"),
        sum(col("a.done_money")).alias("done_money"),
        sum(col("a.execute_transfee")).alias("EXECUTE_TRANSFEE"),
        sum(col("a.market_execute_transfee")).alias("MARKET_EXECUTE_TRANSFEE"),
        sum(col("a.transfee")).alias("TRADE_TRANSFEE"),
        sum(col("a.market_transfee")).alias("MARKET_TRADE_TRANSFEE"),
        sum(col("a.transfee") - col("a.market_transfee")).alias("REMAIN_TRADE_TRANSFEE"),
        sum(col("a.market_deposit")).alias("market_deposit"),
        sum(col("a.done_amount_pj")).alias("done_amount_pj"),
        sum(col("a.done_money_pj")).alias("done_money_pj"),
        sum(col("a.add_fee1")).alias("ADD_FEE1"),
        sum(col("a.add_fee2")).alias("ADD_FEE2"),
        sum(col("a.clear_remain_transfee")).alias("CLEAR_REMAIN_TRANSFEE")
    )

    df_TMP_06008_1 = df_T_RPT_06008_days.alias("b") \
        .select(
        date_format(date_add(to_date(col("b.n_busi_date"), "yyyyMMdd"), 1), "yyyyMMdd").alias("n_busi_date"),
        col("b.rights"),
        col("b.client_id"),
        col("b.money_type"),
        col("b.sett_type"),
        col("b.fund_account_id"),
        lit("0").alias("trade_flag")
    )

    return_to_hive(
        spark=spark,
        df_result=df_TMP_06008_1,
        target_table="cf_busimg.TMP_06008_1",
        insert_mode="overwrite"
    )

    # 更新自然日的yes_rights
    df_y = df_TMP_06008_1.alias("b") \
        .select(
        col("b.n_busi_date"),
        col("b.client_id"),
        col("b.money_type"),
        col("b.sett_type"),
        col("b.fund_account_id"),
        col("b.trade_flag"),
        col("b.rights")
    )

    df_T_RPT_06008_days = update_dataframe(
        df_to_update=spark.table("ddw.T_RPT_06008"),
        df_use_me=df_y,
        join_columns=["n_busi_date", "client_id", "money_type", "sett_type", "fund_account_id", "trade_flag"],
        update_columns=["rights"]
    )

    return_to_hive(
        spark=spark,
        df_result=df_T_RPT_06008_days,
        target_table="ddw.T_RPT_06008",
        insert_mode="append"
    )
