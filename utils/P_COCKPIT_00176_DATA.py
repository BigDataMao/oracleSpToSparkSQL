# -*- coding: utf-8 -*-
import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, greatest, lit, regexp_replace, round, when

from utils.date_utils import get_date_period_and_days
from utils.task_env import log, return_to_hive


@log
def p_cockpit_00176_data(spark: SparkSession, i_month_id):
    """
    IB驻点收入调整表-数据落地
    :param spark: SparkSession对象
    :param i_month_id: 月份ID,格式为"YYYYMM"
    :return:
    """

    days = get_date_period_and_days(spark, busi_month=i_month_id, is_trade_day=False)
    V_CONFIRM_BEGIN_DATE = days[0]
    V_CONFIRM_END_DATE = days[1]
    v_trade_days = days[2]

    # 计算  期初权益  期末权益  日均权益  留存手续费

    df_tmp_176_1 = spark.table("ods.t_ds_crm_broker_investor_rela").alias("t") \
        .withColumn("st_dt", regexp_replace(col("t.st_dt"), "-", "")).alias("t") \
        .join(
        spark.table("edw.h12_fund_account").alias("a"),
        col("t.investor_id") == col("a.fund_account_id")
    ).filter(
        (col("st_dt") <= V_CONFIRM_END_DATE) &
        (col("t.broker_id").like("ZD%")) &
        (col("t.rela_sts") == "A") &
        (col("t.approve_sts") == "0")
    ).dropDuplicates(["st_dt", "investor_id"]).select(
        greatest(col("st_dt"), lit(V_CONFIRM_BEGIN_DATE)).alias("busi_date"),
        col("investor_id").alias("fund_account_id")
    ).alias("a").join(
        spark.table("ddw.t_rpt_06008").alias("t"),
        col("t.fund_account_id") == col("a.fund_account_id")
    ).filter(
        (col("t.n_busi_date").between(col("a.busi_date"), lit(V_CONFIRM_END_DATE)))
    ).groupBy(
        "t.fund_account_id"
    ).agg(
        sum("t.yes_rights").alias("yes_rights"),
        sum("t.rights").alias("end_rights"),
        sum("t.rights").alias("avg_rights"),
        sum("t.remain_transfee").alias("remain_transfee"),
        sum("t.clear_remain_transfee").alias("clear_remain_transfee")
    )

    """
    --计算 利息净收入=利息收入—客户和居间返还  （从薪酬模板-客户利返计算表 取累计息资金*利率*天数/360-利息）
    --利率表 CF_BUSIMG.T_COCKPIT_INTEREST_RATE
    """

    tmp = spark.table("ods.t_ds_ret_investorret_int").alias("a") \
        .filter(
        (regexp_replace(col("a.date_str"), "-", "").between(V_CONFIRM_BEGIN_DATE, V_CONFIRM_END_DATE)) &
        (col("a.today_ri_amt") != - col("a.calint_amt"))
    ).join(
        spark.table("ods.t_ds_dc_investor").alias("c"),
        col("a.investor_id") == col("c.investor_id"),
        "inner"
    ).groupBy(
        col("a.investor_id").alias("fund_account_id"),
        col("a.calint_days"),
        regexp_replace(col("a.date_str"), "-", "").alias("busi_date")
    ).agg(
        round(sum(col("a.calint_amt")), 2).alias("SUM_CALINT_AMT"),
        sum(col("a.int_amt_d")).alias("int_amt_d")
    )

    tmp1 = tmp.alias("t") \
        .join(
        spark.table("ddw.t_cockpit_interest_rate").alias("a"),
        (col("t.busi_date").between(col("a.begin_date"), col("a.end_date"))),
        "inner"
    ).groupBy(
        col("t.fund_account_id"),
        col("t.calint_days"),
        col("a.interest_rate")
    ).agg(
        sum(col("t.SUM_CALINT_AMT")).alias("SUM_CALINT_AMT"),
        sum(col("t.int_amt_d")).alias("int_amt_d")
    )

    df_tmp_176_4 = tmp1.alias("t") \
        .join(
        spark.table("ods.T_DS_CRM_BROKER_INVESTOR_RELA").alias("c"),
        col("t.fund_account_id") == col("c.investor_id"),
        "inner"
    ).filter(
        (col("c.broker_id").like("ZD%")) &
        (col("c.rela_sts") == "A") &
        (col("c.approve_sts") == "0")
    ).select(
        col("t.fund_account_id"),
        (
                col("t.SUM_CALINT_AMT") * col("t.interest_rate") * v_trade_days / col("t.calint_days")
                - col("t.int_amt_d")
        ).alias("interest_clear_income")
    )

    """
    --计算 减免收入=减免返还收入-减免返还支出(减免返还收入来自内核表-投资者交易所返还计算-二次开发的交易所减收，减免返还支出来自客户出入金流水)
    --减免返还收入
    """

    # 定义一个函数将日期转换为上个月的日期
    def get_last_month_date(date_str):
        date = datetime.datetime.strptime(date_str, "%Y%m%d")
        first_day_of_current_month = date.replace(day=1)
        last_day_of_last_month = first_day_of_current_month - datetime.timedelta(days=1)
        last_month_date = last_day_of_last_month.replace(day=min(date.day, last_day_of_last_month.day))
        return last_month_date.strftime("%Y%m%d")

    # 转换日期
    V_CONFIRM_BEGIN_DATE_TMP = get_last_month_date(V_CONFIRM_BEGIN_DATE)
    V_CONFIRM_END_DATE_TMP = get_last_month_date(V_CONFIRM_END_DATE)

    # 集中把agg_exprs的内容提取出来
    fee_columns = [
        ("EXCHANGE_TXFEE_AMT", 2, lambda tx_dt: tx_dt >= V_CONFIRM_BEGIN_DATE),
        ("RET_FEE_AMT", 4, lambda tx_dt: tx_dt >= V_CONFIRM_BEGIN_DATE),
        ("RET_FEE_AMT_czce", 4, lambda tx_dt: tx_dt <= V_CONFIRM_END_DATE_TMP),
        ("RET_FEE_AMT_dce", 4, lambda tx_dt: tx_dt <= V_CONFIRM_END_DATE),
        ("RET_FEE_AMT_shfe", 4, lambda tx_dt: tx_dt >= V_CONFIRM_BEGIN_DATE),
        ("RET_FEE_AMT_shfe1", 4, lambda tx_dt: (tx_dt < lit("20220501")) & (tx_dt <= V_CONFIRM_END_DATE_TMP)),
        ("RET_FEE_AMT_cffex", 4, lambda tx_dt: tx_dt <= V_CONFIRM_END_DATE_TMP),
        ("RET_FEE_AMT_cffex2021", 4, lambda tx_dt: tx_dt <= V_CONFIRM_END_DATE_TMP),
        ("RET_FEE_AMT_dce31", 4, lambda tx_dt: tx_dt <= V_CONFIRM_END_DATE_TMP),
        ("RET_FEE_AMT_dce32", 4, lambda tx_dt: tx_dt <= V_CONFIRM_END_DATE_TMP),
        ("RET_FEE_AMT_dce33", 4, lambda tx_dt: tx_dt <= V_CONFIRM_END_DATE_TMP),
        ("RET_FEE_AMT_dce1", 4, lambda tx_dt: tx_dt <= V_CONFIRM_END_DATE_TMP),
        ("RET_FEE_AMT_dce2", 4, lambda tx_dt: tx_dt <= V_CONFIRM_END_DATE_TMP),
        ("investor_ret_amt", 4, lambda tx_dt: tx_dt >= V_CONFIRM_BEGIN_DATE)
    ]
    # 再变成agg表达式
    agg_exprs = [
        round(
            sum(
                when(
                    cond(regexp_replace(col("tx_dt"), "-", "")),
                    col(col_name)
                ).otherwise(lit(0))
            ), precision
        ).alias(col_name if col_name != "RET_FEE_AMT_tx" else "RET_FEE_AMT")
        for col_name, precision, cond in fee_columns
    ]

    df_tmp_176_2 = spark.table("ods.t_ds_ret_exchange_retfee2").alias("a") \
        .withColumn("tx_dt", regexp_replace(col("tx_dt"), "-", "")).alias("a") \
        .filter(
        (
                (col("a.tx_dt").between(V_CONFIRM_BEGIN_DATE, V_CONFIRM_END_DATE)) |
                (col("a.tx_dt").between(V_CONFIRM_BEGIN_DATE_TMP, V_CONFIRM_END_DATE_TMP))
        )
    ).join(
        spark.table("ods.t_ds_dc_org").alias("b"),
        col("a.orig_department_id") == col("b.department_id"),
        "inner"
    ).join(
        spark.table("ods.t_ds_dc_investor").alias("ff"),
        col("a.investor_id") == col("ff.investor_id"),
        "inner"
    ).join(
        spark.table("ods.T_DS_CRM_BROKER_INVESTOR_RELA").alias("c"),
        col("a.investor_id") == col("c.investor_id"),
        "inner"
    ).filter(
        (col("c.broker_id").like("ZD%")) &
        (col("c.rela_sts") == "A") &
        (col("c.approve_sts") == "0")
    ).groupBy(
        col("a.investor_id").alias("fund_account_id")
    ).agg(
        *agg_exprs
    ).groupBy(
        "fund_account_id"
    ).agg(
        sum(
            col("EXCHANGE_TXFEE_AMT") +
            col("RET_FEE_AMT") +
            col("RET_FEE_AMT_czce") +
            col("RET_FEE_AMT_dce") +
            col("RET_FEE_AMT_cffex") +
            col("RET_FEE_AMT_cffex2021") +
            col("RET_FEE_AMT_dce31") +
            col("RET_FEE_AMT_dce32") +
            col("RET_FEE_AMT_dce33") +
            col("RET_FEE_AMT_dce1") +
            col("RET_FEE_AMT_dce2") +
            col("RET_FEE_AMT_shfe") +
            col("RET_FEE_AMT_shfe1") +
            col("investor_ret_amt")
        ).alias("market_reduct")
    )

    #   --减免返还支出

    df_tmp_176_3 = spark.table("edw.h14_fund_jour").alias("t") \
        .join(
        spark.table("ods.T_DS_CRM_BROKER_INVESTOR_RELA").alias("c"),
        col("t.fund_account_id") == col("c.investor_id"),
        "inner"
    ).filter(
        (col("t.fund_type") == "3") &
        (col("t.fund_direct") == "1") &
        (col("c.broker_id").like("ZD%")) &
        (col("c.rela_sts") == "A") &
        (col("c.approve_sts") == "0") &
        (col("t.busi_date").between(V_CONFIRM_BEGIN_DATE, V_CONFIRM_END_DATE))
    ).groupBy(
        "t.fund_account_id"
    ).agg(
        sum("t.occur_money").alias("occur_money")
    )

    # 最终计算

    tmp = spark.table("ods.T_DS_CRM_BROKER_INVESTOR_RELA").alias("t") \
        .join(
        spark.table("ddw.T_COCKPIT_00110").alias("c"),
        on=(col("t.broker_id") == col("c.broker_id")),
        how="left"
    ).join(
        spark.table("edw.h12_fund_account").alias("e"),
        on=(col("t.investor_id") == col("e.fund_account_id")),
        how="left"
    ).join(
        spark.table("edw.h11_branch").alias("f"),
        on=(col("e.branch_id") == col("f.branch_id")),
    ).join(
        df_tmp_176_1.alias("a"),
        on=(col("t.investor_id") == col("a.fund_account_id")),
        how="inner"
    ).join(
        df_tmp_176_4.alias("b"),
        on=(col("t.investor_id") == col("b.fund_account_id")),
        how="left"
    ).join(
        df_tmp_176_2.alias("c"),
        on=(col("t.investor_id") == col("c.fund_account_id")),
        how="left"
    ).join(
        df_tmp_176_3.alias("d"),
        on=(col("t.investor_id") == col("d.fund_account_id")),
        how="left"
    ).filter(
        (col("t.broker_id").like("ZD%")) &
        (col("t.rela_sts") == "A") &
        (col("t.approve_sts") == "0")
    ).select(
        col("t.investor_id").alias("fund_account_id"),
        (col("b.interest_clear_income") * col("t.data_pct")).alias("idzd_interest_clear_income"),
        (col("c.market_reduct") - col("d.occur_money")).alias("ibzd_market_reduct_income"),
        col("a.clear_remain_transfee").alias("ibzd_clear_remain_transfee")
    ).alias("t").join(
        spark.table("edw.h12_fund_account").alias("b"),
        col("t.FUND_ACCOUNT_ID") == col("b.fund_account_id"),
        "left"
    ).groupBy(
        "b.branch_id"
    ).agg(
        sum(
            col("t.idzd_interest_clear_income") +
            col("t.ibzd_market_reduct_income") +
            col("t.ibzd_clear_remain_transfee")
        ).alias("zd_income")
    ).withColumn(
        "month_id", lit(i_month_id)
    )

    return_to_hive(
        spark=spark,
        df_result=tmp,
        target_table="ddw.T_COCKPIT_00176",
        insert_mode="overwrite",
    )
