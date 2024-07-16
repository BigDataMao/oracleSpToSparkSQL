# -*- coding: utf-8 -*-
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, udf, when, add_months, lit, to_date, round, regexp_replace
from pyspark.sql.types import StringType

from utils.date_utils import get_date_period_and_days, get_trade_date
from utils.task_env import log, return_to_hive


@log
def p_cockpit_00174_data(spark: SparkSession, list_pub_date, i_month_id):
    """
    经纪业务收入落地表
    :param list_pub_date: 交易日列表
    :param spark: SparkSession对象
    :param i_month_id: 月份ID,格式为"YYYYMM"
    :return:
    """

    # 注册get_trade_date_udf
    get_trade_date_udf = udf(lambda x, n: get_trade_date(list_pub_date, x, n), StringType())

    """
    经纪业务收入=留存手续费收入+利息收入+交易所减收
    留存手续费收入=手续费-上交手续费(资金对账表)
    利息收入=驾驶舱一期业务板块里的息差收入
    交易所减收=内核表-投资者交易所返还计算-二次开发(业务报表-薪酬报表)
    """

    # 今天的日期字符串,格式为"YYYYMMDD"
    today_str = datetime.now().strftime("%Y%m%d")

    data = get_date_period_and_days(
        spark=spark,
        busi_month=i_month_id,
        end_date=today_str,
        is_trade_day=True
    )

    v_begin_date = data[0]
    v_end_date = data[1]

    v_nature_begin_date = get_date_period_and_days(
        spark=spark,
        busi_month=i_month_id,
        end_date=today_str,
        is_trade_day=False
    )[0]

    v_nature_end_date = get_trade_date(list_pub_date=list_pub_date, busi_date=v_end_date, n=1)

    # 计算留存手续费
    # TODO cf_stat.T_RPT_06008要不要采集
    df_tmp_174_1 = spark.table("ddw.t_rpt_06008").alias("t") \
        .filter(
        (col("t.n_busi_date").between(v_begin_date, v_end_date))
    ).groupBy(
        "t.fund_account_id"
    ).agg(
        sum("t.remain_transfee").alias("remain_transfee")
    ).select(
        col("t.fund_account_id"),
        col("remain_transfee")
    )

    # 计算利息收入
    df_tmp_trade_date = spark.table("edw.t10_pub_date").alias("c") \
        .filter(
        (col("c.market_no") == "1") &
        (col("c.busi_date").between(v_nature_begin_date, v_nature_end_date))
    ).select(
        col("c.busi_date").alias("n_busi_date"),
        get_trade_date_udf(col("c.busi_date"), lit(0)).alias("trade_date"),
        col("c.trade_flag").cast("int").alias("trade_flag")
    )

    tmp = spark.table("edw.h15_client_sett").alias("t") \
        .filter(
        (col("t.busi_date").between(v_begin_date, v_end_date))
    ).groupBy(
        "t.busi_date",
        "t.fund_account_id"
    ).agg(
        (
            sum("t.rights") -
            sum(
                when(
                    col("t.impawn_money") > col("t.margin"),
                    col("t.impawn_money")
                ).otherwise(col("t.margin"))
                )
        ).alias("interest_base")
    ).select(
        col("t.busi_date"),
        col("t.fund_account_id"),
        col("interest_base")
    )

    tmp1 = tmp.alias("a") \
        .join(df_tmp_trade_date.alias("b"), col("a.busi_date") == col("b.trade_date"), "inner") \
        .groupBy(
        "a.busi_date",
        "a.fund_account_id"
    ).agg(
        sum("a.interest_base").alias("interest_base")
    ).select(
        col("a.busi_date"),
        col("a.fund_account_id"),
        col("interest_base")
    )

    df_tmp_174_2 = tmp1.alias("a") \
        .join(
        spark.table("ddw.t_cockpit_interest_rate").alias("b"),
        (col("a.busi_date").between(col("b.begin_date"), col("b.end_date"))),
        "inner"
    ).groupBy(
        "a.busi_date",
        "a.fund_account_id"
    ).agg(
        sum("a.interest_base").alias("interest_base"),
        sum(col("a.interest_base") * col("b.interest_rate") / 36000).alias("interest_income")
    ).select(
        col("a.busi_date"),
        col("a.fund_account_id"),
        col("interest_base"),
        col("interest_income")
    )

    # 计算交易所减收
    begin_date = to_date(lit(v_begin_date), "yyyyMMdd")
    end_date = to_date(lit(v_end_date), "yyyyMMdd")
    begin_date_minus_1_month = add_months(begin_date, -1)
    end_date_minus_1_month = add_months(end_date, -1)
    # 把end_date_minus_1_month转换为字符串
    begin_date_minus_1_month = regexp_replace(begin_date_minus_1_month.cast(StringType()), "-", "")
    end_date_minus_1_month = regexp_replace(end_date_minus_1_month.cast(StringType()), "-", "")
    # 定义聚合字段和条件
    fee_columns = [
        ("EXCHANGE_TXFEE_AMT", 2, lambda tx_dt: tx_dt >= v_begin_date),
        ("RET_FEE_AMT_tx", 4, lambda tx_dt: tx_dt >= v_begin_date),
        ("RET_FEE_AMT_czce", 4, lambda tx_dt: tx_dt <= end_date_minus_1_month),
        ("RET_FEE_AMT_dce", 4, lambda tx_dt: tx_dt <= end_date_minus_1_month),
        ("RET_FEE_AMT_shfe", 4, lambda tx_dt: tx_dt >= v_begin_date),
        ("RET_FEE_AMT_shfe1", 4, lambda tx_dt: (tx_dt < lit('20220501')) & (tx_dt <= end_date_minus_1_month)),
        ("RET_FEE_AMT_cffex", 4, lambda tx_dt: tx_dt <= end_date_minus_1_month),
        ("RET_FEE_AMT_cffex2021", 4, lambda tx_dt: tx_dt <= end_date_minus_1_month),
        ("RET_FEE_AMT_dce31", 4, lambda tx_dt: tx_dt <= end_date_minus_1_month),
        ("RET_FEE_AMT_dce32", 4, lambda tx_dt: tx_dt <= end_date_minus_1_month),
        ("RET_FEE_AMT_dce33", 4, lambda tx_dt: tx_dt <= end_date_minus_1_month),
        ("RET_FEE_AMT_dce1", 4, lambda tx_dt: tx_dt <= end_date_minus_1_month),
        ("RET_FEE_AMT_dce2", 4, lambda tx_dt: tx_dt <= end_date_minus_1_month),
        ("investor_ret_amt", 4, lambda tx_dt: tx_dt >= v_begin_date),
    ]

    # 动态生成聚合表达式
    agg_exprs = [
        round(
            sum(
                when(
                    cond(regexp_replace(col("tx_dt"), "-", "")),
                    col(col_name)
                ).otherwise(0)
            ), precision
        ).alias(col_name if col_name != "RET_FEE_AMT_tx" else "RET_FEE_AMT")
        for col_name, precision, cond in fee_columns
    ]

    def pure_date(date_str):
        return date_str.replace("-", "")

    pure_date = udf(pure_date, StringType())

    # 进行聚合计算
    df_tmp_174_3 = spark.table("ods.t_ds_ret_exchange_retfee2").alias("a") \
        .filter(
        (
            (pure_date(col("a.tx_dt")).between(begin_date_minus_1_month, end_date_minus_1_month)) |
            (pure_date(col("a.tx_dt")).between(v_begin_date, v_end_date))
        )
    ).join(
        spark.table("ods.t_ds_dc_org").alias("b"),
        col("a.orig_department_id") == col("b.department_id"),
        "inner"
    ).join(
        spark.table("ods.t_ds_dc_investor").alias("ff"),
        col("a.investor_id") == col("ff.investor_id"),
        "inner"
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
    ).select(
        col("fund_account_id"),
        col("market_reduct")
    )

    tmp = df_tmp_174_2.alias("x") \
        .groupBy("x.fund_account_id") \
        .agg(
        sum("x.interest_income").alias("interest_income")
    ).select(
        col("x.fund_account_id"),
        col("interest_income")
    )

    tmp1 = spark.table("edw.h12_fund_account").alias("t") \
        .join(df_tmp_174_1.alias("a"), col("t.fund_account_id") == col("a.fund_account_id"), "left") \
        .join(tmp.alias("b"), col("t.fund_account_id") == col("b.fund_account_id"), "left") \
        .join(df_tmp_174_3.alias("c"), col("t.fund_account_id") == col("c.fund_account_id"), "left") \
        .select(
        lit(i_month_id).alias("busi_month"),
        col("t.fund_account_id"),
        col("t.branch_id"),
        col("a.remain_transfee").alias("remain_transfee"),
        col("b.interest_income").alias("interest_income"),
        col("c.market_reduct").alias("market_reduct"),
        (
            col("a.remain_transfee") +
            col("b.interest_income") +
            col("c.market_reduct")
        ).alias("feature_income_total")
    ).filter(
        col("feature_income_total") != 0
    ).fillna(0)

    return_to_hive(
        spark=spark,
        df_result=tmp1,
        target_table="ddw.t_cockpit_00174",
        insert_mode="overwrite"
    )
