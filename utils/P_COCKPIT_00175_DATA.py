# -*- coding: utf-8 -*-
import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, greatest, lit, regexp_replace, round, when

from utils.date_utils import get_date_period_and_days
from utils.task_env import log, return_to_hive


@log
def p_cockpit_00175_data(spark: SparkSession, list_pub_date, i_month_id):
    """
    IB协同收入调整表-按月落地
    :param list_pub_date: 交易日列表
    :param spark: SparkSession对象
    :param i_month_id: 月份ID,格式为"YYYYMM"
    :return:
    """

    # 注册get_trade_date_udf
    # get_trade_date_udf = udf(lambda x, n: get_trade_date(list_pub_date, x, n), StringType())

    days = get_date_period_and_days(spark, busi_month=i_month_id, is_trade_day=False)
    V_CONFIRM_BEGIN_DATE = days[0]
    V_CONFIRM_END_DATE = days[1]
    v_trade_days = days[2]

    # 计算  期初权益  期末权益  日均权益  留存手续费
    tmp = spark.table("ddw.t_cockpit_00107").alias("t") \
        .filter(
        (col("t.regist_date") <= V_CONFIRM_END_DATE) &
        (col("t.service_type") == "1")
    ).dropDuplicates(
        ["regist_date", "fund_account_id"]
    ).select(
        greatest(col("regist_date"), lit(V_CONFIRM_BEGIN_DATE)).alias("busi_date"),
        col("fund_account_id")
    )

    df_tmp_125_1 = spark.table("edw.h15_client_sett").alias("t") \
        .join(
        other=tmp.alias("a"),
        on=(
                (col("t.fund_account_id") == col("a.fund_account_id")) &
                (col("t.busi_date").between(col("a.busi_date"), lit(V_CONFIRM_END_DATE)))
        ),
        how="inner"
    ).groupBy(
        "t.busi_date",
        "t.fund_account_id"
    ).agg(
        sum("t.yes_rights").alias("yes_rights"),
        sum("t.rights").alias("end_rights"),
        sum("t.rights").alias("avg_rights"),
        sum(
            col("t.transfee") + col("t.delivery_transfee") + col("t.strikefee") -
            col("t.market_transfee") - col("t.market_delivery_transfee") - col("t.market_strikefee")
        ).alias("remain_transfee")
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

    df_tmp_125_4 = tmp1.alias("t") \
        .join(
        spark.table("ddw.t_cockpit_00107").alias("c"),
        col("t.fund_account_id") == col("c.fund_account_id"),
        "inner"
    ).filter(
        col("c.service_type") == "1"
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

    df_tmp_125_2 = spark.table("ods.t_ds_ret_exchange_retfee2").alias("a") \
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
        spark.table("ddw.t_cockpit_00107").alias("c"),
        col("a.investor_id") == col("c.fund_account_id"),
        "inner"
    ).filter(
        col("c.service_type") == "1"
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

    df_tmp_125_3 = spark.table("edw.h14_fund_jour").alias("t") \
        .join(
        spark.table("ddw.t_cockpit_00107").alias("c"),
        col("t.fund_account_id") == col("c.fund_account_id"),
        "inner"
    ).filter(
        (col("t.fund_type") == "3") &
        (col("t.fund_direct") == "1") &
        (col("c.service_type") == "1") &
        (col("t.busi_date").between(V_CONFIRM_BEGIN_DATE, V_CONFIRM_END_DATE))
    ).groupBy(
        "t.fund_account_id"
    ).agg(
        sum("t.occur_money").alias("occur_money")
    )


    # 最终计算

    """
      INSERT INTO CF_BUSIMG.T_COCKPIT_00175
    (MONTH_ID, BRANCH_ID, XT_INCOME)
    with tmp as
     (
      select I_MONTH_ID,
              V_CONFIRM_BEGIN_DATE || '-' || V_CONFIRM_END_DATE, --登记日期   
              t.IB_BRANCH_ID, --证券营业部编号
              t.FUND_ACCOUNT_ID, --资金账号
              t.futu_service_name, --期货服务人员
              t.branch_id, --期货营业部代码
              sum(a.yes_rights), --期初权益
              sum(a.end_rights), --期末权益,
              sum(a.avg_rights), --日均权益,
              sum(a.remain_transfee), --留存手续费
              sum(a.remain_transfee * t.coope_income_reate) as coope_income, --IB协同收入
              sum(nvl(b.interest_clear_income, 0)) as interest_clear_income, --利息收入
              sum(nvl(c.market_reduct, 0) - nvl(d.occur_money, 0)) as market_reduct_income, --减免收入
              t.COOPE_INCOME_REATE, --比例
              sum(nvl(b.interest_clear_income, 0) * t.COOPE_INCOME_REATE) as xt_interest_clear_income, --协同利息收入=利息收入*比例
              sum((nvl(c.market_reduct, 0) - nvl(d.occur_money, 0)) *
                  t.COOPE_INCOME_REATE) as xt_market_reduct_income --协同减免收入=减免收入*比例
        from CF_BUSIMG.T_COCKPIT_00107 t
       inner join CF_BUSIMG.TMP_COCKPIT_00125_1 a
          on t.fund_account_id = a.fund_account_id
        left join CF_BUSIMG.TMP_COCKPIT_00125_4 b
          on t.fund_account_id = b.fund_account_id
        left join CF_BUSIMG.TMP_COCKPIT_00125_2 c
          on t.fund_account_id = c.fund_account_id
        left join CF_BUSIMG.TMP_COCKPIT_00125_3 d
          on t.fund_account_id = d.fund_account_id
       where t.service_type = '1' --服务类型(1:IB协同服务，2：IB驻点服务)
       group by t.IB_BRANCH_ID,
                 t.FUND_ACCOUNT_ID,
                 t.futu_service_name,
                 t.branch_id,
                 t.COOPE_INCOME_REATE),
    tmp1 as
     (
      --因为 期货服务人员会重复，所以要先去重
      select t.FUND_ACCOUNT_ID,
              t.coope_income, --IB协同收入
              t.xt_interest_clear_income, --协同利息收入
              t.xt_market_reduct_income --协同减免收入
        from tmp t
       group by t.FUND_ACCOUNT_ID,
                 t.coope_income, --IB协同收入
                 t.xt_interest_clear_income, --协同利息收入
                 t.xt_market_reduct_income)
    select I_MONTH_ID,
           b.branch_id,
           sum(t.coope_income + t.xt_interest_clear_income +
               t.xt_market_reduct_income) as xt_income
      from tmp1 t
      left join cf_sett.t_fund_account b
        on t.fund_account_id = b.fund_account_id
     group by b.branch_id
    ;
  commit;
    """

    tmp = spark.table("ddw.t_cockpit_00107").alias("t") \
        .join(
        df_tmp_125_1.alias("a"),
        on=(col("t.fund_account_id") == col("a.fund_account_id")),
        how="inner"
    ).join(
        df_tmp_125_4.alias("b"),
        on=(col("t.fund_account_id") == col("b.fund_account_id")),
        how="left"
    ).join(
        df_tmp_125_2.alias("c"),
        on=(col("t.fund_account_id") == col("c.fund_account_id")),
        how="left"
    ).join(
        df_tmp_125_3.alias("d"),
        on=(col("t.fund_account_id") == col("d.fund_account_id")),
        how="left"
    ).filter(
        col("t.service_type") == "1"
    ).groupBy(
        "t.IB_BRANCH_ID",
        "t.FUND_ACCOUNT_ID",
        "t.futu_service_name",
        "t.branch_id",
        "t.COOPE_INCOME_REATE"
    ).agg(
        sum("a.yes_rights").alias("yes_rights"),
        sum("a.end_rights").alias("end_rights"),
        sum("a.avg_rights").alias("avg_rights"),
        sum("a.remain_transfee").alias("remain_transfee"),
        sum(col("a.remain_transfee") * col("t.COOPE_INCOME_REATE")).alias("coope_income"),
        sum(col("b.interest_clear_income")).alias("interest_clear_income"),
        sum(col("c.market_reduct") - col("d.occur_money")).alias("market_reduct_income"),
        sum(col("b.interest_clear_income") * col("t.COOPE_INCOME_REATE")).alias("xt_interest_clear_income"),
        sum((col("c.market_reduct") - col("d.occur_money")) * col("t.COOPE_INCOME_REATE")).alias("xt_market_reduct_income")
    ).select(
        "FUND_ACCOUNT_ID",
        "coope_income",
        "xt_interest_clear_income",
        "xt_market_reduct_income"
    ).dropDuplicates().alias("t").join(
        spark.table("edw.h12_fund_account").alias("b"),
        col("t.FUND_ACCOUNT_ID") == col("b.fund_account_id"),
        "left"
    ).groupBy(
        "b.branch_id"
    ).agg(
        sum(
            col("t.coope_income") +
            col("t.xt_interest_clear_income") +
            col("t.xt_market_reduct_income")
        ).alias("xt_income")
    ).withColumn(
        "month_id", lit(i_month_id)
    )

    return_to_hive(
        spark=spark,
        df_result=tmp,
        target_table="ddw.T_COCKPIT_00175",
        insert_mode="overwrite",
    )

