# -*- coding: utf-8 -*-

from datetime import datetime

from pyspark.sql.functions import col, lit, count, sum, when, regexp_replace, coalesce

from utils.date_utils import get_date_period_and_days
from utils.task_env import return_to_hive, update_dataframe, log


@log
def p_cockpit_00138_data(spark, busi_date):
    """
    经营目标考核情况-数据落地
    :param spark: SparkSession对象
    :param busi_date: 业务日期,格式：yyyymmdd
    :return: None
    """
    # 获取年份
    # global v_end_month, v_begin_month
    v_last_quarter_id_1, v_last_quarter_id_2, v_last_quarter_id_3 = None, None, None
    v_year_id = busi_date[:4]

    # 获取日期所在季度
    date_string = busi_date
    date = datetime.strptime(date_string, '%Y%m%d')
    v_quarter_id = (date.month - 1) // 3 + 1

    # 根据季度获取月份变量
    if v_quarter_id == 1:
        v_begin_month = v_year_id + "01"
        v_end_month = v_year_id + "03"

    elif v_quarter_id == 2:
        v_begin_month = v_year_id + "04"
        v_end_month = v_year_id + "06"
        v_last_quarter_id_1 = "1"
    elif v_quarter_id == 3:
        v_begin_month = v_year_id + "07"
        v_end_month = v_year_id + "09"
        v_last_quarter_id_1 = "1"
        v_last_quarter_id_2 = "2"
    else:
        v_begin_month = v_year_id + "10"
        v_end_month = v_year_id + "12"
        v_last_quarter_id_1 = "1"
        v_last_quarter_id_2 = "2"
        v_last_quarter_id_3 = "3"

    # 获取系统日期
    current_date = datetime.now()
    v_sys_date = current_date.strftime('%Y%m%d')
    # 获取 v_trade_days
    (
        _,
        _,
        v_trade_days
    ) = get_date_period_and_days(
        spark=spark,
        begin_month=v_begin_month,
        end_month=v_end_month,
        end_date=v_sys_date,
        is_trade_day=True
    )

    # 获取v_begin_date, v_end_date
    (
        v_begin_date,
        v_end_date
    ) = get_date_period_and_days(
        spark=spark,
        begin_month=v_begin_month,
        end_month=v_end_month,
        is_trade_day=False
    )

    # 初始化数据
    df_136 = spark.table("ddw.T_COCKPIT_00136")

    df_t_cockpit_00138 = df_136.alais("t") \
        .filter(
        (col("t.year_id") == v_year_id)
    ).select(
        col("t.year_id"),
        lit(v_quarter_id).alias("quarter_id"),
        col("t.oa_branch_id"),
        col("t.oa_branch_name"),
        col("t.index_id"),
        col("t.index_asses_benchmark"),
        col("t.index_type"),
        col("t.index_name"),
        (
            when(
                condition=lit(v_quarter_id) == lit("1"),
                value=col("t.quarter_target_1")
            ).when(
                condition=lit(v_quarter_id) == lit("2"),
                value=col("t.quarter_target_2")
            ).when(
                condition=lit(v_quarter_id) == lit("3"),
                value=col("t.quarter_target_3")
            ).when(
                condition=lit(v_quarter_id) == lit("4"),
                value=col("t.quarter_target_4")
            ).otherwise(lit(0))
        ).alias("year_target_value"),
        col("t.weight_rate"),
        col("t.upper_limit_score")
    )

    # 营业收入完成情况（万）  取数源：财务内核表——营业收入，调整后的收入
    # cf_busimg.t_cockpit_00127
    df_127 = spark.table("ddw.t_cockpit_00127")
    df_branch_oa = spark.table("ddw.t_yy_branch_oa_rela")

    df_y = df_127.alias("t") \
        .filter(
        col("t.month_id").between(lit(v_begin_month), lit(v_end_month))
    ).join(
        other=df_branch_oa.alias("b"),
        on=(
                col("t.book_id") == col("a.yy_book_id")
        ),
        how="inner"
    ).groupBy(
        col("b.oa_branch_id")
    ).agg(
        sum(col("t.f5")).alias("actual_complet_value")  # 营业收入完成情况（万）
    ).select(
        "b.oa_branch_id"
    )

    df_t_cockpit_00138 = update_dataframe(
        df_to_update=df_t_cockpit_00138,
        filter_condition=(
                col("index_asses_benchmark") == lit("1") &  # 营业收入
                col("index_type") == lit("0")  # 绝对指标
        ),
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=["actual_complet_value"]
    )

    """
    更新
    经纪业务手续费收入市占率（万分之）
    经纪业务手续费收入市占率
    取数源：财务内核表：手续费及佣金净收入 + 交易所减免返还 / 行业手续费收入
    """

    df_industry_manage = spark.table("ddw.t_cockpit_industry_manage")
    # 经纪业务手续费收入 -本期
    df_tmp = df_industry_manage.alias("t") \
        .filter(
        col("t.etl_month").between(lit(v_begin_month), lit(v_end_month)),
        col("t.index_name") == lit("手续费收入")
    ).agg(
        sum(col("t.f5") * 100000000).alias("index_value_now")  # 营业收入完成情况（万）
    ).alias("df_tmp")
    # 经纪业务手续费收入市占率
    df_tmp1 = df_127.alias("t") \
        .filter(
        col("t.month_id").between(lit(v_begin_month), lit(v_end_month))
    ).join(
        other=df_branch_oa.alias("a"),
        on=(
                col("t.book_id") == col("a.yy_book_id")
        ),
        how="inner"
    ).groupBy(
        col("b.oa_branch_id")
    ).agg(
        sum(col("t.f5") + col("t.f8")).alias("index_value_branch")  # 经纪业务手续费收入市占率
    ).select(
        "b.oa_branch_id"
    ).alias("df_tmp1")

    df_y = df_tmp.alias("t") \
        .join(
        other=df_tmp1.alias("t1")
    ).select(
        "t1.oa_branch_id",
        (
            when(
                condition=col("t.index_value_now") != lit(0),
                value=col("t1.index_value_branch") / col("t.index_value_now")
            ).otherwise(lit(0))
        ).alias("actual_complet_value")
    )

    df_t_cockpit_00138 = update_dataframe(
        df_to_update=df_t_cockpit_00138,
        filter_condition=(
                col("index_asses_benchmark") == lit("1") &  # 营业收入
                col("index_type") == lit("1")  # 相对指标
        ),
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=["actual_complet_value"]
    )

    """
    客户资产规模 日均客户保证金完成情况  取数源：交易统计表——日均权益（考核口径），调整后的权益。
    后续修改调整
    cf_stat.t_client_sett_data   ddw.t_client_sett_data
    cf_sett.t_fund_account  edw.h12_fund_account
    cf_busimg.t_ctp_branch_oa_rela  ddw.t_ctp_branch_oa_rela
    """
    df_client_sett_data = spark.table("ddw.t_client_sett_data")
    df_fund_account = spark.table("edw.h12_fund_account")
    df_ctp_branch_oa = spark.table("ddw.t_ctp_branch_oa_rela")

    df_y = df_client_sett_data.alias("t") \
        .filter(
        col("t.month_id").between(lit(v_begin_month), lit(v_end_month))
    ).join(
        other=df_fund_account.alias("b"),
        on=(
                col("t.fund_account_id") == col("b.fund_account_id")
        ),
        how="left"
    ).join(
        other=df_ctp_branch_oa.alias("c"),
        on=(
                col("t.branch_id") == col("c.ctp_branch_id")
        ),
        how="inner"
    ).groupBy(
        col("c.oa_branch_id")
    ).agg(
        sum(
            when(
                condition=lit(v_trade_days) > lit(0),
                value=col("t.avg_trade_rights") / lit(v_trade_days)
            ).otherwise(lit(0))
        ).alias("actual_complet_value")
    ).select(
        "c.oa_branch_id"
    )

    df_t_cockpit_00138 = update_dataframe(
        df_to_update=df_t_cockpit_00138,
        filter_condition=(
                col("index_asses_benchmark") == lit("2") &  # 客户资产规模
                col("index_type") == lit("0")  # 绝对指标
        ),
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=["actual_complet_value"]
    )

    # 更新 日均客户保证金市占率  取数源：交易统计表——日均权益（全口径），调整后的权益/行业
    df_tmp = df_industry_manage.alias("t") \
        .filter(
        col("t.etl_month").between(lit(v_begin_month), lit(v_end_month)),
        col("t.index_name") == lit("客户权益")
    ).agg(
        sum(
            when(
                condition=lit(v_trade_days) != lit(0),
                value=col("t.index_value") * 100000000 / lit(v_trade_days)
            ).otherwise(lit(0))
        ).alias("index_value_now")
    ).alias("df_tmp")

    df_tmp1 = df_client_sett_data.alias("t") \
        .filter(
        col("t.busi_date_during").between(lit(v_begin_month), lit(v_end_month))
    ).join(
        other=df_fund_account.alias("b"),
        on=(
                col("t.fund_account_id") == col("b.fund_account_id")
        ),
        how="left"
    ).join(
        other=df_ctp_branch_oa.alias("c"),
        on=(
                col("t.branch_id") == col("c.ctp_branch_id")
        ),
        how="inner"
    ).groupBy(
        col("c.oa_branch_id")
    ).agg(
        sum(
            when(
                condition=lit(v_trade_days) != lit(0),
                value=col("t.avg_trade_rights") / lit(v_trade_days)
            ).otherwise(lit(0))
        ).alias("index_value_branch")
    ).select(
        "c.oa_branch_id"
    ).alias("df_tmp1")

    df_y = df_tmp1.alias("t") \
        .join(
        other=df_tmp.alias("t1"),
        on=(
                lit("1") == lit("1")
        ),
        how="inner"
    ).select(
        "t.oa_branch_id",
        (
            when(
                condition=col("t1.index_value_now") != lit(0),
                value=col("t.index_value_branch") / col("t1.index_value_now")
            ).otherwise(lit(0))
        ).alias("actual_complet_value")
    )

    df_t_cockpit_00138 = update_dataframe(
        df_to_update=df_t_cockpit_00138,
        filter_condition=(
            col("index_asses_benchmark") == lit("2"),  # 客户资产规模
            col("index_type") == lit("1")  # 相对指标
        ),
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=["actual_complet_value"]
    )

    """
    考核利润 绝对指标 考核利润完成情况（万） 取数源：财务内核表：三、营业利润（亏损以“-”号填列）　
    来源表 cf_busimg.t_cockpit_00127
    """
    df_y = df_127.alias("t") \
        .filter(
        col("t.month_id").between(lit(v_begin_month), lit(v_end_month))
    ).join(
        other=df_branch_oa.alias("b"),
        on=(
                col("t.book_id") == col("b.yy_book_id")
        ),
        how="inner"
    ).groupBy(
        col("t.oa_branch_id")
    ).agg(
        sum(col("t.b18")).alias("actual_complet_value")
    ).select(
        "b.oa_branch_id"
    )

    df_t_cockpit_00138 = update_dataframe(
        df_to_update=df_t_cockpit_00138,
        filter_condition=(
                col("index_asses_benchmark") == lit("3") &  # 考核利润
                col("index_type") == lit("0")  # 绝对指标
        ),
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=["actual_complet_value"]
    )

    # 成交额 绝对指标  客户交易成交金额（双边） 分支机构是双边计算的，直接取CTP的数据，成交额（调整后数据）
    # TODO 需要建表
    df_trade_sum_data = spark.table("ddw.t_trade_sum_data")
    df_tmp = df_trade_sum_data.alias("t") \
        .filter(
        col("t.busi_date_during").between(lit(v_begin_month), lit(v_end_month))
    ).join(
        other=df_fund_account.alias("b"),
        on=(
                col("t.fund_account_id") == col("b.fund_account_id")
        ),
        how="left"
    ).join(
        other=df_ctp_branch_oa.alias("c"),
        on=(
                col("b.branch_id") == col("c.ctp_branch_id")
        ),
        how="inner"
    ).groupBy(
        col("c.oa_branch_id")
    ).agg(
        sum(col("t.done_money")).alias("index_value_branch")
    ).select(
        "c.oa_branch_id"
    ).alias("df_tmp")

    # TODO cf_busimg.t_cockpit_industry_trad 表暂定为 ddw.t_cockpit_industry_trad
    df_cockpit_industry_trad = spark.table("ddw.t_cockpit_industry_trad")

    df_tmp1 = df_cockpit_industry_trad.alias("t") \
        .filter(
        col("t.etl_month").between(lit(v_begin_month), lit(v_end_month))
    ).agg(
        sum(col("t.trad_amt") * 2 * 100000000).alias("index_value_now")
    ).alias("df_tmp1")

    df_y = df_tmp.alias("t") \
        .join(
        other=df_tmp1.alias("t1"),
        on=(
                lit("1") == lit("1")
        ),
        how="inner"
    ).select(
        col("t.oa_branch_id"),
        col("t.index_value_branch").alias("actual_complet_value")
    )

    df_t_cockpit_00138 = update_dataframe(
        df_to_update=df_t_cockpit_00138,
        filter_condition=(
                col("index_asses_benchmark") == lit("4") &  # 成交额
                col("index_type") == lit("0")  # 绝对指标
        ),
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=["actual_complet_value"]
    )

    # 成交额 相对指标  成交额市占率(双边） 分支机构是双边计算的，直接取CTP的数据，成交额（调整后数据）/行业
    df_tmp = df_trade_sum_data.alias("t") \
        .filter(
        col("t.busi_date_during").between(lit(v_begin_month), lit(v_end_month))
    ).join(
        other=df_fund_account.alias("b"),
        on=(
                col("t.fund_account_id") == col("b.fund_account_id")
        ),
        how="left"
    ).join(
        other=df_ctp_branch_oa.alias("c"),
        on=(
                col("b.branch_id") == col("c.ctp_branch_id")
        ),
        how="inner"
    ).groupBy(
        col("c.oa_branch_id")
    ).agg(
        sum(col("t.done_money")).alias("index_value_branch")
    ).select(
        "c.oa_branch_id"
    ).alias("df_tmp")

    df_tmp1 = df_cockpit_industry_trad.alias("t") \
        .filter(
        col("t.etl_month").between(lit(v_begin_month), lit(v_end_month))
    ).agg(
        sum(col("t.trad_amt") * 2 * 100000000).alias("index_value_now")
    ).alias("df_tmp1")

    df_y = df_tmp.alias("t") \
        .join(
        other=df_tmp1.alias("t1"),
        on=(
                lit("1") == lit("1")
        ),
        how="inner"
    ).select(
        col("t.oa_branch_id"),
        (
            when(
                condition=col("t1.index_value_now") != lit(0),
                value=col("t.index_value_branch") / col("t1.index_value_now")
            ).otherwise(0)
        ).alias("actual_complet_value")
    )

    df_t_cockpit_00138 = update_dataframe(
        df_to_update=df_t_cockpit_00138,
        filter_condition=(
                col("index_asses_benchmark") == lit("4") &  # 成交额
                col("index_type") == lit("1")  # 相对指标
        ),
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=["actual_complet_value"]
    )

    # 成交量 绝对指标  客户交易成交量（双边） 分支机构是双边计算的，直接取CTP的数据，成交量（调整后数据）
    df_tmp = df_trade_sum_data.alias("t") \
        .filter(
        col("t.busi_date_during").between(lit(v_begin_month), lit(v_end_month))
    ).join(
        other=df_fund_account.alias("b"),
        on=(
                col("t.fund_account_id") == col("b.fund_account_id")
        ),
        how="left"
    ).join(
        other=df_ctp_branch_oa.alias("c"),
        on=(
                col("b.branch_id") == col("c.ctp_branch_id")
        ),
        how="inner"
    ).groupBy(
        col("c.oa_branch_id")
    ).agg(
        sum(col("t.done_amount")).alias("index_value_branch")
    ).select(
        "c.oa_branch_id"
    ).alias("df_tmp")

    df_t_cockpit_00138 = update_dataframe(
        df_to_update=df_t_cockpit_00138,
        filter_condition=(
                col("index_asses_benchmark") == lit("5") &  # 成交额
                col("index_type") == lit("0")  # 绝对指标
        ),
        df_use_me=df_tmp,
        join_columns=["oa_branch_id"],
        update_columns=["actual_complet_value"]
    )

    # 成交量 相对指标  成交量市占率(双边） 分支机构是双边计算的，直接取CTP的数据，成交量（调整后数据）/行业
    df_tmp = df_trade_sum_data.alias("t") \
        .filter(
        col("t.busi_date_during").between(lit(v_begin_month), lit(v_end_month))
    ).join(
        other=df_fund_account.alias("b"),
        on=(
                col("t.fund_account_id") == col("b.fund_account_id")
        ),
        how="left"
    ).join(
        other=df_ctp_branch_oa.alias("c"),
        on=(
                col("b.branch_id") == col("c.ctp_branch_id")
        ),
        how="inner"
    ).groupBy(
        col("c.oa_branch_id")
    ).agg(
        sum(col("t.done_amount")).alias("index_value_branch")
    ).select(
        "c.oa_branch_id"
    ).alias("df_tmp")

    df_tmp1 = df_cockpit_industry_trad.alias("t") \
        .filter(
        col("t.etl_month").between(lit(v_begin_month), lit(v_end_month))
    ).agg(
        sum(col("t.trad_num") * 2 * 100000000).alias("index_value_now")
    ).alias("df_tmp1")

    df_y = df_tmp.alias("t") \
        .join(
        other=df_tmp1.alias("t1"),
        on=(
                lit("1") == lit("1")
        ),
        how="inner"
    ).select(
        col("t.oa_branch_id"),
        (
            when(
                condition=col("t1.index_value_now") != lit(0),
                value=col("t.index_value_branch") / col("t1.index_value_now")
            ).otherwise(0)
        ).alias("actual_complet_value")
    )

    df_t_cockpit_00138 = update_dataframe(
        df_to_update=df_t_cockpit_00138,
        filter_condition=(
                col("index_asses_benchmark") == lit("5") &  # 成交额
                col("index_type") == lit("1")  # 相对指标
        ),
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=["actual_complet_value"]
    )
    """
    --新增有效客户数
    绝对指标
    新增直接开发有效客户数量
    （1）所选择的月份区间中，新开户的客户数量
    （2）减掉:
    业绩关系查询中，所选的月份区间中，满足以下三个
    a.关系类型为“居间关系”，
    b.关系状态为“有效”，
    c.审批状态为“审批通过”，
    筛选条件得到的客户。
    （3）剩余客户数量，
    在所选择的月份区间，有交易的客户数量
    b.关系状态为“有效”，
    c.审批状态为“审批通过”，
    """
    df_broker_investor_rela = spark.table("ods.t_ds_crm_broker_investor_rela")
    df_crm_broker = spark.table("ods.t_ds_crm_broker")
    df_mdp_dept00 = spark.table("ods.t_ds_mdp_dept00")
    df_dc_investor = spark.table("ods.t_ds_dc_investor")
    df_branch = spark.table("edw.h11_branch")
    df_tmp = df_broker_investor_rela.alias("a") \
        .filter(
        col("a.rela_sts") == lit("A"),  # 有效
        col("a.approve_sts") == lit("0"),  # 审核通过
        col("a.data_pct").isNotNull()
    ).join(
        other=df_crm_broker.alias("b"),
        on=(
                col("a.broker_id") == col("b.broker_id")
        ),
        how="inner"
    ).join(
        other=df_mdp_dept00.alias("f"),
        on=(
                col("b.department_id") == col("f.chdeptcode")
        ),
        how="inner"
    ).join(
        other=df_dc_investor.alias("c"),
        on=(
                col("a.investor_id") == col("c.investor_id")
        ),
        how="inner"
    ).join(
        other=df_fund_account.alias("x"),
        on=(
                col("c.investor_id") == col("x.fund_account_id")
        ),
        how="left"
    ).join(
        other=df_branch.alias("x1"),
        on=(
                col("x.branch_id") == col("x1.branch_id")
        ),
        how="left"
    ).join(
        other=df_ctp_branch_oa.alias("x2"),
        on=(
                col("x1.branch_id") == col("x2.ctp_branch_id")
        ),
        how="left"
    ).select(
        col("a.broker_id"),
        col("b.broker_nam").alias("broker_name"),
        col("a.investor_id").alias("fund_account_id"),
        col("c.investor_nam").alias("client_name"),
        regexp_replace(col("a.st_dt"), pattern="-", replacement=""),
        regexp_replace(col("a.end_dt"), pattern="-", replacement=""),
        (
            when(
                condition=col("a.broker_rela_typ") == lit("301"),
                value="居间关系"
            ).when(
                condition=col("a.broker_rela_typ") == lit("001"),
                value="开发关系"
            ).when(
                condition=col("a.broker_rela_typ") == lit("002"),
                value="服务关系"
            ).when(
                condition=col("a.broker_rela_typ") == lit("003"),
                value="维护关系"
            ).otherwise("-")
        ).alias("broker_rela_type"),
        col("a.data_pct"),
        (
            when(
                condition=col("a.rela_sts") == lit("A"),
                value="有效"
            ).when(
                condition=col("a.rela_sts") == lit("S"),
                value="停止使用"
            ).otherwise("-")
        ).alias("rela_status"),
        col("a.approve_dt"),
        (
            when(
                condition=col("a.approve_sts") == lit("0"),
                value="审核通过"
            ).when(
                condition=col("a.approve_sts") == lit("1"),
                value="审核不通过"
            ).when(
                condition=col("a.approve_sts") == lit("2"),
                value="等待审核"
            ).otherwise("-")
        ).alias("approve_sts"),
        col("a.comment_desc"),
        col("a.check_comments"),
        (
            when(
                condition=regexp_replace(col("a.st_dt"), pattern="-", replacement="") < lit(v_begin_date),
                value=lit(v_begin_date)
            ).when(
                condition=regexp_replace(col("a.st_dt"), pattern="-", replacement="") >= lit(v_begin_date),
                value=regexp_replace(col("a.st_dt"), pattern="-", replacement="")
            ).otherwise("")
        ).alias("rela_begin_date"),
        (
            when(
                condition=regexp_replace(col("a.end_dt"), pattern="-", replacement="") <= lit(v_end_date),
                value=regexp_replace(col("a.end_dt"), pattern="-", replacement="")
            ).when(
                condition=regexp_replace(col("a.end_dt"), pattern="-", replacement="") > lit(v_end_date),
                value=lit(v_end_date)
            ).otherwise("")
        ).alias("rela_end_date")
    ).alias("df_tmp")

    df_t = df_tmp.alias("t") \
        .filter(
        col("t.real_begin_date") <= col("t.real_end_date")
    ).groupBy(
        col("t.fund_account_id"),
        col("t.broker_rela_type")
    ).select(
        col("t.fund_account_id"),
        col("t.broker_rela_type")
    ).alias("df_t")

    return_to_hive(
        spark=spark,
        df_result=df_t,
        target_table="ddw.tmp_cockpit_00138_1",
        insert_mode="overwrite"
    )

    df_00138_1 = spark.table("ddw.tmp_cockpit_00138_1")
    df_tmp = df_fund_account.alias("t") \
        .filter(
        col("t.open_date").between(lit(v_begin_date), lit(v_end_date))
    ).join(
        other=df_00138_1.alias("a").filter(
            col("a.broker_rela_type") == "居间关系"
        ),
        on=(
                col("t.fund_account_id") == col("a.fund_account_id")
        ),
        how="left_anti"
    ).select(
        col("t.fund_account_id")
    ).alias("df_tmp")

    df_tmp1 = df_trade_sum_data.alias("t") \
        .filter(
        col("t.busi_date_during").between(lit(v_begin_month), lit(v_end_month)),
        col("t.total_transfee") != lit(0)  # 手续费不为0，有交易
    ).join(
        other=df_tmp.alias("a"),
        on=(
                col("t.fund_account_id") == col("a.fund_account_id")
        ),
        how="inner"
    ).groupBy(
        col("t.fund_account_id")
    ).select(
        col("t.fund_account_id")
    ).alias("df_tmp1")

    df_y = df_tmp1.alias("t") \
        .join(
        other=df_fund_account.alias("b"),
        on=(
                col("t.fund_account_id") == col("b.fund_account_id")
        ),
        how="left"
    ).join(
        other=df_ctp_branch_oa.alias("c"),
        on=(
                col("b.branch_id") == col("c.ctp_branch_id")
        ),
        how="left"
    ).groupBy(
        col("c.oa_branch_id")
    ).agg(
        count("*").alias("actual_complet_value")
    ).select(
        col("c.oa_branch_id")
    )

    df_t_cockpit_00138 = update_dataframe(
        df_to_update=df_t_cockpit_00138,
        filter_condition=(
                col("index_asses_benchmark") == lit("6") &  # 新增有效客户数
                col("index_type") == lit("0") &  # 绝对指标
                col("index_name").like("新增直接开发有效客户数量")
        ),
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=["actual_complet_value"]
    )
    """
    --新增有效客户数
    绝对指标
    新增有效客户数量（户）
    （1）所选择的月份区间中，新开户的客户数量
    （2）减掉:
    业绩关系查询中，所选的月份区间中，满足以下两个
    a.关系状态为
    "有效"，
    b.审批状态为
    "审批通过“，
    筛选条件得到的客户。
    （3）剩余客户数量，
    在所选择的月份区间，有交易的客户数量
    """
    # 新开客户
    df_tmp = df_fund_account.alias("t") \
        .filter(
        col("t.open_date").between(lit(v_begin_date), lit(v_end_date))
    ).join(
        other=df_00138_1.alias("a"),
        on=(
                col("t.fund_account_id") == col("b.fund_account_id")
        ),
        how="left_anti"
    ).select(
        col("t.fund_account_id")
    ).alias("df_tmp")

    df_tmp1 = df_trade_sum_data.alias("t") \
        .filter(
        col("t.busi_date_during").between(lit(v_begin_date), lit(v_end_date)),
        col("t.total_transfee") != lit(0)
    ).join(
        other=df_tmp.alias("a"),
        on=(
                col("t.fund_account_id") == col("a.fund_account_id")
        ),
        how="inner"
    ).groupBy(
        col("t.fund_account_id")
    ).select(
        col("t.fund_account_id")
    ).alias("df_tmp1")

    df_y = df_tmp1.alias("t") \
        .join(
        other=df_fund_account.alias("b"),
        on=(
                col("t.fund_account_id") == col("b.fund_account_id")
        ),
        how="left"
    ).join(
        other=df_ctp_branch_oa.alias("c"),
        on=(
                col("b.branch_id") == col("c.ctp_branch_id")
        ),
        how="left"
    ).groupBy(
        col("c.oa_branch_id")
    ).agg(
        count("*").alias("actual_complet_value")
    ).select(
        col("c.oa_branch_id")
    )

    df_t_cockpit_00138 = update_dataframe(
        df_to_update=df_t_cockpit_00138,
        filter_condition=(
                col("index_asses_benchmark") == lit("6") &  # 新增有效客户数
                col("index_type") == lit("0") &  # 绝对指标
                col("index_name").like("新增有效客户数量")
        ),
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=["actual_complet_value"]
    )

    # 产品销售（万） 绝对指标  新增公司统一组织的产品销售额  （资管表3月度销售人员保有奖励分配情况—新增量）
    df_cockpit_00096 = spark.table("cf_busimg.t_cockpit_00096")
    df_y = df_cockpit_00096.alias("a") \
        .filter(
        col("t.busi_date").between(lit(v_begin_date), lit(v_end_date)),
    ).join(
        other=df_fund_account.alias("b"),
        on=(
            col("a.id_no") == col("b.id_no"),
            col("a.client_name") == col("b.client_name")
        ),
        how="inner"
    ).join(
        other=df_ctp_branch_oa.alias("c"),
        on=(
                col("b.branch_id") == col("c.ctp_branch_id")
        ),
        how="inner"
    ).groupBy(
        col("c.oa_branch_id")
    ).agg(
        sum(
            when(
                condition=col("a.wh_trade_type").isin(lit("0"), lit("1")),
                value=col("a.confirm_share")
            ).otherwise(lit(0))
        ).alias("actual_complet_value")  # 新增量
    ).select(
        col("c.oa_branch_id")
    )

    df_t_cockpit_00138 = update_dataframe(
        df_to_update=df_t_cockpit_00138,
        filter_condition=(
                col("index_asses_benchmark") == lit("7") &  # 产品销售
                col("index_type") == lit("0")  # 绝对指标
        ),
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=["actual_complet_value"]
    )

    """
    / *
    “调整项”和“完成值”
    逻辑如下：
    一、时间区间为：第一季度，无调整项，显示为“0”；调整季度，无调整季度，显示为“-”。完成值等于实际完成值。
    二、时间区间为：第二、三、四季度、年度。
    1、如果，当前季度的实际完成值大于目标值，无调整项，显示为0。调整季度，显示为“-”。完成值等于实际完成值
    2、如果，当前季度的实际完成值小于目标值；
    （1）而且，之前季度的实际完成值小于或等于目标值。则之前季度无补齐当前季度的完成值，无调整项，显示为“0”，调整季度，无调整季度，显示为“-”。完成值等于实际完成值
    （2）而且，之前季度的实际完成值大于目标值。则差值作为当前季度的“调整项”，补齐当前季度的完成值。调整季度，显示为“调整项”所在的季度。完成值 = 实际完成值 + 调整项
    注：可能存在之前多个季度，对当前季度的调整的情况。
    """

    if v_quarter_id == 1:
        # 第一季度
        df_y = df_t_cockpit_00138.alias("t") \
            .filter(
            col("t.year_id") == lit(v_year_id),
            col("t.quarter_id") == lit(v_quarter_id)
        ).select(
            col("t.oa_branch_id"),
            col("t.index_id"),
            col("t.index_asses_benchmark"),
            col("t.index_type"),
            col("actual_complet_value").alias("complet_value"),
            lit("-").alias("adjust_quarter_1"),
            lit(0).alias("adjust_1"),
            lit("-").alias("adjust_quarter_2"),
            lit(0).alias("adjust_2"),
            lit("-").alias("adjust_quarter_3"),
            lit(0).alias("adjust_3")
        )

        df_t_cockpit_00138 = update_dataframe(
            df_to_update=df_t_cockpit_00138,
            df_use_me=df_y,
            join_columns=[
                "oa_branch_id",
                "index_id",
                "index_asses_benchmark",
                "index_type"
            ],
            update_columns=[
                "complet_value",
                "adjust_quarter_1",
                "adjust_1",
                "adjust_quarter_2",
                "adjust_2",
                "adjust_quarter_3",
                "adjust_3",
            ]
        )

    elif v_quarter_id == 2:
        # --第二季度
        df_tmp = df_t_cockpit_00138.alias("t") \
            .filter(
            col("t.year_id") == lit(v_year_id),
            col("t.quarter_id") == lit(v_last_quarter_id_1)
        ).select(
            col("t.year_id"),
            col("t.oa_branch_id"),
            col("t.index_id"),
            col("t.index_asses_benchmark"),
            col("t.index_type"),
            (
                when(
                    condition=col("t.actual_complet_value") <= col("t.year_target_value"),  # 上一季度的实际完成值小于等于目标值
                    value=lit(0)
                ).otherwise(col("actual_complet_value") - col("t.year_target_value"))
            ).alias("adjust_value")  # 调整值
        ).alias("df_tmp")

        df_tmp1 = df_t_cockpit_00138.alias("t") \
            .filter(
            col("t.year_id") == lit(v_year_id),
            col("t.quarter_id") == lit(v_quarter_id)
        ).select(
            col("t.year_id"),
            col("t.quarter_id"),
            col("t.oa_branch_id"),
            col("t.index_id"),
            col("t.index_asses_benchmark"),
            col("t.index_type"),
            col("t.actual_complet_value"),
            (
                when(
                    condition=col("t.actual_complet_value") > col("t.year_target_value"),  # 当前季度的实际完成值大于目标值
                    value=lit("-")
                ).otherwise("")
            ).alias("adjust_quarter_2"),  # 调整所在季度2
            lit(0).alias("adjust_2")  # 调整值2
        ).alias("df_tmp1")

        df_y = df_tmp1.alias("t") \
            .join(
            other=df_tmp.alias("a"),
            on=(
                    col("t.year_id") == col("a.year_id") &
                    col("t.oa_branch_id") == col("a.oa_branch_id") &
                    col("t.index_id") == col("a.index_id") &
                    col("t.index_asses_benchmark") == col("a.index_asses_benchmark") &
                    col("t.index_type") == col("a.index_type")
            ),
            how="inner"
        ).select(
            col("t.year_id"),
            col("t.quarter_id"),
            col("t.oa_branch_id"),
            col("t.index_id"),
            col("t.index_asses_benchmark"),
            col("t.index_type"),
            (
                when(
                    condition=col("t.adjust_quarter_2").isNull() & col("a.adjust_value") > lit(0),  # 当前季度的实际完成值大于目标值
                    value=lit("1季度")
                ).otherwise("-")
            ).alias("adjust_quarter_1"),
            (
                when(
                    condition=col("t.adjust_quarter_2").isNull() & col("a.adjust_value") > lit(0),  # 当前季度的实际完成值大于目标值
                    value=col("a.adjust_value")
                ).otherwise(0)
            ).alias("adjust_1"),
            lit("-").alias("adjust_quarter_2"),
            col("t.adjust_2"),
            lit("-").alias("adjust_quarter_3"),
            lit(0).alias("t.adjust_3"),
            (
                when(
                    condition=col("t.adjust_quarter_2") == lit("-"),  # 当前季度的实际完成值大于目标值
                    value=col("t.actual_complet_value")
                ).when(
                    condition=col("t.adjust_quarter_2").isNull(),  # 当前季度的实际完成值大于目标值
                    value=col("t.actual_complet_value") + coalesce(col("a.adjust_value"), lit(0))
                ).otherwise(0)
            ).alias("complet_value"),
        )

        df_t_cockpit_00138 = update_dataframe(
            df_to_update=df_t_cockpit_00138,
            df_use_me=df_y,
            join_columns=[
                "oa_branch_id",
                "index_id",
                "index_asses_benchmark",
                "index_type"
            ],
            update_columns=[
                "complet_value",
                "adjust_quarter_1",
                "adjust_1",
                "adjust_quarter_2",
                "adjust_2",
                "adjust_quarter_3",
                "adjust_3",
            ]
        )

    elif v_quarter_id == 3:
        # 第三季度
        df_tmp1 = df_t_cockpit_00138.alias("t") \
            .filter(
            col("t.year_id") == lit(v_year_id),
            col("t.quarter_id") == lit(v_last_quarter_id_1)
        ).select(
            col("t.year_id"),
            col("t.oa_branch_id"),
            col("t.index_id"),
            col("t.index_asses_benchmark"),
            col("t.index_type"),
            (
                when(
                    condition=col("t.actual_complet_value") <= col("t.year_target_value"),  # 上一季度的实际完成值小于等于目标值
                    value=lit(0)
                ).otherwise(col("actual_complet_value") - col("t.year_target_value"))
            ).alias("adjust_value")  # 调整值
        ).alias("df_tmp1")

        df_tmp2 = df_t_cockpit_00138.alias("t") \
            .filter(
            col("t.year_id") == lit(v_year_id),
            col("t.quarter_id") == lit(v_last_quarter_id_2)
        ).select(
            col("t.year_id"),
            col("t.oa_branch_id"),
            col("t.index_id"),
            col("t.index_asses_benchmark"),
            col("t.index_type"),
            (
                when(
                    condition=col("t.actual_complet_value") <= col("t.year_target_value"),  # 上一季度的实际完成值小于等于目标值
                    value=lit(0)
                ).otherwise(col("actual_complet_value") - col("t.year_target_value"))
            ).alias("adjust_value")  # 调整值
        ).alias("df_tmp2")

        df_tmp3 = df_t_cockpit_00138.alias("t") \
            .filter(
            col("t.year_id") == lit(v_year_id),
            col("t.quarter_id") == lit(v_quarter_id)
        ).select(
            col("t.year_id"),
            col("t.quarter_id"),
            col("t.oa_branch_id"),
            col("t.index_id"),
            col("t.index_asses_benchmark"),
            col("t.index_type"),
            col("t.actual_complet_value"),
            (
                when(
                    condition=col("t.actual_complet_value") > col("t.year_target_value"),  # 上一季度的实际完成值小于等于目标值
                    value=lit("-")
                ).otherwise("")
            ).alias("adjust_quarter_3"),  # 调整所在季度2
            lit(0).alias("adjust_3")  # 调整值2
        ).alias("df_tmp3")

        df_y = df_tmp3.alias("t") \
            .join(
            other=df_tmp1.alias("a"),
            on=(
                    col("t.year_id") == col("a.year_id") &
                    col("t.oa_branch_id") == col("a.oa_branch_id") &
                    col("t.index_id") == col("a.index_id") &
                    col("t.index_asses_benchmark") == col("a.index_asses_benchmark") &
                    col("t.index_type") == col("a.index_type")
            ),
            how="left"
        ).join(
            other=df_tmp2.alias("b"),
            on=(
                    col("t.year_id") == col("b.year_id") &
                    col("t.oa_branch_id") == col("b.oa_branch_id") &
                    col("t.index_id") == col("b.index_id") &
                    col("t.index_asses_benchmark") == col("b.index_asses_benchmark") &
                    col("t.index_type") == col("b.index_type")
            ),
            how="left"
        ).select(
            col("t.year_id"),
            col("t.quarter_id"),
            col("t.oa_branch_id"),
            col("t.index_id"),
            col("t.index_asses_benchmark"),
            col("t.index_type"),
            (
                when(
                    condition=col("t.adjust_quarter_3").isNull() & col("a.adjust_value") > lit(0),  # 当前季度的实际完成值大于目标值
                    value=lit("1季度")
                ).otherwise("-")
            ).alias("adjust_quarter_1"),
            (
                when(
                    condition=col("t.adjust_quarter_3").isNull() & col("a.adjust_value") > lit(0),  # 当前季度的实际完成值大于目标值
                    value=col("a.adjust_value")
                ).otherwise(0)
            ).alias("adjust_1"),
            (
                when(
                    condition=col("t.adjust_quarter_3").isNull() & col("b.adjust_value") > lit(0),  # 当前季度的实际完成值大于目标值
                    value=lit("2季度")
                ).otherwise("-")
            ).alias("adjust_quarter_2"),
            (
                when(
                    condition=col("t.adjust_quarter_3").isNull() & col("b.adjust_value") > lit(0),  # 当前季度的实际完成值大于目标值
                    value=col("b.adjust_value")
                ).otherwise(0)
            ).alias("adjust_2"),
            lit("-").alias("adjust_quarter_3"),
            lit(0).alias("t.adjust_3"),
            (
                when(
                    condition=col("t.adjust_quarter_3") == lit("-"),  # 当前季度的实际完成值大于目标值
                    value=col("t.actual_complet_value")
                ).when(
                    condition=col("t.adjust_quarter_3").isNull(),  # 当前季度的实际完成值大于目标值
                    value=col("t.actual_complet_value") + coalesce(col("a.adjust_value"), lit(0)) + coalesce(
                        col("b.adjust_value"), lit(0))
                ).otherwise(0)
            ).alias("complet_value"),
        )

        df_t_cockpit_00138 = update_dataframe(
            df_to_update=df_t_cockpit_00138,
            df_use_me=df_y,
            join_columns=[
                "oa_branch_id",
                "index_id",
                "index_asses_benchmark",
                "index_type"
            ],
            update_columns=[
                "complet_value",
                "adjust_quarter_1",
                "adjust_1",
                "adjust_quarter_2",
                "adjust_2",
                "adjust_quarter_3",
                "adjust_3",
            ]
        )
    else:
        # 第四季度
        df_tmp1 = df_t_cockpit_00138.alias("t") \
            .filter(
            col("t.year_id") == lit(v_year_id),
            col("t.quarter_id") == lit(v_last_quarter_id_1)
        ).select(
            col("t.year_id"),
            col("t.oa_branch_id"),
            col("t.index_id"),
            col("t.index_asses_benchmark"),
            col("t.index_type"),
            (
                when(
                    condition=col("t.actual_complet_value") <= col("t.year_target_value"),  # 上一季度的实际完成值小于等于目标值
                    value=lit(0)
                ).otherwise(col("actual_complet_value") - col("t.year_target_value"))
            ).alias("adjust_value")  # 调整值
        ).alias("df_tmp1")

        df_tmp2 = df_t_cockpit_00138.alias("t") \
            .filter(
            col("t.year_id") == lit(v_year_id),
            col("t.quarter_id") == lit(v_last_quarter_id_2)
        ).select(
            col("t.year_id"),
            col("t.oa_branch_id"),
            col("t.index_id"),
            col("t.index_asses_benchmark"),
            col("t.index_type"),
            (
                when(
                    condition=col("t.actual_complet_value") <= col("t.year_target_value"),  # 上一季度的实际完成值小于等于目标值
                    value=lit(0)
                ).otherwise(col("actual_complet_value") - col("t.year_target_value"))
            ).alias("adjust_value")  # 调整值
        ).alias("df_tmp2")

        df_tmp3 = df_t_cockpit_00138.alias("t") \
            .filter(
            col("t.year_id") == lit(v_year_id),
            col("t.quarter_id") == lit(v_last_quarter_id_3)
        ).select(
            col("t.year_id"),
            col("t.oa_branch_id"),
            col("t.index_id"),
            col("t.index_asses_benchmark"),
            col("t.index_type"),
            (
                when(
                    condition=col("t.actual_complet_value") <= col("t.year_target_value"),  # 上一季度的实际完成值小于等于目标值
                    value=lit(0)
                ).otherwise(col("actual_complet_value") - col("t.year_target_value"))
            ).alias("adjust_value")  # 调整值
        ).alias("df_tmp3")

        df_tmp4 = df_t_cockpit_00138.alias("t") \
            .filter(
            col("t.year_id") == lit(v_year_id),
            col("t.quarter_id") == lit(v_quarter_id)
        ).select(
            col("t.year_id"),
            col("t.quarter_id"),
            col("t.oa_branch_id"),
            col("t.index_id"),
            col("t.index_asses_benchmark"),
            col("t.index_type"),
            col("t.actual_complet_value"),
            (
                when(
                    condition=col("t.actual_complet_value") > col("t.year_target_value"),  # 当前季度的实际完成值大于目标值
                    value=lit("-")
                ).otherwise("")
            ).alias("adjust_quarter_4"),  # 调整所在季度4
            lit(0).alias("adjust_4")  # 调整值4
        ).alias("df_tmp4")

        df_y = df_tmp4.alias("t") \
            .join(
            other=df_tmp1.alias("a"),
            on=(
                    col("t.year_id") == col("a.year_id") &
                    col("t.oa_branch_id") == col("a.oa_branch_id") &
                    col("t.index_id") == col("a.index_id") &
                    col("t.index_asses_benchmark") == col("a.index_asses_benchmark") &
                    col("t.index_type") == col("a.index_type")
            ),
            how="left"
        ).join(
            other=df_tmp2.alias("b"),
            on=(
                    col("t.year_id") == col("b.year_id") &
                    col("t.oa_branch_id") == col("b.oa_branch_id") &
                    col("t.index_id") == col("b.index_id") &
                    col("t.index_asses_benchmark") == col("b.index_asses_benchmark") &
                    col("t.index_type") == col("b.index_type")
            ),
            how="left"
        ).join(
            other=df_tmp3.alias("c"),
            on=(
                    col("t.year_id") == col("c.year_id") &
                    col("t.oa_branch_id") == col("c.oa_branch_id") &
                    col("t.index_id") == col("c.index_id") &
                    col("t.index_asses_benchmark") == col("c.index_asses_benchmark") &
                    col("t.index_type") == col("c.index_type")
            ),
            how="left"
        ).select(
            col("t.year_id"),
            col("t.quarter_id"),
            col("t.oa_branch_id"),
            col("t.index_id"),
            col("t.index_asses_benchmark"),
            col("t.index_type"),
            (
                when(
                    condition=col("t.adjust_quarter_4").isNull() & col("a.adjust_value") > lit(0),  # 当前季度的实际完成值大于目标值
                    value=lit("1季度")
                ).otherwise("-")
            ).alias("adjust_quarter_1"),
            (
                when(
                    condition=col("t.adjust_quarter_4").isNull() & col("a.adjust_value") > lit(0),  # 当前季度的实际完成值大于目标值
                    value=col("a.adjust_value")
                ).otherwise(0)
            ).alias("adjust_1"),
            (
                when(
                    condition=col("t.adjust_quarter_4").isNull() & col("b.adjust_value") > lit(0),  # 当前季度的实际完成值大于目标值
                    value=lit("2季度")
                ).otherwise("-")
            ).alias("adjust_quarter_2"),
            (
                when(
                    condition=col("t.adjust_quarter_4").isNull() & col("b.adjust_value") > lit(0),  # 当前季度的实际完成值大于目标值
                    value=col("b.adjust_value")
                ).otherwise(0)
            ).alias("adjust_2"),
            (
                when(
                    condition=col("t.adjust_quarter_4").isNull() & col("c.adjust_value") > lit(0),  # 当前季度的实际完成值大于目标值
                    value=lit("2季度")
                ).otherwise("-")
            ).alias("adjust_quarter_3"),
            (
                when(
                    condition=col("t.adjust_quarter_4").isNull() & col("c.adjust_value") > lit(0),  # 当前季度的实际完成值大于目标值
                    value=col("b.adjust_value")
                ).otherwise(0)
            ).alias("adjust_3"),
            (
                when(
                    condition=col("t.adjust_quarter_4") == lit("-"),  # 当前季度的实际完成值大于目标值
                    value=col("t.actual_complet_value")
                ).when(
                    condition=col("t.adjust_quarter_4").isNull(),  # 当前季度的实际完成值大于目标值
                    value=col("t.actual_complet_value") + coalesce(col("a.adjust_value"), lit(0)) + coalesce(
                        col("b.adjust_value"), lit(0)) + coalesce(col("c.adjust_value"), lit(0))
                ).otherwise(0)
            ).alias("complet_value"),
        )

        df_t_cockpit_00138 = update_dataframe(
            df_to_update=df_t_cockpit_00138,
            df_use_me=df_y,
            join_columns=[
                "oa_branch_id",
                "index_id",
                "index_asses_benchmark",
                "index_type"
            ],
            update_columns=[
                "complet_value",
                "adjust_quarter_1",
                "adjust_1",
                "adjust_quarter_2",
                "adjust_2",
                "adjust_quarter_3",
                "adjust_3",
            ]
        )
    """
    目标完成率 = 完成值 / 目标值
    得分:
    1、如果，目标达成率大于分值上限比例，那么，得分 = 权重 * 分支上线比例 * 100
    2、如果，目标达成率小于等于0，那么，得分 = 0
    3、如果，目标达成率，大于0，小于等于分值上限比例，那么，得分 = 目标达成率 * 权重 * 100
    """
    df_y = df_t_cockpit_00138.alias("t") \
        .filter(
        col("t.year_id") == lit(v_year_id),
        col("t.quarter_id") == lit(v_quarter_id)
    ).select(
        col("t.oa_branch_id"),
        col("t.index_id"),
        col("t.index_asses_benchmark"),
        col("t.index_type"),
        (
            when(
                condition=col("t.year_target_value") != lit(0),
                value=col("t.complet_value") / col("t.year_target_value")
            ).otherwise(0)
        ).alias("complet_achievement_rate")
    )

    df_t_cockpit_00138 = update_dataframe(
        df_to_update=df_t_cockpit_00138,
        df_use_me=df_y,
        join_columns=[
            "oa_branch_id",
            "index_id",
            "index_asses_benchmark",
            "index_type"
        ],
        update_columns=["complet_achievement_rate"]
    )

    df_y = df_t_cockpit_00138.alias("t") \
        .filter(
        col("t.year_id") == lit(v_year_id),
        col("t.quarter_id") == lit(v_quarter_id)
    ).select(
        col("t.oa_branch_id"),
        col("t.index_id"),
        col("t.index_asses_benchmark"),
        col("t.index_type"),
        (
            when(
                condition=col("t.complet_achievement_rate") > col("t.upper_limit_score") / 100,
                value=col("t.weight_rate") * col("t.upper_limit_score")
            ).when(
                condition=col("t.complet_achievement_rate") > lit(0) & col("t.complet_achievement_rate") <= col(
                    "t.upper_limit_score") / 100,
                value=col("t.complet_achievement_rate") * col("t.weight_rate") * lit(100)
            ).when(
                condition=col("t.complet_achievement_rate") <= lit(0),
                value=lit(0)
            ).otherwise(0)
        ).alias("get_score")
    )

    df_t_cockpit_00138 = update_dataframe(
        df_to_update=df_t_cockpit_00138,
        df_use_me=df_y,
        join_columns=[
            "oa_branch_id",
            "index_id",
            "index_asses_benchmark",
            "index_type"
        ],
        update_columns=["get_score"]
    )

    return_to_hive(
        spark=spark,
        df_result=df_t_cockpit_00138,
        target_table="ddw.t_cockpit_00138",
        insert_mode="overwrite",
        partition_column=["year_id", "quarter_id"],
        partition_value=[v_year_id, v_quarter_id]
    )
