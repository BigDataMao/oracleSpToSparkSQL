# -*- coding: utf-8 -*-
"""
年度任务目标填报表-校验
"""
import logging

from pyspark.sql.functions import col, lit, min, max, count, sum, coalesce, when, trim, regexp_replace

from utils.task_env import return_to_hive, update_dataframe
from utils.date_utils import get_date_period_and_days


def p_cockpit_00135_data(spark, busi_date):
    i_year_id = busi_date[:4]
    v_last_year_begin_month = str(int(i_year_id) - 1) + '01'
    v_last_year_end_month = str(int(i_year_id) - 1) + '12'

    (
        v_last_year_begin_date,
        v_last_year_end_date,
        v_last_year_trade_days
    ) = get_date_period_and_days(
        spark=spark,
        begin_month=v_last_year_begin_month,
        end_month=v_last_year_end_month
    )

    """
    更新 上年度完成值
    营业收入（万） 绝对指标 营业收入完成情况  取数源：财务内核表——营业收入，调整后的收入
    来源表 cf_busimg.t_cockpit_00127
    """

    df_135_y = spark.table("ddw.t_cockpit_00135") \
        .filter(
        col("year_id") == lit(i_year_id),
    )

    df_y = spark.table("ddw.t_cockpit_00127").alias("t") \
        .filter(
        col("t.month_id") == lit(v_last_year_end_month)
    ).join(
        other=spark.table("ddw.t_yy_branch_oa_rela").alias("b"),
        on=(col("t.book_id") == col("b.yy_book_id")),
        how="inner"
    ).groupBy(
        col("b.oa_branch_id")
    ).agg(
        sum(col("t.adjusted_income")).alias("last_year_income")
    ).select(
        "b.oa_branch_id",
        sum(col("t.g5")).alias("LAST_YEAR_COMPLET_VALUE"),
    )

    df_135_y = update_dataframe(
        df_to_update=df_135_y.filter(
            col("INDEX_ASSES_BENCHMARK") == lit("1"),  # 营业收入
            col("INDEX_TYPE") == lit("0"),  # 绝对指标
        ),
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=["LAST_YEAR_COMPLET_VALUE"]
    )

    """
    经纪业务手续费收入市占率 取数源：财务内核表：手续费及佣金净收入+交易所减免返还/行业手续费收入
    """

    tmp = spark.table("ddw.T_COCKPIT_INDUSTRY_MANAGE").alias("t") \
        .filter(
        col("t.index_name") == lit("手续费收入"),
        col("t.etl_month").between(v_last_year_begin_month, v_last_year_end_month)
    ).agg(
        sum(col("t.index_value") * 100000000).alias("index_value_now")  # 经纪业务手续费收入 -本期
    ).select(
        "index_value_now"
    )

    tmp_1 = spark.table("ddw.t_cockpit_00127").alias("t") \
        .filter(
        col("t.month_id") == lit(v_last_year_end_month)
    ).join(
        other=spark.table("ddw.t_yy_branch_oa_rela").alias("b"),
        on=(col("t.book_id") == col("b.yy_book_id")),
        how="inner"
    ).groupBy(
        col("b.oa_branch_id")
    ).agg(
        sum(col("t.g6") + col("t.g8")).alias("LAST_YEAR_COMPLET_VALUE")
    ).select(
        "b.oa_branch_id",
        "LAST_YEAR_COMPLET_VALUE"
    )

    df_y = tmp.alias("t") \
        .crossJoin(tmp_1.alias("t1")) \
        .select(
        "t1.oa_branch_id",
        when(
            coalesce(col("t.index_value_now"), lit(0)) != lit(0),
            coalesce(col("t1.LAST_YEAR_COMPLET_VALUE"), lit(0)) / coalesce(col("t.index_value_now"), lit(0))
        ).otherwise(lit(0)).alias("LAST_YEAR_COMPLET_VALUE")
    )

    df_135_y = update_dataframe(
        df_to_update=df_135_y.filter(
            col("INDEX_ASSES_BENCHMARK") == lit("1"),  # 营业收入
            col("INDEX_TYPE") == lit("1"),  # 相对指标
        ),
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=["LAST_YEAR_COMPLET_VALUE"]
    )

    """
    客户资产规模 日均客户保证金完成情况 取数源：交易统计表——日均权益（考核口径），调整后的权益。
    """
    # TODO: 后续修改调整

    # TODO: CF_STAT.T_CLIENT_SETT_DATA需要采集

    df_y = spark.table("ods.cf_stat_t_client_sett_data").alias("t") \
        .filter(
        col("t.busi_date_during").between(v_last_year_begin_month, v_last_year_end_month)
    ).join(
        other=spark.table("edw.h12_fund_account").alias("b"),
        on=(col("t.fund_account_id") == col("b.fund_account_id")),
        how="left"
    ).join(
        other=spark.table("ddw.t_yy_branch_oa_rela").alias("c"),
        on=(col("b.branch_id") == col("c.CTP_BRANCH_ID")),
        how="inner"
    ).groupBy(
        col("c.oa_branch_id")
    ).agg(
        sum(
            when(
                v_last_year_trade_days != lit(0),
                col("t.avg_trade_rights") / v_last_year_trade_days
            ).otherwise(lit(0)).alias("LAST_YEAR_COMPLET_VALUE")
        )
    ).select(
        "c.oa_branch_id",
        "LAST_YEAR_COMPLET_VALUE"
    )

    df_135_y = update_dataframe(
        df_to_update=df_135_y.filter(
            col("INDEX_ASSES_BENCHMARK") == lit("2"),  # 客户资产规模
            col("INDEX_TYPE") == lit("0"),  # 绝对指标
        ),
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=["LAST_YEAR_COMPLET_VALUE"]
    )

    """
    日均客户保证金市占率  取数源：交易统计表——日均权益（全口径），调整后的权益/行业
    """

    tmp = spark.table("ddw.T_COCKPIT_INDUSTRY_MANAGE").alias("t") \
        .filter(
        (col("t.index_name") == lit("客户权益")) &
        (col("t.etl_month").between(v_last_year_begin_month, v_last_year_end_month))
    ).agg(
        sum(
            when(
                v_last_year_trade_days != lit(0),
                col("t.index_value") * 100000000 / v_last_year_trade_days)
        ).otherwise(lit(0)).alias("index_value_now")
    ).select(
        "index_value_now"
    )

    tmp1 = spark.table("ods.CF_STAT_T_CLIENT_SETT_DATA").alias("t") \
        .filter(
        (col("t.busi_date_during").between(v_last_year_begin_month, v_last_year_end_month))
    ).join(
        other=spark.table("edw.h12_fund_account").alias("b"),
        on=(col("t.fund_account_id") == col("b.fund_account_id")),
        how="left"
    ).join(
        other=spark.table("ddw.t_ctp_branch_oa_rela").alias("c"),
        on=(col("b.branch_id") == col("c.CTP_BRANCH_ID")),
        how="inner"
    ).groupBy(
        col("c.OA_BRANCH_ID")
    ).agg(
        sum(
            when(
                col(v_last_year_trade_days) != lit(0),
                col("t.avg_trade_rights") / v_last_year_trade_days)
        ).otherwise(lit(0)).alias("LAST_YEAR_COMPLET_VALUE")
    ).select(
        "c.OA_BRANCH_ID",
        "LAST_YEAR_COMPLET_VALUE"
    )

    # TODO: 别名可能不对或不规范
    df_y = tmp1.alias("t") \
        .crossJoin(tmp.alias("t1")) \
        .select(
        col("t.OA_BRANCH_ID"),
        when(
            coalesce(col("tmp.index_value_now"), lit(0)) != lit(0),
            col("t.LAST_YEAR_COMPLET_VALUE") / col("t1.index_value_now")
        ).otherwise(lit(0)).alias("LAST_YEAR_COMPLET_VALUE")
    )

    df_135_y = update_dataframe(
        df_to_update=df_135_y.filter(
            col("INDEX_ASSES_BENCHMARK") == lit("2"),  # 客户资产规模
            col("INDEX_TYPE") == lit("1"),  # 相对指标
        ),
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=["LAST_YEAR_COMPLET_VALUE"]
    )

    """
    考核利润 绝对指标 考核利润完成情况（万） 取数源：财务内核表：三、营业利润（亏损以“-”号填列）　
    来源表 cf_busimg.t_cockpit_00127
    """

    df_y = spark.table("ddw.t_cockpit_00127").alias("t") \
        .filter(
        col("t.month_id") == lit(v_last_year_end_month)
    ).join(
        other=spark.table("ddw.t_yy_branch_oa_rela").alias("b"),
        on=(col("t.book_id") == col("b.yy_book_id")),
        how="inner"
    ).groupBy(
        col("b.oa_branch_id")
    ).agg(
        sum(col("t.g18")).alias("LAST_YEAR_COMPLET_VALUE")  # 营业收入完成情况（万）
    ).select(
        "b.oa_branch_id",
        "LAST_YEAR_COMPLET_VALUE"
    )

    df_135_y = update_dataframe(
        df_to_update=df_135_y.filter(
            col("INDEX_ASSES_BENCHMARK") == lit("3"),  # 考核利润
            col("INDEX_TYPE") == lit("0"),  # 绝对指标
        ),
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=["LAST_YEAR_COMPLET_VALUE"]
    )

    """
    成交额 绝对指标  客户交易成交金额（双边） 分支机构是双边计算的，直接取CTP的数据，成交额（调整后数据）
    """

    df_y = spark.table("ods.CF_STAT_T_TRADE_SUM_DATA").alias("t") \
        .filter(
        (col("t.busi_date_during").between(v_last_year_begin_month, v_last_year_end_month))
    ).join(
        other=spark.table("edw.h12_fund_account").alias("b"),
        on=(col("t.fund_account_id") == col("b.fund_account_id")),
        how="left"
    ).join(
        other=spark.table("ddw.t_ctp_branch_oa_rela").alias("c"),
        on=(col("b.branch_id") == col("c.CTP_BRANCH_ID")),
        how="inner"
    ).groupBy(
        col("c.OA_BRANCH_ID")
    ).agg(
        sum(col("t.done_money")).alias("LAST_YEAR_COMPLET_VALUE")
    ).select(
        "c.OA_BRANCH_ID",
        "LAST_YEAR_COMPLET_VALUE"
    )

    df_135_y = update_dataframe(
        df_to_update=df_135_y.filter(
            col("INDEX_ASSES_BENCHMARK") == lit("4"),  # 成交额
            col("INDEX_TYPE") == lit("0"),  # 绝对指标
        ),
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=["LAST_YEAR_COMPLET_VALUE"]
    )

    """
    成交额 相对指标  成交额市占率(双边） 分支机构是双边计算的，直接取CTP的数据，成交额（调整后数据）/行业
    """

    tmp = spark.table("ods.CF_STAT_T_TRADE_SUM_DATA").alias("t") \
        .filter(
        (col("t.etl_month").between(v_last_year_begin_month, v_last_year_end_month))
    ).join(
        other=spark.table("edw.h12_fund_account").alias("b"),
        on=(col("t.fund_account_id") == col("b.fund_account_id")),
        how="left"
    ).join(
        other=spark.table("ddw.t_ctp_branch_oa_rela").alias("c"),
        on=(col("b.branch_id") == col("c.CTP_BRANCH_ID")),
        how="inner"
    ).groupBy(
        col("c.OA_BRANCH_ID")
    ).agg(
        sum(col("t.done_money")).alias("LAST_YEAR_COMPLET_VALUE")
    ).select(
        "c.OA_BRANCH_ID",
        "LAST_YEAR_COMPLET_VALUE"
    )

    tmp1 = spark.table("ddw.T_COCKPIT_INDUSTRY_TRAD").alias("t") \
        .filter(
        (col("t.etl_month").between(v_last_year_begin_month, v_last_year_end_month))
    ).agg(
        sum(col("t.TRAD_AMT") * 2 * 100000000).alias("index_value_now")
    ).select(
        "index_value_now"
    )

    df_y = tmp.alias("t") \
        .crossJoin(tmp1.alias("t1")) \
        .select(
        col("t.OA_BRANCH_ID"),
        when(
            coalesce(col("t1.index_value_now"), lit(0)) != lit(0),
            col("t.LAST_YEAR_COMPLET_VALUE") / coalesce(col("t1.index_value_now"), lit(0))
        ).otherwise(lit(0)).alias("LAST_YEAR_COMPLET_VALUE")
    )

    df_135_y = update_dataframe(
        df_to_update=df_135_y.filter(
            col("INDEX_ASSES_BENCHMARK") == lit("4"),  # 成交额
            col("INDEX_TYPE") == lit("1"),  # 相对指标
        ),
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=["LAST_YEAR_COMPLET_VALUE"]
    )

    """
    成交量 绝对指标  客户交易成交量（双边） 分支机构是双边计算的，直接取CTP的数据，成交量（调整后数据）
    """

    df_y = spark.table("ods.CF_STAT_T_TRADE_SUM_DATA").alias("t") \
        .filter(
        (col("t.busi_date_during").between(v_last_year_begin_month, v_last_year_end_month))
    ).join(
        other=spark.table("edw.h12_fund_account").alias("b"),
        on=(col("t.fund_account_id") == col("b.fund_account_id")),
        how="left"
    ).join(
        other=spark.table("ddw.t_ctp_branch_oa_rela").alias("c"),
        on=(col("b.branch_id") == col("c.CTP_BRANCH_ID")),
        how="inner"
    ).groupBy(
        col("c.OA_BRANCH_ID")
    ).agg(
        sum(col("t.done_amount")).alias("LAST_YEAR_COMPLET_VALUE")
    ).select(
        "c.OA_BRANCH_ID",
        "LAST_YEAR_COMPLET_VALUE"
    )

    df_135_y = update_dataframe(
        df_to_update=df_135_y.filter(
            col("INDEX_ASSES_BENCHMARK") == lit("5"),  # 成交量
            col("INDEX_TYPE") == lit("0"),  # 绝对指标
        ),
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=["LAST_YEAR_COMPLET_VALUE"]
    )

    """
    成交量 相对指标  成交量市占率(双边） 分支机构是双边计算的，直接取CTP的数据，成交量（调整后数据）/行业
    """

    tmp = spark.table("ods.CF_STAT_T_TRADE_SUM_DATA").alias("t") \
        .filter(
        (col("t.etl_month").between(v_last_year_begin_month, v_last_year_end_month))
    ).join(
        other=spark.table("edw.h12_fund_account").alias("b"),
        on=(col("t.fund_account_id") == col("b.fund_account_id")),
        how="left"
    ).join(
        other=spark.table("ddw.t_ctp_branch_oa_rela").alias("c"),
        on=(col("b.branch_id") == col("c.CTP_BRANCH_ID")),
        how="inner"
    ).groupBy(
        col("c.OA_BRANCH_ID")
    ).agg(
        sum(col("t.done_amount")).alias("LAST_YEAR_COMPLET_VALUE")
    ).select(
        "c.OA_BRANCH_ID",
        "LAST_YEAR_COMPLET_VALUE"
    )

    tmp1 = spark.table("ddw.T_COCKPIT_INDUSTRY_TRAD").alias("t") \
        .filter(
        (col("t.etl_month").between(v_last_year_begin_month, v_last_year_end_month))
    ).agg(
        sum(col("t.TRAD_NUM") * 2 * 100000000).alias("index_value_now")  # 成交量 -本期
    ).select(
        "index_value_now"
    )

    df_y = tmp.alias("t") \
        .crossJoin(tmp1.alias("t1")) \
        .select(
        col("t.OA_BRANCH_ID"),
        when(
            coalesce(col("t1.index_value_now"), lit(0)) != lit(0),
            col("t.LAST_YEAR_COMPLET_VALUE") / coalesce(col("t1.index_value_now"), lit(0))
        ).otherwise(lit(0)).alias("LAST_YEAR_COMPLET_VALUE")
    )

    df_135_y = update_dataframe(
        df_to_update=df_135_y.filter(
            col("INDEX_ASSES_BENCHMARK") == lit("5"),  # 成交量
            col("INDEX_TYPE") == lit("1"),  # 相对指标
        ),
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=["LAST_YEAR_COMPLET_VALUE"]
    )

    """
    新增有效客户数 绝对指标  新增直接开发有效客户数量

    （1）所选择的月份区间中，新开户的客户数量
    （2）减掉:
        业绩关系查询中，所选的月份区间中，满足以下三个
            a.关系类型为“居间关系”，
            b.关系状态为“有效”，
            c.审批状态为“审批通过”，
        筛选条件得到的客户。
    （3）剩余客户数量，
        在所选择的月份区间，有交易的客户数量
    """

    tmp = spark.table("ods.CTP63_T_DS_CRM_BROKER_INVESTOR_RELA").alias("a") \
        .filter(
        (lit("a.RELA_STS") == lit("A")) &
        (lit("a.APPROVE_STS") == lit("0")) &
        (lit("a.data_pct").isNotNull())
    ).join(
        other=spark.table("ods.CTP63_T_DS_CRM_BROKER").alias("b"),
        on=(col("a.broker_id") == col("b.broker_id")),
        how="inner"
    ).join(
        other=spark.table("ods.CTP63_T_DS_MDP_DEPT00").alias("f"),
        on=(col("b.department_id") == col("f.chdeptcode")),
        how="inner"
    ).join(
        other=spark.table("ods.CTP63_T_DS_DC_INVESTOR").alias("c"),
        on=(col("a.investor_id") == col("c.investor_id")),
        how="inner"
    ).join(
        other=spark.table("edw.h12_fund_account").alias("x"),
        on=(col("c.investor_id") == col("x.fund_account_id")),
        how="left"
    ).join(
        other=spark.table("edw.h11_branch").alias("x1"),
        on=(col("x.branch_id") == col("x1.branch_id")),
        how="left"
    ).join(
        other=spark.table("ddw.t_yy_branch_oa_rela").alias("x2"),
        on=(col("x1.branch_id") == col("x2.ctp_branch_id")),
        how="left"
    ).select(
        col("a.BROKER_ID"),
        col("b.broker_nam").alias("BROKER_NAME"),
        trim(col("a.INVESTOR_ID")).alias("FUND_ACCOUNT_ID"),
        col("c.investor_nam").alias("CLIENT_NAME"),
        regexp_replace(col("a.ST_DT"), '-', '').alias("BEGIN_DATE"),
        regexp_replace(col("a.END_DT"), '-', '').alias("END_DATE"),
        when(col("a.BROKER_RELA_TYP") == lit('301'), '居间关系')
        .when(col("a.BROKER_RELA_TYP") == lit('001'), '开发关系')
        .when(col("a.BROKER_RELA_TYP") == lit('002'), '服务关系')
        .when(col("a.BROKER_RELA_TYP") == lit('003'), '维护关系')
        .otherwise('-').alias("BROKER_RELA_TYPE"),
        col("a.data_pct"),
        when(col("a.RELA_STS") == lit('A'), '有效')
        .when(col("a.RELA_STS") == lit('S'), '停止使用')
        .otherwise('-').alias("RELA_STATUS"),
        col("a.APPROVE_DT"),
        when(col("a.APPROVE_STS") == lit('0'), '审核通过')
        .when(col("a.APPROVE_STS") == lit('1'), '审核不通过')
        .when(col("a.APPROVE_STS") == lit('2'), '等待审核')
        .otherwise('-').alias("APPROVE_STS"),
        col("a.comment_desc"),
        col("a.check_comments"),
        when(
            regexp_replace(col("a.ST_DT"), '-', '') < lit(v_last_year_begin_date),
            lit(v_last_year_begin_date))
        .when(
            regexp_replace(col("a.ST_DT"), '-', '') >= lit(v_last_year_begin_date),
            regexp_replace(col("a.ST_DT"), '-', ''))
        .otherwise('').alias("REAL_BEGIN_DATE"),
        when(
            regexp_replace(col("a.END_DT"), '-', '') <= lit(v_last_year_end_date),
            regexp_replace(col("a.END_DT"), '-', ''))
        .when(
            regexp_replace(col("a.END_DT"), '-', '') > lit(v_last_year_end_date),
            lit(v_last_year_end_date))
        .otherwise('').alias("REAL_END_DATE")
    )

    df_135_1 = tmp.alias("t") \
        .filter(
        col("t.REAL_BEGIN_DATE") <= col("t.REAL_END_DATE")
    ).groupBy(
        col("t.fund_account_id"),
        col("t.BROKER_RELA_TYPE"),
    ).select(
        "t.fund_account_id",
        "t.BROKER_RELA_TYPE"
    )

    logging.info("df_135_1[新增有效客户数],共有数据%s条", df_135_1.count())

    # 新开客户，且排除居间关系的客户
    tmp = spark.table("edw.h12_fund_account").alias("t") \
        .filter(
        col("t.open_date").between(v_last_year_begin_date, v_last_year_end_date)
    ).join(
        other=df_135_1.alias("a"),
        on=(
                col("t.fund_account_id") == col("a.fund_account_id") &
                col("a.BROKER_RELA_TYPE") != lit("居间关系")
        ),
        how="left_anti"
    ).select(
        "t.fund_account_id"
    )

    tmp_1 = spark.table("ods.CF_STAT_T_TRADE_SUM_DATA").alias("t") \
        .filter(
        col("t.busi_date_during").between(v_last_year_begin_month, v_last_year_end_month),
        col("t.TOTAL_TRANSFEE") != lit(0)
    ).join(
        other=tmp.alias("a"),
        on=(col("t.fund_account_id") == col("a.fund_account_id")),
        how="inner"
    ).select(
        "t.fund_account_id"
    )

    df_y = tmp_1.alias("t") \
        .join(
        other=spark.table("edw.h12_fund_account").alias("b"),
        on=(col("t.fund_account_id") == col("b.fund_account_id")),
        how="left"
    ).join(
        other=spark.table("ddw.t_yy_branch_oa_rela").alias("c"),
        on=(col("b.branch_id") == col("c.CTP_BRANCH_ID")),
        how="inner"
    ).groupBy(
        col("c.oa_branch_id")
    ).agg(
        count("*").alias("LAST_YEAR_COMPLET_VALUE")
    ).select(
        "c.oa_branch_id",
        "LAST_YEAR_COMPLET_VALUE"
    )

    df_135_y = update_dataframe(
        df_to_update=df_135_y.filter(
            col("INDEX_ASSES_BENCHMARK") == lit("6"),  # 新增有效客户数
            col("INDEX_TYPE") == lit("0"),  # 绝对指标
            col("INDEX_NAME").like("%新增直接开发有效客户数量%")
        ),
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=["LAST_YEAR_COMPLET_VALUE"]
    )

    """
    新增有效客户数  绝对指标 新增有效客户数量（户）
    （1）所选择的月份区间中，新开户的客户数量
    （2）减掉:
    业绩关系查询中，所选的月份区间中，满足以下两个
        a.关系状态为"有效"，
        b.审批状态为"审批通过“，
    筛选条件得到的客户。
    （3）剩余客户数量，
    在所选择的月份区间，有交易的客户数量
    """

    # 新开客户，不排除居间关系的客户
    tmp = spark.table("edw.h12_fund_account").alias("t") \
        .filter(
        col("t.open_date").between(v_last_year_begin_date, v_last_year_end_date)
    ).join(
        other=df_135_1.alias("a"),
        on=(
                col("t.fund_account_id") == col("a.fund_account_id")
        ),
        how="left_anti"
    ).select(
        "t.fund_account_id"
    )

    tmp_1 = spark.table("ods.CF_STAT_T_TRADE_SUM_DATA").alias("t") \
        .filter(
        col("t.busi_date_during").between(v_last_year_begin_month, v_last_year_end_month),
        col("t.TOTAL_TRANSFEE") != lit(0)
    ).join(
        other=tmp.alias("a"),
        on=(col("t.fund_account_id") == col("a.fund_account_id")),
        how="inner"
    ).select(
        "t.fund_account_id"
    )

    df_y = tmp_1.alias("t") \
        .join(
        other=spark.table("edw.h12_fund_account").alias("b"),
        on=(col("t.fund_account_id") == col("b.fund_account_id")),
        how="left"
    ).join(
        other=spark.table("ddw.t_yy_branch_oa_rela").alias("c"),
        on=(col("b.branch_id") == col("c.CTP_BRANCH_ID")),
        how="inner"
    ).groupBy(
        col("c.oa_branch_id")
    ).agg(
        count("*").alias("LAST_YEAR_COMPLET_VALUE")
    ).select(
        "c.oa_branch_id",
        "LAST_YEAR_COMPLET_VALUE"
    )

    df_135_y = update_dataframe(
        df_to_update=df_135_y.filter(
            col("INDEX_ASSES_BENCHMARK") == lit("6"),  # 新增有效客户数
            col("INDEX_TYPE") == lit("0"),  # 绝对指标
            col("INDEX_NAME").like("%新增有效客户数量%")
        ),
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=["LAST_YEAR_COMPLET_VALUE"]
    )

    """
    产品销售（万） 绝对指标  新增公司统一组织的产品销售额  （资管表3月度销售人员保有奖励分配情况—新增量）
    """

    df_y = spark.table("ddw.T_COCKPIT_00096").alias("a") \
        .filter(
        col("a.busi_date").between(v_last_year_begin_date, v_last_year_end_date)
    ).join(
        other=spark.table("h12_fund_account").alias("b"),
        on=(
            col("a.id_no") == col("b.id_no"),
            col("a.client_name") == col("b.client_name")
        ),
        how="inner"
    ).join(
        other=spark.table("ddw.t_yy_branch_oa_rela").alias("c"),
        on=(col("b.branch_id") == col("c.CTP_BRANCH_ID")),
        how="inner"
    ).groupBy(
        col("c.oa_branch_id")
    ).agg(
        sum(
            when(
                col("a.wh_trade_type").isin("0", "1"),
                col("a.confirm_share")
            ).otherwise(lit(0))
        ).alias("LAST_YEAR_COMPLET_VALUE")  # 新增量
    ).select(
        "c.oa_branch_id",
        "LAST_YEAR_COMPLET_VALUE"
    )

    df_135_y = update_dataframe(
        df_to_update=df_135_y.filter(
            col("INDEX_ASSES_BENCHMARK") == lit("7"),  # 产品销售
            col("INDEX_TYPE") == lit("0"),  # 绝对指标
        ),
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=["LAST_YEAR_COMPLET_VALUE"]
    )

    """
    更新增长率
    """

    df_135_y = df_135_y.withColumn(
        "growth_rate",
        when(
            col("last_year_complet_value") != lit(0),
            col("year_target_value") / col("last_year_complet_value") - 1
        ).otherwise(lit(0))
    )

    """
    更新占比
    """

    str_nums = ["1", "2", "3", "4"]
    for i in str_nums:
        df_135_y = df_135_y.alias("a") \
            .withColumn(
            "a.QUARTER_TARGET_PROP_:s".format(i),
            when(
                col("a.year_target_value") != lit(0),
                col("a.QUARTER_TARGET_PROP_:s".format(i)) / col("a.year_target_value")
            ).otherwise(lit(0))
        )

    """
    更新相对指标的季度目标
    1 营业收入
    2 客户资产规模
    4 成交额
    5 成交量
    """
    # TODO: 有1,2,4,5共4项,尽量用循环

    str_nums = ["1", "2", "4", "5"]

    for i in str_nums:
        tmp = df_135_y.alias("t") \
            .filter(
            col("t.INDEX_ASSES_BENCHMARK") == lit(i),
            col("t.INDEX_TYPE") == lit("0"),
        ).select(
            "t.oa_branch_id",
            when(
                col("t.year_target_value") != lit(0),
                col("t.QUARTER_TARGET_PROP_1") / col("t.year_target_value")
            ).otherwise(lit(0)).alias("QUARTER_TARGET_PROP_1"),
            when(
                col("t.year_target_value") != lit(0),
                col("t.QUARTER_TARGET_PROP_2") / col("t.year_target_value")
            ).otherwise(lit(0)).alias("QUARTER_TARGET_PROP_2"),
            when(
                col("t.year_target_value") != lit(0),
                col("t.QUARTER_TARGET_PROP_3") / col("t.year_target_value")
            ).otherwise(lit(0)).alias("QUARTER_TARGET_PROP_3"),
            when(
                col("t.year_target_value") != lit(0),
                col("t.QUARTER_TARGET_PROP_4") / col("t.year_target_value")
            ).otherwise(lit(0)).alias("QUARTER_TARGET_PROP_4"),
        )

        tmp1 = df_135_y.alias("t") \
            .filter(
            col("t.INDEX_ASSES_BENCHMARK") == lit(i),
            col("t.INDEX_TYPE") == lit("1"),
        ).select(
            "t.oa_branch_id",
            "t.YEAR_TARGET_VALUE",
        )

        df_y = tmp1.alias("t") \
            .join(
            other=tmp.alias("a"),
            on=(col("t.oa_branch_id") == col("a.oa_branch_id")),
            how="inner"
        ).select(
            (col("t.YEAR_TARGET_VALUE") * coalesce(col("a.QUARTER_TARGET_1"), lit(0)) * 4).alias("QUARTER_TARGET_1"),
            (col("t.YEAR_TARGET_VALUE") * coalesce(col("a.QUARTER_TARGET_2"), lit(0)) * 4).alias("QUARTER_TARGET_2"),
            (col("t.YEAR_TARGET_VALUE") * coalesce(col("a.QUARTER_TARGET_3"), lit(0)) * 4).alias("QUARTER_TARGET_3"),
            (col("t.YEAR_TARGET_VALUE") * coalesce(col("a.QUARTER_TARGET_4"), lit(0)) * 4).alias("QUARTER_TARGET_4"),
        )

        df_135_y = update_dataframe(
            df_to_update=df_135_y.filter(
                col("INDEX_ASSES_BENCHMARK") == lit(i),
                col("INDEX_TYPE") == lit("1"),
            ),
            df_use_me=df_y,
            join_columns=["oa_branch_id"],
            update_columns=["QUARTER_TARGET_1", "QUARTER_TARGET_2", "QUARTER_TARGET_3", "QUARTER_TARGET_4"]
        )

    """
    更新校验结果 --绝对指标 + 相对指标
    """

    df_135_y = df_135_y.alias("a") \
        .withColumn(
        "check_result",
        when(
            col("a.index_type") == lit("0") &
            col("a.TARGET_GROWTH_RATE") == col("a.growth_rate") &
            col("a.QUARTER_TARGET_RATE_1") == col("a.QUARTER_TARGET_PROP_1") &
            col("a.QUARTER_TARGET_RATE_2") == col("a.QUARTER_TARGET_PROP_2") &
            col("a.QUARTER_TARGET_RATE_3") == col("a.QUARTER_TARGET_PROP_3") &
            col("a.QUARTER_TARGET_RATE_4") == col("a.QUARTER_TARGET_PROP_4"),
            lit(1)
        ).when(
            col("a.index_type") == lit("1") &
            col("a.QUARTER_TARGET_1") +
            col("a.QUARTER_TARGET_2") +
            col("a.QUARTER_TARGET_3") +
            col("a.QUARTER_TARGET_4") == col("a.YEAR_TARGET_VALUE"),
            lit(1)
        ).otherwise(lit(0))
    )
