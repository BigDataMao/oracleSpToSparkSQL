# -*- coding: utf-8 -*-
"""
经营目标完成情况-数据落地
逻辑来源: utils/P_COCKPIT_00137_DATA.sql
"""
import logging

from pyspark.sql.functions import col, lit, sum, when, coalesce, trim, regexp_replace, expr

from utils.date_utils import get_date_period_and_days
from utils.task_env import return_to_hive, update_dataframe, log


@log
def p_cockpit_00137_data(spark, busi_date):
    """
    经营目标完成情况-数据落地
    """
    i_month_id = busi_date[:6]
    v_year_id = busi_date[:4]
    (
        v_begin_trade_date,
        v_end_trade_date,
        v_trade_days
    ) = get_date_period_and_days(spark, busi_month=i_month_id, is_trade_day=True)

    (
        v_begin_date,
        v_end_date,
    ) = get_date_period_and_days(spark, busi_month=i_month_id, is_trade_day=False)


    """
    初始化数据:年度目标维护表
    """

    df_137_begin = spark.table("ddw.T_COCKPIT_00135").alias("t") \
        .filter(
        (col("t.year_id") == v_year_id) &
        (col("t.check_result") == "1") &
        (col("t.index_status") == "1")
    ).join(
        other=spark.table("ddw.T_COCKPIT_00137").alias("a"),
        on=(
                col("t.year_id") == col("a.year_id") &
                col("t.oa_branch_id") == col("a.oa_branch_id") &
                col("t.index_id") == col("a.index_id")
        ),
        how="left_anti"
    ).select(
        col("t.year_id"),
        lit(i_month_id).alias("busi_month"),  # 人为添加月份分区
        col("t.oa_branch_id"),
        col("t.oa_branch_name"),
        col("t.index_id"),
        col("t.index_asses_benchmark"),
        col("t.index_type"),
        col("t.index_name"),
    )

    return_to_hive(
        spark=spark,
        df_result=df_137_begin,
        target_table="ddw.T_COCKPIT_00137",
        insert_mode="overwrite",
        partition_column=["year_id", "busi_month"],
        partition_value=[v_year_id, i_month_id]
    )

    df_137_ym = spark.table("ddw.T_COCKPIT_00137").filter(
        col("year_id") == v_year_id,
        col("busi_month") == i_month_id
    )

    """
    更新数据  分支机构当月值
    营业收入完成情况（万）  取数源：财务内核表——营业收入，调整后的收入
    来源表 cf_busimg.t_cockpit_00127
    """

    df_y = spark.table("ddw.T_COCKPIT_00127").alias("t") \
        .filter(
        (col("t.month_id") == i_month_id)
    ).join(
        other=spark.table("ddw.t_yy_branch_oa_rela").alias("b"),
        on=(col("t.book_id") == col("b.yy_book_id")),
        how="inner",
    ).groupBy(
        col("b.oa_branch_id")
    ).agg(
        sum("t.b5").alias("INDEX_VALUE_BRANCH")
    ).select(
        col("b.oa_branch_id"),
        col("INDEX_VALUE_BRANCH")
    )

    df_137_ym = update_dataframe(
        df_to_update=df_137_ym,
        filter_condition=col("INDEX_ASSES_BENCHMARK") == "1",
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=["INDEX_VALUE_BRANCH"]
    )

    """
    经纪业务手续费收入市占率（万分之）
    经纪业务手续费收入市占率 取数源：财务内核表：手续费及佣金净收入+交易所减免返还/行业手续费收入
    """

    tmp = spark.table("ddw.T_COCKPIT_INDUSTRY_MANAGE").alias("t") \
        .filter(
        (col("t.etl_month") == i_month_id)
    ).agg(
        sum(
            col("t.index_value") * 100000000
        ).alias("index_value_now")  # 经纪业务手续费收入 -本期
    ).select(
        col("index_value_now")
    )

    tmp1 = spark.table("ddw.t_cockpit_00127").alias("t") \
        .filter(
        (col("t.month_id") == i_month_id)
    ).join(
        other=spark.table("ddw.t_yy_branch_oa_rela").alias("b"),
        on=(col("t.book_id") == col("b.yy_book_id")),
        how="inner",
    ).groupBy(
        col("b.oa_branch_id")
    ).agg(
        sum(
            col("t.b6") + col("t.b8")
        ).alias("INDEX_VALUE_BRANCH")  # 经纪业务手续费收入市占率
    ).select(
        col("b.oa_branch_id"),
        col("INDEX_VALUE_BRANCH")
    )

    df_y = tmp.alias("t").crossJoin(tmp1.alias("t1")) \
        .select(
        col("t1.oa_branch_id"),
        when(
            coalesce(col("t.index_value_now"), lit(0)) == lit(0),
            coalesce(col("t1.INDEX_VALUE_BRANCH"), lit(0)) / coalesce(col("t.index_value_now"), lit(0))
        ).otherwise(
            lit(0)
        ).alias("INDEX_VALUE_BRANCH"),
        col("t.index_value_now").alias("INDEX_VALUE_INDUST")
    )

    df_137_ym = update_dataframe(
        df_to_update=df_137_ym,
        filter_condition=(
            (col("INDEX_ASSES_BENCHMARK") == lit("1")) &
            (col("INDEX_TYPE") == lit("1"))
        ),
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=["INDEX_VALUE_BRANCH", "INDEX_VALUE_INDUST"]
    )

    """
    客户资产规模 日均客户保证金完成情况 取数源：交易统计表——日均权益（考核口径），调整后的权益。
    后续修改调整
    """

    df_y = spark.table("ddw.T_CLIENT_SETT_DATA").alias("t") \
        .filter(
        (col("t.busi_date_during") == i_month_id)
    ).join(
        other=spark.table("edw.h12_fund_account").alias("b"),
        on=(col("t.fund_account_id") == col("b.fund_account_id")),
        how="left"
    ).join(
        other=spark.table("ddw.t_ctp_branch_oa_rela").alias("c"),
        on=(col("b.ctp_branch_id") == col("c.ctp_branch_id")),
        how="inner"
    ).groupBy(
        col("c.oa_branch_id")
    ).agg(
        sum(
            when(
                v_trade_days > 0,
                col("t.avg_trade_rights") / v_trade_days
            ).otherwise(
                lit(0)
            )
        ).alias("INDEX_VALUE_BRANCH")
    ).select(
        col("c.oa_branch_id"),
        col("INDEX_VALUE_BRANCH")
    )

    df_137_ym = update_dataframe(
        df_to_update=df_137_ym,
        filter_condition=(
            (col("INDEX_ASSES_BENCHMARK") == lit("2")) &
            (col("INDEX_TYPE") == lit("0"))
        ),
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=["INDEX_VALUE_BRANCH"]
    )

    """
    日均客户保证金市占率  取数源：交易统计表——日均权益（全口径），调整后的权益/行业
    """

    tmp = spark.table("ddw.T_COCKPIT_INDUSTRY_MANAGE").alias("t") \
        .filter(
        (col("t.index_name") == lit("客户权益")) &
        (col("t.etl_month") == i_month_id)
    ).agg(
        sum(
            when(
                v_trade_days != 0,
                col("t.avg_trade_rights") / v_trade_days
            ).otherwise(lit(0))
        ).alias("INDEX_VALUE_NOW")
    ).select(
        col("INDEX_VALUE_NOW")
    )

    tmp1 = spark.table("ddw.T_CLIENT_SETT_DATA").alias("t") \
        .filter(
        (col("t.busi_date_during") == i_month_id)
    ).join(
        other=spark.table("edw.h12_fund_account").alias("b"),
        on=(col("t.fund_account_id") == col("b.fund_account_id")),
        how="left"
    ).join(
        other=spark.table("ddw.t_ctp_branch_oa_rela").alias("c"),
        on=(col("b.ctp_branch_id") == col("c.ctp_branch_id")),
        how="inner"
    ).groupBy(
        col("c.oa_branch_id")
    ).agg(
        sum(
            when(
                v_trade_days != 0,
                col("t.avg_trade_rights") / v_trade_days
            ).otherwise(
                lit(0)
            )
        ).alias("INDEX_VALUE_BRANCH")
    ).select(
        col("c.oa_branch_id"),
        col("INDEX_VALUE_BRANCH")
    )

    df_y = tmp1.alias("t").crossJoin(tmp.alias("t1")) \
        .select(
        col("t.oa_branch_id"),
        when(
            coalesce(col("t1.INDEX_VALUE_NOW"), lit(0)) != lit(0),
            col("t.INDEX_VALUE_BRANCH") / col("t1.INDEX_VALUE_NOW")
        ).otherwise(
            lit(0)
        ).alias("INDEX_VALUE_BRANCH"),
        col("t.INDEX_VALUE_NOW").alias("INDEX_VALUE_INDUST")
    )

    df_137_ym = update_dataframe(
        df_to_update=df_137_ym,
        filter_condition=(
            (col("INDEX_ASSES_BENCHMARK") == lit("2")) &
            (col("INDEX_TYPE") == lit("1"))
        ),
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=["INDEX_VALUE_BRANCH"]
    )

    """
    考核利润 绝对指标 考核利润完成情况（万） 取数源：财务内核表：三、营业利润（亏损以“-”号填列）　
    来源表 cf_busimg.t_cockpit_00127
    """

    df_y = spark.table("ddw.T_COCKPIT_00127").alias("t") \
        .filter(
        (col("t.month_id") == i_month_id)
    ).join(
        other=spark.table("ddw.t_yy_branch_oa_rela").alias("b"),
        on=(col("t.book_id") == col("b.yy_book_id")),
        how="inner",
    ).groupBy(
        col("b.oa_branch_id")
    ).agg(
        sum("t.b18").alias("INDEX_VALUE_BRANCH")
    ).select(
        col("b.oa_branch_id"),
        col("INDEX_VALUE_BRANCH")
    )

    df_137_ym = update_dataframe(
        df_to_update=df_137_ym,
        filter_condition=(
            (col("INDEX_ASSES_BENCHMARK") == lit("3")) &
            (col("INDEX_TYPE") == lit("0"))
        ),
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=["INDEX_VALUE_BRANCH"]
    )

    """
    成交额 绝对指标  客户交易成交金额（双边） 分支机构是双边计算的，直接取CTP的数据，成交额（调整后数据）
    """

    tmp = spark.table("ddw.T_TRADE_SUM_DATA").alias("t") \
        .filter(
        (col("t.busi_date_during") == i_month_id)
    ).join(
        other=spark.table("ddw.h12_fund_account").alias("b"),
        on=(col("t.fund_account_id") == col("b.fund_account_id")),
        how="left"
    ).join(
        other=spark.table("ddw.t_ctp_branch_oa_rela").alias("c"),
        on=(col("b.branch_id") == col("c.ctp_branch_id")),
        how="inner"
    ).groupBy(
        col("c.oa_branch_id")
    ).agg(
        sum("t.done_money").alias("INDEX_VALUE_BRANCH")
    ).select(
        col("c.oa_branch_id"),
        col("INDEX_VALUE_BRANCH")
    )

    tmp1 = spark.table("ddw.T_COCKPIT_INDUSTRY_TRAD").alias("t") \
        .filter(
        (col("t.etl_month") == i_month_id)
    ).agg(
        sum("t.TRAD_AMT").alias("INDEX_VALUE_NOW")
    ).select(
        col("INDEX_VALUE_NOW")
    )

    df_y = tmp.alias("t").crossJoin(tmp1.alias("t1")) \
        .select(
        col("t.oa_branch_id"),
        col("t.INDEX_VALUE_BRANCH"),
        coalesce(col("t1.INDEX_VALUE_NOW"), lit(0)).alias("INDEX_VALUE_INDUST")
    )

    df_137_ym = update_dataframe(
        df_to_update=df_137_ym,
        filter_condition=(
            (col("INDEX_ASSES_BENCHMARK") == lit("4")) &
            (col("INDEX_TYPE") == lit("0"))
        ),
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=["INDEX_VALUE_BRANCH", "INDEX_VALUE_INDUST"]
    )

    """
    成交额 相对指标  成交额市占率(双边） 分支机构是双边计算的，直接取CTP的数据，成交额（调整后数据）/行业
    """

    tmp = spark.table("ddw.T_TRADE_SUM_DATA").alias("t") \
        .filter(
        (col("t.busi_date_during") == i_month_id)
    ).join(
        other=spark.table("ddw.h12_fund_account").alias("b"),
        on=(col("t.fund_account_id") == col("b.fund_account_id")),
        how="left"
    ).join(
        other=spark.table("ddw.t_ctp_branch_oa_rela").alias("c"),
        on=(col("b.branch_id") == col("c.ctp_branch_id")),
        how="inner"
    ).groupBy(
        col("c.oa_branch_id")
    ).agg(
        sum("t.done_money").alias("INDEX_VALUE_BRANCH")
    ).select(
        col("c.oa_branch_id"),
        col("INDEX_VALUE_BRANCH")
    )

    tmp1 = spark.table("ddw.T_COCKPIT_INDUSTRY_TRAD").alias("t") \
        .filter(
        (col("t.etl_month") == i_month_id)
    ).agg(
        sum(
            col("t.TRAD_AMT") * 2 * 100000000
        ).alias("INDEX_VALUE_NOW")
    ).select(
        col("INDEX_VALUE_NOW")
    )

    df_y = tmp.alias("t").crossJoin(tmp1.alias("t1")) \
        .select(
        col("t.oa_branch_id"),
        when(
            coalesce(col("t1.INDEX_VALUE_NOW"), lit(0)) != lit(0),
            col("t.INDEX_VALUE_BRANCH") / col("t1.INDEX_VALUE_NOW")
        ).otherwise(
            lit(0)
        ).alias("INDEX_VALUE_BRANCH")
    )

    df_137_ym = update_dataframe(
        df_to_update=df_137_ym,
        filter_condition=(
            (col("INDEX_ASSES_BENCHMARK") == lit("4")) &
            (col("INDEX_TYPE") == lit("1"))
        ),
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=["INDEX_VALUE_BRANCH"]
    )

    """
    成交量 绝对指标  客户交易成交量（双边） 分支机构是双边计算的，直接取CTP的数据，成交量（调整后数据）
    """

    tmp = spark.table("ddw.T_TRADE_SUM_DATA").alias("t") \
        .filter(
        (col("t.busi_date_during") == i_month_id)
    ).join(
        other=spark.table("ddw.h12_fund_account").alias("b"),
        on=(col("t.fund_account_id") == col("b.fund_account_id")),
        how="left"
    ).join(
        other=spark.table("ddw.t_ctp_branch_oa_rela").alias("c"),
        on=(col("b.branch_id") == col("c.ctp_branch_id")),
        how="inner"
    ).groupBy(
        col("c.oa_branch_id")
    ).agg(
        sum("t.done_amount").alias("INDEX_VALUE_BRANCH")
    ).select(
        col("c.oa_branch_id"),
        col("INDEX_VALUE_BRANCH")
    )

    tmp1 = spark.table("ddw.T_COCKPIT_INDUSTRY_TRAD").alias("t") \
        .filter(
        (col("t.etl_month") == i_month_id)
    ).agg(
        sum(
            col("t.TRAD_NUM") * 2 * 100000000
        ).alias("INDEX_VALUE_NOW")
    ).select(
        col("INDEX_VALUE_NOW")
    )

    df_y = tmp.alias("t").crossJoin(tmp1.alias("t1")) \
        .select(
        col("t.oa_branch_id"),
        col("t.INDEX_VALUE_BRANCH"),
        coalesce(col("t1.INDEX_VALUE_NOW"), lit(0)).alias("INDEX_VALUE_INDUST")
    )

    df_137_ym = update_dataframe(
        df_to_update=df_137_ym,
        filter_condition=(
            (col("INDEX_ASSES_BENCHMARK") == lit("5")) &
            (col("INDEX_TYPE") == lit("0"))
        ),
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=["INDEX_VALUE_BRANCH", "INDEX_VALUE_INDUST"]
    )

    """
    成交量 相对指标  成交量市占率(双边） 分支机构是双边计算的，直接取CTP的数据，成交量（调整后数据）/行业
    """

    tmp = spark.table("ddw.T_TRADE_SUM_DATA").alias("t") \
        .filter(
        (col("t.busi_date_during") == i_month_id)
    ).join(
        other=spark.table("ddw.h12_fund_account").alias("b"),
        on=(col("t.fund_account_id") == col("b.fund_account_id")),
        how="left"
    ).join(
        other=spark.table("ddw.t_ctp_branch_oa_rela").alias("c"),
        on=(col("b.branch_id") == col("c.ctp_branch_id")),
        how="inner"
    ).groupBy(
        col("c.oa_branch_id")
    ).agg(
        sum("t.done_amount").alias("INDEX_VALUE_BRANCH")
    ).select(
        col("c.oa_branch_id"),
        col("INDEX_VALUE_BRANCH")
    )

    tmp1 = spark.table("ddw.T_COCKPIT_INDUSTRY_TRAD").alias("t") \
        .filter(
        (col("t.etl_month") == i_month_id)
    ).agg(
        sum(
            col("t.TRAD_NUM") * 2 * 100000000
        ).alias("INDEX_VALUE_NOW")
    ).select(
        col("INDEX_VALUE_NOW")
    )

    df_y = tmp.alias("t").crossJoin(tmp1.alias("t1")) \
        .select(
        col("t.oa_branch_id"),
        when(
            coalesce(col("t1.INDEX_VALUE_NOW"), lit(0)) != lit(0),
            col("t.INDEX_VALUE_BRANCH") / col("t1.INDEX_VALUE_NOW")
        ).otherwise(
            lit(0)
        ).alias("INDEX_VALUE_BRANCH")
    )

    df_137_ym = update_dataframe(
        df_to_update=df_137_ym,
        filter_condition=(
            (col("INDEX_ASSES_BENCHMARK") == lit("5")) &
            (col("INDEX_TYPE") == lit("1"))
        ),
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=["INDEX_VALUE_BRANCH"]
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

    df_tmp = spark.table("ods.T_DS_CRM_BROKER_INVESTOR_RELA").alias("a") \
        .filter(
        (col("a.RELA_STS") == lit("A")) &
        (col("a.APPROVE_STS") == lit("0")) &
        (col("a.data_pct").isNotNull())
    ).join(
        other=spark.table("ods.T_DS_CRM_BROKER").alias("b"),
        on=(col("a.broker_id") == col("b.broker_id")),
        how="inner"
    ).join(
        other=spark.table("ods.T_DS_MDP_DEPT00").alias("f"),
        on=(col("b.department_id") == col("f.chdeptcode")),
        how="inner"
    ).join(
        other=spark.table("ods.T_DS_DC_INVESTOR").alias("c"),
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
        other=spark.table("ddw.t_ctp_branch_oa_rela").alias("x2"),
        on=(col("x1.branch_id") == col("x2.ctp_branch_id")),
        how="left"
    ).select(
        col("a.broker_id"),
        col("b.broker_nam").alias("broker_name"),
        trim(col("a.investor_id")).alias("fund_account_id"),
        col("c.investor_nam").alias("client_name"),
        regexp_replace(col("a.ST_DT"), "-", "").alias("begin_date"),
        regexp_replace(col("a.END_DT"), "-", "").alias("end_date"),
        when(
            col("a.broker_rela_typ") == lit("301"),
            lit("居间关系")
            ).when(
                col("a.broker_rela_typ") == lit("001"),
                lit("开发关系")
            ).when(
                col("a.broker_rela_typ") == lit("002"),
                lit("服务关系")
            ).when(
                col("a.broker_rela_typ") == lit("003"),
                lit("维护关系")
            ).otherwise(
                lit("-")
            ).alias("BROKER_RELA_TYPE"),
        col("a.data_pct"),
        when(
            col("a.RELA_STS") == lit("A"),
            lit("有效")
            ).when(
                col("a.RELA_STS") == lit("S"),
                lit("停止使用")
            ).otherwise(
                lit("-")
            ).alias("RELA_STATUS"),
        col("a.APPROVE_DT"),
        when(
            col("a.APPROVE_STS") == lit("0"),
            lit("审批通过")
            ).when(
                col("a.APPROVE_STS") == lit("1"),
                lit("审批不通过")
            ).when(
                col("a.APPROVE_STS") == lit("2"),
                lit("等待审核")
            ).otherwise(
            lit("-")
            ).alias("APPROVE_STS"),
        col("a.comment_desc"),
        col("a.check_comments"),
        when(
            (regexp_replace(col("a.ST_DT"), "-", "") < v_begin_date),
            lit(v_begin_date)
            ).when(
            (regexp_replace(col("a.ST_DT"), "-", "") >= v_begin_date),
            regexp_replace(col("a.ST_DT"), "-", "")
            ).otherwise(
            lit("")
            ).alias("REAL_BEGIN_DATE"),
        when(
            (regexp_replace(col("a.END_DT"), "-", "") <= v_end_date),
            regexp_replace(col("a.END_DT"), "-", "")
            ).when(
            (regexp_replace(col("a.END_DT"), "-", "") > v_end_date),
            lit(v_end_date)
            ).otherwise(
            lit("")
            ).alias("REAL_END_DATE")
    )

    df_137_1 = df_tmp.alias("t") \
        .filter(
        col("t.REAL_BEGIN_DATE") <= col("t.REAL_END_DATE")
    ).groupBy(
        col("t.fund_account_id"),
        col("t.BROKER_RELA_TYPE")
    ).select(
        col("t.fund_account_id"),
        col("t.BROKER_RELA_TYPE")
    )

    # 1. 新增直接开发有效客户数量
    # 2. 新增有效客户数量
    # 两者一起处理
    condition = {
        "a": {
            "relation_type": (col("a.broker_rela_type") == lit("居间关系")),
            "index_name": "%新增直接开发有效客户数量%",
        },
        "b": {
            "relation_type": True,
            "index_name": "%新增有效客户数量%",
        }
    }

    for key in condition:
        tmp = spark.table("edw.h12_fund_account").alias("t") \
            .filter(
            (col("t.open_date").between(v_begin_date, v_end_date))
        ).join(
            df_137_1.alias("a"),
            on=(
                col("t.fund_account_id") == col("a.fund_account_id") &
                expr(condition[key]["relation_type"])
            ),
            how="left_anti"
        ).select(
            col("t.fund_account_id")
        )

        tmp1 = spark.table("ddw.T_TRADE_SUM_DATA").alias("t") \
            .filter(
            (col("t.busi_date_during") == i_month_id) &
            (col("t.TOTAL_TRANSFEE") != 0)
        ).join(
            tmp.alias("a"),
            on=(col("t.fund_account_id") == col("a.fund_account_id")),
            how="inner"
        ).groupBy(
            col("t.fund_account_id")
        ).select(
            col("t.fund_account_id")
        )

        df_y = tmp1.alias("t") \
            .join(
            other=spark.table("edw.h12_fund_account").alias("b"),
            on=(col("t.fund_account_id") == col("b.fund_account_id")),
            how="left"
        ).join(
            other=spark.table("ddw.t_ctp_branch_oa_rela").alias("c"),
            on=(col("b.branch_id") == col("c.ctp_branch_id")),
            how="inner"
        ).groupBy(
            col("c.oa_branch_id")
        ).agg(
            sum(lit(1)).alias("INDEX_VALUE_BRANCH")
        ).select(
            col("c.oa_branch_id"),
            col("INDEX_VALUE_BRANCH")
        )

        df_137_ym = update_dataframe(
            df_to_update=df_137_ym,
            filter_condition=(
                (col("INDEX_ASSES_BENCHMARK") == lit("6")) &
                (col("INDEX_TYPE") == lit("0")) &
                (col("INDEX_NAME").like(condition[key]["index_name"]))
            ),
            df_use_me=df_y,
            join_columns=["oa_branch_id"],
            update_columns=["INDEX_VALUE_BRANCH"]
        )

    """
    产品销售（万） 绝对指标  新增公司统一组织的产品销售额  （资管表3月度销售人员保有奖励分配情况—新增量）
    """

    df_y = spark.table("ddw.T_COCKPIT_00096").alias("a") \
        .filter(
        (col("a.busi_date").between(v_begin_date, v_end_date))
    ).join(
        other=spark.table("edw.h12_fund_account").alias("b"),
        on=(
            col("a.id_no") == col("b.id_no") &
            col("a.client_name") == col("b.client_name")
        ),
        how="inner"
    ).join(
        other=spark.table("ddw.t_ctp_branch_oa_rela").alias("c"),
        on=(col("b.branch_id") == col("c.ctp_branch_id")),
        how="inner"
    ).groupBy(
        col("c.oa_branch_id")
    ).agg(
        sum(
            when(
                col("a.wh_trade_type").isin(["0", "1"]),
                col("a.confirm_share")
            ).otherwise(
                lit(0)
            )
        ).alias("INDEX_VALUE_BRANCH")
    ).select(
        col("c.oa_branch_id"),
        col("INDEX_VALUE_BRANCH")
    )

    df_137_ym = update_dataframe(
        df_to_update=df_137_ym,
        filter_condition=(
            (col("INDEX_ASSES_BENCHMARK") == lit("7")) &
            (col("INDEX_TYPE") == lit("0"))
        ),
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=["INDEX_VALUE_BRANCH"]
    )

    """
    写回分区
    """

    return_to_hive(
        spark=spark,
        df_result=df_137_ym,
        target_table="ddw.T_COCKPIT_00137",
        insert_mode="overwrite",
        partition_column=["year_id", "busi_month"],
        partition_value=[v_year_id, i_month_id]
    )
