# -*- coding: utf-8 -*-
from pyspark.sql.functions import col, lit, sum, when, substring, regexp_replace, round

from utils.date_utils import *
from utils.task_env import *

logger = config.get_logger()


@log
def p_cockpit_client_analyse_data(spark, busi_date):
    """
    客户分析落地表(客户分析-业务单位)
    """

    v_busi_month = busi_date[:6]
    v_last_busi_month = str(int(v_busi_month) - 100)
    v_last_year = str(int(v_busi_month[:4]) - 1)
    v_last_open_begin_date = v_last_year + '0101'
    v_last_open_end_date = v_last_year + '1231'
    v_open_begin_date = busi_date[:4] + '0101'
    v_open_end_date = v_busi_month + '31'  # TODO: 这里存疑

    v_begin_date, v_end_date, v_trade_days = get_date_period_and_days(
        spark=spark,
        busi_month=busi_date[:6],
        end_date=busi_date,
        is_trade_day=True
    )

    v_last_begin_date, v_last_end_date, v_last_trade_days = get_date_period_and_days(
        spark=spark,
        busi_month=v_last_busi_month,
        is_trade_day=True
    )

    # 本年已交易天数
    v_busi_year_trade_days = get_date_period_and_days(
        spark=spark,
        busi_year=busi_date[:4],
        begin_date=v_open_begin_date,
        end_date=v_end_date,
        is_trade_day=True
    )[2]

    # 上一年交易天数
    v_last_year_trade_days = get_date_period_and_days(
        spark=spark,
        begin_date=v_last_open_begin_date,
        end_date=v_last_open_end_date,
        is_trade_day=True
    )[2]

    # 初始化数据
    logger.info(to_color_str("初始化数据", "blue"))

    df_m = spark.table("ddw.T_OA_BRANCH").alias("t") \
        .filter(
        col("t.canceled").isNull()
    ).select(
        lit(v_busi_month).alias("busi_month"),
        col("t.departmentid").alias("oa_branch_id")
    )

    # TODO: CF_BUSIMG.T_COCKPIT_CLIENT_ANALYSE为分区表,分区字段为busi_month
    return_to_hive(
        spark=spark,
        target_table="ddw.T_COCKPIT_CLIENT_ANALYSE",
        df_result=df_m,
        insert_mode="overwrite",
    )

    df_m = spark.table("ddw.T_COCKPIT_CLIENT_ANALYSE").alias("t") \
        .filter(
        col("t.busi_month") == v_busi_month
    )

    # 权益结构
    logger.info(to_color_str("更新数据", "blue"))

    tmp = spark.table("edw.h15_client_sett").alias("t") \
        .filter(
        (col("t.busi_date").between(v_begin_date, v_end_date))
    ).join(
        other=spark.table("edw.h12_fund_account").alias("b"),
        on=(
                col("t.fund_account_id") == col("b.fund_account_id")
        ),
        how="left"
    ).join(
        other=spark.table("ddw.t_ctp_branch_oa_rela").alias("c"),
        on=(
                col("b.branch_id") == col("c.ctp_branch_id")
        ),
        how="inner"
    ).filter(
        col("c.oa_branch_id").isNotNull()
    ).groupBy(
        col("t.fund_account_id"),
        col("b.client_type"),
        col("c.oa_branch_id")
    ).agg(
        sum("t.rights").alias("rights")
    ).select(
        col("t.fund_account_id"),
        col("b.client_type"),
        col("c.oa_branch_id"),
        col("rights")
    )

    tmp_last = spark.table("edw.h15_client_sett").alias("t") \
        .filter(
        (col("t.busi_date").between(v_last_begin_date, v_last_end_date))
    ).join(
        other=spark.table("edw.h12_fund_account").alias("b"),
        on=(
                col("t.fund_account_id") == col("b.fund_account_id")
        ),
        how="left"
    ).join(
        other=spark.table("ddw.t_ctp_branch_oa_rela").alias("c"),
        on=(
                col("b.branch_id") == col("c.ctp_branch_id")
        ),
        how="inner"
    ).filter(
        col("c.oa_branch_id").isNotNull()
    ).groupBy(
        col("t.fund_account_id"),
        col("b.client_type"),
        col("c.oa_branch_id")
    ).agg(
        sum("t.rights").alias("rights")
    ).select(
        col("t.fund_account_id"),
        col("b.client_type"),
        col("c.oa_branch_id"),
        col("rights")
    )

    tmp_1 = tmp.alias("t") \
        .groupBy(
        col("t.oa_branch_id")
    ).agg(
        (
                sum("t.rights") / v_trade_days
        ).alias("total_rights"),
        (
                sum(
                    when(col("t.client_type") == "0", col("t.rights")).otherwise(0)
                ) / v_trade_days
        ).alias("rights_0"),
        (
                sum(
                    when(col("t.client_type") == "1", col("t.rights")).otherwise(0)
                ) / v_trade_days
        ).alias("rights_1"),
        (
                sum(
                    when(col("t.client_type").isin("3", "4"), col("t.rights")).otherwise(0)
                ) / v_trade_days
        ).alias("rights_3")
    ).select(
        col("t.oa_branch_id"),
        col("total_rights"),
        col("rights_0"),
        col("rights_1"),
        col("rights_3")
    )

    tmp_2 = tmp_last.alias("t") \
        .groupBy(
        col("t.oa_branch_id")
    ).agg(
        (
                sum("t.rights") / v_last_trade_days
        ).alias("total_rights"),
        (
                sum(
                    when(col("t.client_type") == "0", col("t.rights")).otherwise(0)
                ) / v_last_trade_days
        ).alias("rights_0"),
        (
                sum(
                    when(col("t.client_type") == "1", col("t.rights")).otherwise(0)
                ) / v_last_trade_days
        ).alias("rights_1"),
        (
                sum(
                    when(col("t.client_type").isin("3", "4"), col("t.rights")).otherwise(0)
                ) / v_last_trade_days
        ).alias("rights_3")
    ).select(
        col("t.oa_branch_id"),
        col("total_rights"),
        col("rights_0"),
        col("rights_1"),
        col("rights_3")
    )

    tmp_result = df_m.alias("t") \
        .join(
        other=tmp_1.alias("a"),
        on=col("t.oa_branch_id") == col("a.oa_branch_id"),
        how="left"
    ).join(
        other=tmp_2.alias("b"),
        on=col("t.oa_branch_id") == col("b.oa_branch_id"),
        how="left"
    ).select(
        col("t.oa_branch_id"),
        (
                when(
                    (col("a.total_rights").isNotNull()) & (col("a.total_rights") != 0),
                    col("a.rights_1") / col("a.total_rights")
                ).otherwise(0) * 100
        ).alias("rights_1"),
        (
                when(
                    (col("a.total_rights").isNotNull()) & (col("a.total_rights") != 0),
                    col("a.rights_3") / col("a.total_rights")
                ).otherwise(0) * 100
        ).alias("rights_3"),
        (
                when(
                    (col("a.total_rights").isNotNull()) & (col("a.total_rights") != 0),
                    col("a.rights_0") / col("a.total_rights")
                ).otherwise(0) * 100
        ).alias("rights_0"),
        (
                when(
                    (col("b.total_rights").isNotNull()) & (col("b.total_rights") != 0),
                    col("b.rights_1") / col("b.total_rights")
                ).otherwise(0) * 100
        ).alias("last_rights_1"),
        (
                when(
                    (col("b.total_rights").isNotNull()) & (col("b.total_rights") != 0),
                    col("b.rights_3") / col("b.total_rights")
                ).otherwise(0) * 100
        ).alias("last_rights_3"),
        (
                when(
                    (col("b.total_rights").isNotNull()) & (col("b.total_rights") != 0),
                    col("b.rights_0") / col("b.total_rights")
                ).otherwise(0) * 100
        ).alias("last_rights_0")
    )

    df_y = tmp_result.alias("t") \
        .select(
        col("t.oa_branch_id"),
        col("t.rights_1"),
        (
                when(
                    (col("t.last_rights_1").isNotNull()) & (col("t.last_rights_1") != 0),
                    col("t.rights_1") / col("t.last_rights_1") - 1
                ).otherwise(0) * 100
        ).alias("rights_1_yoy"),
        col("t.rights_3"),
        (
                when(
                    (col("t.last_rights_3").isNotNull()) & (col("t.last_rights_3") != 0),
                    col("t.rights_3") / col("t.last_rights_3") - 1
                ).otherwise(0) * 100
        ).alias("rights_3_yoy"),
        col("t.rights_0"),
        (
                when(
                    (col("t.last_rights_0").isNotNull()) & (col("t.last_rights_0") != 0),
                    col("t.rights_0") / col("t.last_rights_0") - 1
                ).otherwise(0) * 100
        ).alias("rights_0_yoy")
    )

    df_m = update_dataframe(
        df_to_update=df_m,
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=[
            "rights_1",
            "rights_1_yoy",
            "rights_3",
            "rights_3_yoy",
            "rights_0",
            "rights_0_yoy"
        ]
    )

    return_to_hive(
        spark=spark,
        target_table="ddw.T_COCKPIT_CLIENT_ANALYSE",
        df_result=df_m,
        insert_mode="overwrite",
    )

    df_m = spark.table("ddw.T_COCKPIT_CLIENT_ANALYSE").alias("t") \
        .filter(
        col("t.busi_month") == v_busi_month
    )

    # 新增权益结构
    logger.info(to_color_str("新增权益结构", "blue"))

    tmp = spark.table("edw.h15_client_sett").alias("t") \
        .filter(
        (col("t.busi_date").between(v_begin_date, v_end_date))
    ).join(
        other=spark.table("edw.h12_fund_account").alias("b"),
        on=(
                col("t.fund_account_id") == col("b.fund_account_id")
        ),
        how="left"
    ).filter(
        (col("b.open_date").between(v_open_begin_date, v_open_end_date))
    ).join(
        other=spark.table("ddw.t_ctp_branch_oa_rela").alias("c"),
        on=(
                col("b.branch_id") == col("c.ctp_branch_id")
        ),
        how="inner"
    ).filter(
        col("c.oa_branch_id").isNotNull()
    ).groupBy(
        col("t.fund_account_id"),
        col("b.client_type"),
        col("c.oa_branch_id")
    ).agg(
        sum("t.rights").alias("rights")
    ).select(
        col("t.fund_account_id"),
        col("b.client_type"),
        col("c.oa_branch_id"),
        col("rights")
    )

    tmp_1 = tmp.alias("t") \
        .groupBy(
        col("t.oa_branch_id")
    ).agg(
        (
                sum("t.rights") / v_trade_days
        ).alias("total_rights"),
        (
                sum(
                    when(col("t.client_type") == "0", col("t.rights")).otherwise(0)
                ) / v_trade_days
        ).alias("rights_0"),
        (
                sum(
                    when(col("t.client_type") == "1", col("t.rights")).otherwise(0)
                ) / v_trade_days
        ).alias("rights_1"),
        (
                sum(
                    when(col("t.client_type").isin("3", "4"), col("t.rights")).otherwise(0)
                ) / v_trade_days
        ).alias("rights_3")
    ).select(
        col("t.oa_branch_id"),
        col("total_rights"),
        col("rights_0"),
        col("rights_1"),
        col("rights_3")
    )

    tmp_result = df_m.alias("t") \
        .join(
        other=tmp_1.alias("a"),
        on=col("t.oa_branch_id") == col("a.oa_branch_id"),
        how="left"
    ).select(
        col("t.oa_branch_id"),
        (
                when(
                    (col("a.total_rights").isNotNull()) & (col("a.total_rights") != 0),
                    col("a.rights_1") / col("a.total_rights")
                ).otherwise(0) * 100
        ).alias("NEW_RIGHTS_1"),
        (
                when(
                    (col("a.total_rights").isNotNull()) & (col("a.total_rights") != 0),
                    col("a.rights_3") / col("a.total_rights")
                ).otherwise(0) * 100
        ).alias("NEW_RIGHTS_3"),
        (
                when(
                    (col("a.total_rights").isNotNull()) & (col("a.total_rights") != 0),
                    col("a.rights_0") / col("a.total_rights")
                ).otherwise(0) * 100
        ).alias("NEW_RIGHTS_0")
    )

    df_y = tmp_result.alias("t") \
        .select(
        col("t.oa_branch_id"),
        col("t.NEW_RIGHTS_1"),
        col("t.NEW_RIGHTS_3"),
        col("t.NEW_RIGHTS_0")
    )

    df_m = update_dataframe(
        df_to_update=df_m,
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=[
            "NEW_RIGHTS_1",
            "NEW_RIGHTS_3",
            "NEW_RIGHTS_0"
        ]
    )

    return_to_hive(
        spark=spark,
        target_table="ddw.T_COCKPIT_CLIENT_ANALYSE",
        df_result=df_m,
        insert_mode="overwrite",
    )

    df_m = spark.table("ddw.T_COCKPIT_CLIENT_ANALYSE").alias("t") \
        .filter(
        col("t.busi_month") == v_busi_month
    )

    # 成交量结构
    # 成交额结构
    logger.info(to_color_str("成交量结构 成交额结构", "blue"))

    tmp = spark.table("edw.h15_hold_balance").alias("t") \
        .filter(
        (col("t.busi_date").between(v_begin_date, v_end_date))
    ).join(
        other=spark.table("edw.h12_fund_account").alias("b"),
        on=(
                col("t.fund_account_id") == col("b.fund_account_id")
        ),
        how="left"
    ).join(
        other=spark.table("ddw.t_ctp_branch_oa_rela").alias("c"),
        on=(
                col("b.branch_id") == col("c.ctp_branch_id")
        ),
        how="inner"
    ).filter(
        col("c.oa_branch_id").isNotNull()
    ).groupBy(
        col("t.fund_account_id"),
        col("b.client_type"),
        col("c.oa_branch_id")
    ).agg(
        sum("t.done_amt").alias("done_amount"),
        sum("t.done_sum").alias("done_money")
    ).select(
        col("t.fund_account_id"),
        col("b.client_type"),
        col("c.oa_branch_id"),
        col("done_amount"),
        col("done_money")
    )

    tmp_last = spark.table("edw.h15_hold_balance").alias("t") \
        .filter(
        (col("t.busi_date").between(v_last_begin_date, v_last_end_date))
    ).join(
        other=spark.table("edw.h12_fund_account").alias("b"),
        on=(
                col("t.fund_account_id") == col("b.fund_account_id")
        ),
        how="left"
    ).join(
        other=spark.table("ddw.t_ctp_branch_oa_rela").alias("c"),
        on=(
                col("b.branch_id") == col("c.ctp_branch_id")
        ),
        how="inner"
    ).filter(
        col("c.oa_branch_id").isNotNull()
    ).groupBy(
        col("t.fund_account_id"),
        col("b.client_type"),
        col("c.oa_branch_id")
    ).agg(
        sum("t.done_amt").alias("done_amount"),
        sum("t.done_sum").alias("done_money")
    ).select(
        col("t.fund_account_id"),
        col("b.client_type"),
        col("c.oa_branch_id"),
        col("done_amount"),
        col("done_money")
    )

    tmp_1 = tmp.alias("t") \
        .groupBy(
        col("t.oa_branch_id")
    ).agg(
        sum("t.done_amount").alias("total_done_amount"),
        sum("t.done_money").alias("total_done_money"),
        sum(
            when(col("t.client_type") == "0", col("t.done_amount")).otherwise(0)
        ).alias("DONE_AMOUNT_0"),
        sum(
            when(col("t.client_type") == "1", col("t.done_amount")).otherwise(0)
        ).alias("DONE_AMOUNT_1"),
        sum(
            when(col("t.client_type").isin("3", "4"), col("t.done_amount")).otherwise(0)
        ).alias("DONE_AMOUNT_3"),
        sum(
            when(col("t.client_type") == "0", col("t.done_money")).otherwise(0)
        ).alias("done_money_0"),
        sum(
            when(col("t.client_type") == "1", col("t.done_money")).otherwise(0)
        ).alias("done_money_1"),
        sum(
            when(col("t.client_type").isin("3", "4"), col("t.done_money")).otherwise(0)
        ).alias("done_money_3")
    ).select(
        col("t.oa_branch_id"),
        col("total_done_amount"),
        col("total_done_money"),
        col("DONE_AMOUNT_0"),
        col("DONE_AMOUNT_1"),
        col("DONE_AMOUNT_3"),
        col("done_money_0"),
        col("done_money_1"),
        col("done_money_3")
    )

    tmp_2 = tmp_last.alias("t") \
        .groupBy(
        col("t.oa_branch_id")
    ).agg(
        sum("t.done_amount").alias("total_done_amount"),
        sum("t.done_money").alias("total_done_money"),
        sum(
            when(col("t.client_type") == "0", col("t.done_amount")).otherwise(0)
        ).alias("DONE_AMOUNT_0"),
        sum(
            when(col("t.client_type") == "1", col("t.done_amount")).otherwise(0)
        ).alias("DONE_AMOUNT_1"),
        sum(
            when(col("t.client_type").isin("3", "4"), col("t.done_amount")).otherwise(0)
        ).alias("DONE_AMOUNT_3"),
        sum(
            when(col("t.client_type") == "0", col("t.done_money")).otherwise(0)
        ).alias("done_money_0"),
        sum(
            when(col("t.client_type") == "1", col("t.done_money")).otherwise(0)
        ).alias("done_money_1"),
        sum(
            when(col("t.client_type").isin("3", "4"), col("t.done_money")).otherwise(0)
        ).alias("done_money_3")
    ).select(
        col("t.oa_branch_id"),
        col("total_done_amount"),
        col("total_done_money"),
        col("DONE_AMOUNT_0"),
        col("DONE_AMOUNT_1"),
        col("DONE_AMOUNT_3"),
        col("done_money_0"),
        col("done_money_1"),
        col("done_money_3")
    )

    tmp_result = df_m.alias("t").join(
        other=tmp_1.alias("a"),
        on=col("t.oa_branch_id") == col("a.oa_branch_id"),
        how="left"
    ).join(
        other=tmp_2.alias("b"),
        on=col("t.oa_branch_id") == col("b.oa_branch_id"),
        how="left"
    ).select(
        col("t.oa_branch_id"),
        (
                when(
                    (col("a.total_done_amount").isNotNull()) & (col("a.total_done_amount") != 0),
                    col("a.DONE_AMOUNT_1") / col("a.total_done_amount")
                ).otherwise(0) * 100
        ).alias("DONE_AMOUNT_1"),
        (
                when(
                    (col("a.total_done_amount").isNotNull()) & (col("a.total_done_amount") != 0),
                    col("a.DONE_AMOUNT_3") / col("a.total_done_amount")
                ).otherwise(0) * 100
        ).alias("DONE_AMOUNT_3"),
        (
                when(
                    (col("a.total_done_amount").isNotNull()) & (col("a.total_done_amount") != 0),
                    col("a.DONE_AMOUNT_0") / col("a.total_done_amount")
                ).otherwise(0) * 100
        ).alias("DONE_AMOUNT_0"),
        (
                when(
                    (col("b.total_done_amount").isNotNull()) & (col("b.total_done_amount") != 0),
                    col("b.DONE_AMOUNT_1") / col("b.total_done_amount")
                ).otherwise(0) * 100
        ).alias("last_DONE_AMOUNT_1"),
        (
                when(
                    (col("b.total_done_amount").isNotNull()) & (col("b.total_done_amount") != 0),
                    col("b.DONE_AMOUNT_3") / col("b.total_done_amount")
                ).otherwise(0) * 100
        ).alias("last_DONE_AMOUNT_3"),
        (
                when(
                    (col("b.total_done_amount").isNotNull()) & (col("b.total_done_amount") != 0),
                    col("b.DONE_AMOUNT_0") / col("b.total_done_amount")
                ).otherwise(0) * 100
        ).alias("last_DONE_AMOUNT_0"),
        (
                when(
                    (col("a.total_done_money").isNotNull()) & (col("a.total_done_money") != 0),
                    col("a.done_money_1") / col("a.total_done_money")
                ).otherwise(0) * 100
        ).alias("done_money_1"),
        (
                when(
                    (col("a.total_done_money").isNotNull()) & (col("a.total_done_money") != 0),
                    col("a.done_money_3") / col("a.total_done_money")
                ).otherwise(0) * 100
        ).alias("done_money_3"),
        (
                when(
                    (col("a.total_done_money").isNotNull()) & (col("a.total_done_money") != 0),
                    col("a.done_money_0") / col("a.total_done_money")
                ).otherwise(0) * 100
        ).alias("done_money_0"),
        (
                when(
                    (col("b.total_done_money").isNotNull()) & (col("b.total_done_money") != 0),
                    col("b.done_money_1") / col("b.total_done_money")
                ).otherwise(0) * 100
        ).alias("last_done_money_1"),
        (
                when(
                    (col("b.total_done_money").isNotNull()) & (col("b.total_done_money") != 0),
                    col("b.done_money_3") / col("b.total_done_money")
                ).otherwise(0) * 100
        ).alias("last_done_money_3"),
        (
                when(
                    (col("b.total_done_money").isNotNull()) & (col("b.total_done_money") != 0),
                    col("b.done_money_0") / col("b.total_done_money")
                ).otherwise(0) * 100
        ).alias("last_done_money_0")
    )

    df_y = tmp_result.alias("t") \
        .select(
        col("t.oa_branch_id"),
        col("t.DONE_AMOUNT_1"),
        (
                when(
                    (col("t.last_DONE_AMOUNT_1").isNotNull()) & (col("t.last_DONE_AMOUNT_1") != 0),
                    col("t.DONE_AMOUNT_1") / col("t.last_DONE_AMOUNT_1") - 1
                ).otherwise(0) * 100
        ).alias("DONE_AMOUNT_1_yoy"),
        col("t.DONE_AMOUNT_3"),
        (
                when(
                    (col("t.last_DONE_AMOUNT_3").isNotNull()) & (col("t.last_DONE_AMOUNT_3") != 0),
                    col("t.DONE_AMOUNT_3") / col("t.last_DONE_AMOUNT_3") - 1
                ).otherwise(0) * 100
        ).alias("DONE_AMOUNT_3_yoy"),
        col("t.DONE_AMOUNT_0"),
        (
                when(
                    (col("t.last_DONE_AMOUNT_0").isNotNull()) & (col("t.last_DONE_AMOUNT_0") != 0),
                    col("t.DONE_AMOUNT_0") / col("t.last_DONE_AMOUNT_0") - 1
                ).otherwise(0) * 100
        ).alias("DONE_AMOUNT_0_yoy"),
        col("t.done_money_1"),
        (
                when(
                    (col("t.last_done_money_1").isNotNull()) & (col("t.last_done_money_1") != 0),
                    col("t.done_money_1") / col("t.last_done_money_1") - 1
                ).otherwise(0) * 100
        ).alias("done_money_1_yoy"),
        col("t.done_money_3"),
        (
                when(
                    (col("t.last_done_money_3").isNotNull()) & (col("t.last_done_money_3") != 0),
                    col("t.done_money_3") / col("t.last_done_money_3") - 1
                ).otherwise(0) * 100
        ).alias("done_money_3_yoy"),
        col("t.done_money_0"),
        (
                when(
                    (col("t.last_done_money_0").isNotNull()) & (col("t.last_done_money_0") != 0),
                    col("t.done_money_0") / col("t.last_done_money_0") - 1
                ).otherwise(0) * 100
        ).alias("done_money_0_yoy")
    )

    df_m = update_dataframe(
        df_to_update=df_m,
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=[
            "DONE_AMOUNT_1",
            "DONE_AMOUNT_1_yoy",
            "DONE_AMOUNT_3",
            "DONE_AMOUNT_3_yoy",
            "DONE_AMOUNT_0",
            "DONE_AMOUNT_0_yoy",
            "done_money_1",
            "done_money_1_yoy",
            "done_money_3",
            "done_money_3_yoy",
            "done_money_0",
            "done_money_0_yoy"
        ]
    )

    return_to_hive(
        spark=spark,
        target_table="ddw.T_COCKPIT_CLIENT_ANALYSE",
        df_result=df_m,
        insert_mode="overwrite",
    )

    df_m = spark.table("ddw.T_COCKPIT_CLIENT_ANALYSE").alias("t") \
        .filter(
        col("t.busi_month") == v_busi_month
    )

    # 经纪业务收入结构
    # TODO: 暂时没有逻辑，不开发

    # 增量权益分析-当年新开客户权益(万元)
    logger.info(to_color_str("增量权益分析-当年新开客户权益(万元)", "blue"))

    tmp = spark.table("edw.h15_client_sett").alias("t") \
        .filter(
        (col("t.busi_date").between(v_open_begin_date, v_end_date))
    ).join(
        other=spark.table("edw.h12_fund_account").alias("b"),
        on=(
                col("t.fund_account_id") == col("b.fund_account_id")
        ),
        how="left"
    ).filter(
        (col("b.open_date").between(v_open_begin_date, v_open_end_date))
    ).join(
        other=spark.table("ddw.t_ctp_branch_oa_rela").alias("c"),
        on=(
                col("b.branch_id") == col("c.ctp_branch_id")
        ),
        how="inner"
    ).filter(
        col("c.oa_branch_id").isNotNull()
    ).groupBy(
        col("t.fund_account_id"),
        col("c.oa_branch_id")
    ).agg(
        sum("t.rights").alias("rights")
    ).select(
        col("t.fund_account_id"),
        col("c.oa_branch_id"),
        col("rights")
    )

    df_y = tmp.alias("t") \
        .groupBy(
        col("t.oa_branch_id")
    ).agg(
        (sum("t.rights") / v_busi_year_trade_days * 10000).alias("RIGHTS_ADD_NEW")
    ).select(
        col("t.oa_branch_id"),
        col("RIGHTS_ADD_NEW")
    )

    df_m = update_dataframe(
        df_to_update=df_m,
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=[
            "RIGHTS_ADD_NEW"
        ]
    )

    return_to_hive(
        spark=spark,
        target_table="ddw.T_COCKPIT_CLIENT_ANALYSE",
        df_result=df_m,
        insert_mode="overwrite",
    )

    df_m = spark.table("ddw.T_COCKPIT_CLIENT_ANALYSE").alias("t") \
        .filter(
        col("t.busi_month") == v_busi_month
    )

    # 增量权益分析-老客户权益变化值(万元)
    # 排除当年新增客户日均权益的所有客户的日群权益，减掉去年全年客户日均权益
    logger.info(to_color_str("增量权益分析-老客户权益变化值(万元)", "blue"))

    tmp = spark.table("edw.h15_client_sett").alias("t") \
        .filter(
        (col("t.busi_date").between(v_open_begin_date, v_end_date))
    ).join(
        other=spark.table("edw.h12_fund_account").alias("b"),
        on=(
                col("t.fund_account_id") == col("b.fund_account_id")
        ),
        how="left"
    ).filter(
        (col("b.open_date") < v_open_begin_date)
    ).join(
        other=spark.table("ddw.t_ctp_branch_oa_rela").alias("c"),
        on=(
                col("b.branch_id") == col("c.ctp_branch_id")
        ),
        how="inner"
    ).filter(
        col("c.oa_branch_id").isNotNull()
    ).groupBy(
        col("t.fund_account_id"),
        col("c.oa_branch_id")
    ).agg(
        sum("t.rights").alias("rights")
    ).select(
        col("t.fund_account_id"),
        col("c.oa_branch_id"),
        col("rights")
    )

    tmp_last = spark.table("edw.h15_client_sett").alias("t") \
        .filter(
        (col("t.busi_date").between(v_last_open_begin_date, v_last_open_end_date))
    ).join(
        other=spark.table("edw.h12_fund_account").alias("b"),
        on=(
                col("t.fund_account_id") == col("b.fund_account_id")
        ),
        how="left"
    ).join(
        other=spark.table("ddw.t_ctp_branch_oa_rela").alias("c"),
        on=(
                col("b.branch_id") == col("c.ctp_branch_id")
        ),
        how="inner"
    ).filter(
        col("c.oa_branch_id").isNotNull()
    ).groupBy(
        col("t.fund_account_id"),
        col("c.oa_branch_id")
    ).agg(
        sum("t.rights").alias("rights")
    ).select(
        col("t.fund_account_id"),
        col("c.oa_branch_id"),
        col("rights")
    )

    tmp_result1 = tmp.alias("t") \
        .groupBy(
        col("t.oa_branch_id")
    ).agg(
        (sum("t.rights") / v_busi_year_trade_days).alias("avg_rights")
    ).select(
        col("t.oa_branch_id"),
        col("avg_rights")
    )

    tmp_result2 = tmp_last.alias("t") \
        .groupBy(
        col("t.oa_branch_id")
    ).agg(
        (sum("t.rights") / v_last_year_trade_days).alias("avg_rights")
    ).select(
        col("t.oa_branch_id"),
        col("avg_rights")
    )

    df_y = df_m.alias("t").join(
        other=tmp_result1.alias("a"),
        on=col("t.oa_branch_id") == col("a.oa_branch_id"),
        how="left"
    ).join(
        other=tmp_result2.alias("b"),
        on=col("t.oa_branch_id") == col("b.oa_branch_id"),
        how="left"
    ).select(
        col("t.oa_branch_id"),
        (col("a.avg_rights") - col("b.avg_rights")).alias("RIGHTS_ADD_OLD")
    )

    df_m = update_dataframe(
        df_to_update=df_m,
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=[
            "RIGHTS_ADD_OLD"
        ]
    )

    return_to_hive(
        spark=spark,
        target_table="ddw.T_COCKPIT_CLIENT_ANALYSE",
        df_result=df_m,
        insert_mode="overwrite",
    )

    df_m = spark.table("ddw.T_COCKPIT_CLIENT_ANALYSE").alias("t") \
        .filter(
        col("t.busi_month") == v_busi_month
    )

    # 客户年龄结构
    # 有效客户年龄结构
    logger.info(to_color_str("客户年龄结构 有效客户年龄结构", "blue"))

    tmp = spark.table("edw.h15_hold_balance").alias("t") \
        .filter(
        (col("t.busi_date").between(v_begin_date, v_end_date))
    ).join(
        other=spark.table("edw.h12_fund_account").alias("b"),
        on=(
                col("t.fund_account_id") == col("b.fund_account_id")
        ),
        how="left"
    ).join(
        other=spark.table("ddw.t_ctp_branch_oa_rela").alias("c"),
        on=(
                col("b.branch_id") == col("c.ctp_branch_id")
        ),
        how="inner"
    ).filter(
        col("c.oa_branch_id").isNotNull()
    ).select(
        col("t.fund_account_id"),
        col("c.oa_branch_id")
    ).dropDuplicates()

    tmp_1 = tmp.alias("t") \
        .join(
        other=spark.table("edw.h12_fund_account").alias("b"),
        on=col("t.fund_account_id") == col("b.fund_account_id"),
        how="left"
    ).select(
        col("t.fund_account_id"),
        col("t.oa_branch_id"),
        substring(col("b.id_no"), 7, 8).alias("birth")
    )

    df_y = tmp_1.alias("t") \
        .groupBy(
        col("t.oa_branch_id")
    ).agg(
        sum(
            when(col("t.birth") <= "19691231", 1).otherwise(0)
        ).alias("AGE_60"),
        sum(
            when(
                (col("t.birth") >= "19700101") & (col("t.birth") <= "19791231"),
                1
            ).otherwise(0)
        ).alias("AGE_70"),
        sum(
            when(
                (col("t.birth") >= "19800101") & (col("t.birth") <= "19891231"),
                1
            ).otherwise(0)
        ).alias("AGE_80"),
        sum(
            when(
                (col("t.birth") >= "19900101") & (col("t.birth") <= "19991231"),
                1
            ).otherwise(0)
        ).alias("AGE_90"),
        sum(
            when(
                col("t.birth") >= "20000101",
                1
            ).otherwise(0)
        ).alias("AGE_00")
    ).select(
        col("t.oa_branch_id"),
        col("AGE_60"),
        col("AGE_70"),
        col("AGE_80"),
        col("AGE_90"),
        col("AGE_00")
    )

    df_m = update_dataframe(
        df_to_update=df_m,
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=[
            "AGE_60",
            "AGE_70",
            "AGE_80",
            "AGE_90",
            "AGE_00"
        ]
    )

    return_to_hive(
        spark=spark,
        target_table="ddw.T_COCKPIT_CLIENT_ANALYSE",
        df_result=df_m,
        insert_mode="overwrite",
    )

    df_m = spark.table("ddw.T_COCKPIT_CLIENT_ANALYSE").alias("t") \
        .filter(
        col("t.busi_month") == v_busi_month
    )

    # 客户年龄结构
    # 新增有效客户年龄结构
    # 新开户
    logger.info(to_color_str("客户年龄结构", "blue"))

    tmp = spark.table("edw.h15_hold_balance").alias("t") \
        .filter(
        (col("t.busi_date").between(v_open_begin_date, v_end_date))
    ).join(
        other=spark.table("edw.h12_fund_account").alias("b"),
        on=(
                col("t.fund_account_id") == col("b.fund_account_id")
        ),
        how="left"
    ).filter(
        (col("b.open_date").between(v_open_begin_date, v_open_end_date))
    ).join(
        other=spark.table("ddw.t_ctp_branch_oa_rela").alias("c"),
        on=(
                col("b.branch_id") == col("c.ctp_branch_id")
        ),
        how="inner"
    ).filter(
        col("c.oa_branch_id").isNotNull()
    ).select(
        col("t.fund_account_id"),
        col("c.oa_branch_id")
    ).dropDuplicates()

    tmp_1 = tmp.alias("t") \
        .join(
        other=spark.table("edw.h12_fund_account").alias("b"),
        on=col("t.fund_account_id") == col("b.fund_account_id"),
        how="left"
    ).select(
        col("t.fund_account_id"),
        col("t.oa_branch_id"),
        substring(col("b.id_no"), 7, 8).alias("birth")
    )

    df_y = tmp_1.alias("t") \
        .groupBy(
        col("t.oa_branch_id")
    ).agg(
        sum(
            when(col("t.birth") <= "19691231", 1).otherwise(0)
        ).alias("AGE_60_NEW"),
        sum(
            when(
                (col("t.birth") >= "19700101") & (col("t.birth") <= "19791231"),
                1
            ).otherwise(0)
        ).alias("AGE_70_NEW"),
        sum(
            when(
                (col("t.birth") >= "19800101") & (col("t.birth") <= "19891231"),
                1
            ).otherwise(0)
        ).alias("AGE_80_NEW"),
        sum(
            when(
                (col("t.birth") >= "19900101") & (col("t.birth") <= "19991231"),
                1
            ).otherwise(0)
        ).alias("AGE_90_NEW"),
        sum(
            when(
                col("t.birth") >= "20000101",
                1
            ).otherwise(0)
        ).alias("AGE_00_NEW")
    ).select(
        col("t.oa_branch_id"),
        col("AGE_60_NEW"),
        col("AGE_70_NEW"),
        col("AGE_80_NEW"),
        col("AGE_90_NEW"),
        col("AGE_00_NEW")
    )

    df_m = update_dataframe(
        df_to_update=df_m,
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=[
            "AGE_60_NEW",
            "AGE_70_NEW",
            "AGE_80_NEW",
            "AGE_90_NEW",
            "AGE_00_NEW"
        ]
    )

    return_to_hive(
        spark=spark,
        target_table="ddw.T_COCKPIT_CLIENT_ANALYSE",
        df_result=df_m,
        insert_mode="overwrite",
    )

    df_m = spark.table("ddw.T_COCKPIT_CLIENT_ANALYSE").alias("t") \
        .filter(
        col("t.busi_month") == v_busi_month
    )

    # 计算客户的净贡献
    logger.info(to_color_str("计算客户的净贡献", "blue"))

    # TODO: 以下逻辑看花眼,自动生成没检查
    df_jgx = spark.table("ods.t_ds_adm_investor_value").alias("a") \
        .filter(
        regexp_replace(col("a.date_dt"), "-", "").between(v_begin_date, v_end_date)
    ).join(
        other=spark.table("ods.t_ds_adm_brokerdata_detail").alias("a2"),
        on=(
                (col("a.date_dt") == col("a2.tx_dt")) &
                (col("a.investor_id") == col("a2.investor_id")) &
                (col("a2.rec_freq") == "M")
        ),
        how="inner"
    ).groupBy(
        col("a.investor_id")
    ).agg(
        sum(
            col("a.subsistence_fee_amt") *
            when(col("a2.pct") == 2, col("a2.pct")).otherwise(col("a2.data_pct"))
        ).alias("subsistence_fee_amt"),
        sum(
            round(col("a.int_amt") *
                  when(col("a2.pct") == 2, col("a2.pct")).otherwise(col("a2.data_pct")),
                  2)
        ).alias("int_amt"),
        sum(
            round(col("a.exchangeret_amt") *
                  when(col("a2.pct") == 2, col("a2.pct")).otherwise(col("a2.data_pct")),
                  2)
        ).alias("exchangeret_amt"),
        sum(
            col("a.oth_amt") *
            when(col("a2.pct") == 2, col("a2.pct")).otherwise(col("a2.data_pct"))
        ).alias("oth_amt"),
        sum(
            round(col("a.fd_amt") *
                  when(col("a2.pct") == 2, col("a2.pct")).otherwise(col("a2.data_pct")),
                  2)
        ).alias("fd_amt"),
        sum(
            round(col("a.i_int_amt") *
                  when(col("a2.pct") == 2, col("a2.pct")).otherwise(col("a2.data_pct")),
                  2)
        ).alias("i_int_amt"),
        sum(
            col("a.i_exchangeret_amt") *
            when(col("a2.pct") == 2, col("a2.pct")).otherwise(col("a2.data_pct"))
        ).alias("i_exchangeret_amt"),
        sum(
            col("a.broker_amt") *
            when(col("a2.pct") == 2, col("a2.pct")).otherwise(col("a2.data_pct"))
        ).alias("broker_amt"),
        sum(
            col("a.soft_amt") *
            when(col("a2.pct") == 2, col("a2.pct")).otherwise(col("a2.data_pct"))
        ).alias("soft_amt"),
        sum(
            col("a.i_oth_amt") *
            when(col("a2.pct") == 2, col("a2.pct")).otherwise(col("a2.data_pct"))
        ).alias("i_oth_amt"),
        sum(
            round(col("a.broker_int_amt") *
                  when(col("a2.pct") == 2, col("a2.pct")).otherwise(col("a2.data_pct")),
                  2)
        ).alias("broker_int_amt"),
        sum(
            round(col("a.broker_eret_amt") *
                  when(col("a2.pct") == 2, col("a2.pct")).otherwise(col("a2.data_pct")),
                  2)
        ).alias("broker_eret_amt"),
        sum(
            round(
                coalesce(col("a2.ib_amt"), col("a.staff_amt") *
                         when(col("a2.pct") == 2, col("a2.pct")).otherwise(col("a2.data_pct"))
                         ),
                2
            )
        ).alias("ib_amt"),
        sum(
            round(
                coalesce(col("a2.staff_int_amt"), col("a.staff_int_amt") *
                         when(col("a2.pct") == 2, col("a2.pct")).otherwise(col("a2.data_pct"))
                         ),
                2
            )
        ).alias("staff_int_amt"),
        sum(
            round(col("a.staff_eret_amt") *
                  when(col("a2.pct") == 2, col("a2.pct")).otherwise(col("a2.data_pct")),
                  2)
        ).alias("staff_eret_amt")
    ).select(
        col("a.investor_id"),
        (
                col("subsistence_fee_amt") +
                col("int_amt") +
                col("exchangeret_amt") +
                col("oth_amt") -
                col("fd_amt") -
                col("i_int_amt") -
                col("i_exchangeret_amt") -
                col("broker_amt") -
                col("soft_amt") -
                col("i_oth_amt") -
                col("broker_int_amt") -
                col("broker_eret_amt") -
                col("ib_amt") -
                col("staff_int_amt") -
                col("staff_eret_amt")
        ).alias("jgx")
    )

    return_to_hive(
        spark=spark,
        df_result=df_jgx,
        target_table="ddw.tmp_COCKPIT_CLIENT_jgx",
        insert_mode="overwrite"
    )

    """
    千万工程-有效客户数,
    千万工程-日均权益, (亿元)
    千万工程-成交量, (万手)
    千万工程-成交额, (亿元)
    千万工程-净贡献(万元)
    """
    logger.info(to_color_str("千万工程-有效客户数, 千万工程-日均权益, 千万工程-成交量, 千万工程-成交额, 千万工程-净贡献", "blue"))

    my_dict = {
        "BQ4909": "QW",
        "BQ4908": "GP",
        "BQ4910": "GJZ",
        "BQ4907": "tb",
    }

    def cal_qw_gp_gjz_tb(label_id, prefix, df_m_func):
        """
        计算千万工程-有效客户数, 千万工程-日均权益, 千万工程-成交量, 千万工程-成交额, 千万工程-净贡献
        :param label_id: 标签ID
        :param prefix: 前缀
        :param df_m_func: DataFrame
        :return: DataFrame
        """

        df_tmp = spark.table("ods.T_crmmg_HIS_LABEL_CLIENT").alias("t") \
            .filter(
            (col("t.months") == v_busi_month)
        ).join(
            other=spark.table("ods.t_crmmg_label").alias("c"),
            on=(
                    col("t.label_id") == col("c.label_id")
            ),
            how="inner"
        ).filter(
            col("c.label_id") == label_id
        ).join(
            other=spark.table("edw.h12_fund_account").alias("a"),
            on=(
                    col("t.client_id") == col("a.fund_account_id")
            ),
            how="inner"
        ).join(
            other=spark.table("ddw.t_ctp_branch_oa_rela").alias("b"),
            on=(
                    col("a.branch_id") == col("b.ctp_branch_id")
            ),
            how="inner"
        ).filter(
            col("b.oa_branch_id").isNotNull()
        ).select(
            col("b.oa_branch_id"),
            col("a.fund_account_id")
        )

        df_tmp_0 = df_tmp.alias("a") \
            .groupBy(
            col("a.oa_branch_id")
        ).agg(
            count(lit(1)).alias("client_num")
        ).select(
            col("a.oa_branch_id"),
            col("client_num")
        )

        df_tmp_1 = spark.table("edw.h15_client_sett").alias("t") \
            .filter(
            (col("t.busi_date").between(v_begin_date, v_end_date))
        ).join(
            other=df_tmp.alias("a"),
            on=(
                    col("t.fund_account_id") == col("a.fund_account_id")
            ),
            how="inner"
        ).groupBy(
            col("a.oa_branch_id")
        ).agg(
            (sum(col("t.rights")) / v_trade_days).alias("avg_rights")
        ).select(
            col("a.oa_branch_id"),
            col("avg_rights")
        )

        df_tmp_2 = spark.table("edw.h15_hold_balance").alias("t") \
            .filter(
            (col("t.busi_date").between(v_begin_date, v_end_date))
        ).join(
            other=df_tmp.alias("a"),
            on=(
                    col("t.fund_account_id") == col("a.fund_account_id")
            ),
            how="inner"
        ).groupBy(
            col("a.oa_branch_id")
        ).agg(
            sum("t.done_amt").alias("done_amount"),
            sum("t.done_sum").alias("done_sum")
        ).select(
            col("a.oa_branch_id"),
            col("done_amount"),
            col("done_sum")
        )

        df_tmp_3 = spark.table("ddw.tmp_COCKPIT_CLIENT_jgx").alias("t") \
            .join(
            other=df_tmp.alias("a"),
            on=(
                    col("t.CLIENT_ID") == col("a.fund_account_id")
            ),
            how="inner"
        ).groupBy(
            col("a.oa_branch_id")
        ).agg(
            sum("t.jgx").alias("jgx")
        ).select(
            col("a.oa_branch_id"),
            col("jgx")
        )

        df_y_func = df_m_func.alias("t") \
            .join(
            other=df_tmp_0.alias("a"),
            on=col("t.oa_branch_id") == col("a.oa_branch_id"),
            how="left"
        ).join(
            other=df_tmp_1.alias("b"),
            on=col("t.oa_branch_id") == col("b.oa_branch_id"),
            how="left"
        ).join(
            other=df_tmp_2.alias("c"),
            on=col("t.oa_branch_id") == col("c.oa_branch_id"),
            how="left"
        ).join(
            other=df_tmp_3.alias("d"),
            on=col("t.oa_branch_id") == col("d.oa_branch_id"),
            how="left"
        ).select(
            col("t.oa_branch_id"),
            coalesce(col("a.client_num"), lit(0)).alias("{}_CLIENT_NUM".format(prefix)),
            (coalesce(col("b.avg_rights"), lit(0)) / 100000000).alias("{}_AVG_RIGHTS".format(prefix)),
            (coalesce(col("c.done_amount"), lit(0)) / 10000).alias("{}_DONE_AMOUNT".format(prefix)),
            (coalesce(col("c.done_sum"), lit(0)) / 100000000).alias("{}_DONE_MONEY".format(prefix)),
            (coalesce(col("d.jgx"), lit(0)) / 10000).alias("{}_NET_CONTRIBUTION".format(prefix))
        )

        df_m_func = update_dataframe(
            df_to_update=df_m_func,
            df_use_me=df_y_func,
            join_columns=["oa_branch_id"],
            update_columns=[
                "{}_CLIENT_NUM".format(prefix),
                "{}_AVG_RIGHTS".format(prefix),
                "{}_DONE_AMOUNT".format(prefix),
                "{}_DONE_MONEY".format(prefix),
                "{}_NET_CONTRIBUTION".format(prefix)
            ]
        )

        return df_m_func

    for key, value in my_dict.items():
        df_m = cal_qw_gp_gjz_tb(key, value, df_m)
        return_to_hive(
            spark=spark,
            target_table="ddw.T_COCKPIT_CLIENT_ANALYSE",
            df_result=df_m,
            insert_mode="overwrite",
        )

        df_m = spark.table("ddw.T_COCKPIT_CLIENT_ANALYSE").alias("t") \
            .filter(
            col("t.busi_month") == v_busi_month
        )

    return_to_hive(
        spark=spark,
        df_result=df_m,
        target_table="ddw.T_COCKPIT_CLIENT_ANALYSE",
        insert_mode="overwrite",
        partition_column=["busi_month"],
        partition_value=v_busi_month
    )
