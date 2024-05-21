# -*- coding: utf-8 -*-

from pyspark.sql.functions import sum

from utils.date_utils import *
from utils.task_env import *

logger = Config().get_logger()


@log
def p_cockpit_busi_analyse_d_data(spark, busi_date):
    """
    经营分析-业务单位-单日期落地
    """

    v_new_begin_date = busi_date[:4] + "0101"
    v_new_end_date = busi_date
    v_yoy_busi_date = get_previous_year_date(busi_date)
    v_yoy_busi_date = get_date_period_and_days(
        spark=spark,
        end_date=v_yoy_busi_date,
        is_trade_day=True
    )[1]

    # TODO: CF_BUSIMG.T_COCKPIT_BUSI_ANALYSE_D,分区字段,busi_date

    # 初始化数据
    df_d = spark.table("ddw.T_OA_BRANCH").alias("t") \
        .filter(
        col("t.canceled").isNull()
    ).select(
        lit(busi_date).alias("BUSI_DATE"),
        col("t.departmentid").alias("OA_BRANCH_ID")
    )

    return_to_hive(
        spark=spark,
        df_result=df_d,
        target_table="ddw.T_COCKPIT_BUSI_ANALYSE_D",
        insert_mode="overwrite"
    )

    df_d = spark.table("ddw.T_COCKPIT_BUSI_ANALYSE_D").filter(
        col("BUSI_DATE") == busi_date
    )

    """
    更新数据
        业务结构-期末权益-存量客户
        业务结构-期末权益-存量客户占比
        业务结构-期末权益-新增客户     新增按照当年1月1日，新增数据
        业务结构-期末权益-新增客户占比
        业务指标-期末权益
        业务指标-期末权益同比
    """
    logger.info(to_color_str("更新数据", "blue"))

    tmp_new = spark.table("edw.h15_client_sett").alias("t") \
        .filter(
        col("busi_date") == busi_date
    ).join(
        other=spark.table("edw.h12_fund_account").alias("b"),
        on=col("t.fund_account_id") == col("b.fund_account_id"),
        how="left"
    ).join(
        other=spark.table("ddw.t_ctp_branch_oa_rela").alias("c"),
        on=col("b.branch_id") == col("c.ctp_branch_id"),
        how="inner"
    ).filter(
        col("c.oa_branch_id").isNotNull()
    ).groupBy(
        "t.fund_account_id",
        (col("b.open_date").between(v_new_begin_date, v_new_end_date)).alias("is_new_flag"),
        "c.oa_branch_id"
    ).agg(
        sum("t.rights").alias("end_rights")
    ).select(
        col("t.fund_account_id"),
        col("c.oa_branch_id"),
        col("end_rights"),
        col("is_new_flag")
    )

    tmp_yoy = spark.table("edw.h15_client_sett").alias("t").filter(
        col("t.busi_date") == v_yoy_busi_date
    ).join(
        other=spark.table("edw.h12_fund_account").alias("b"),
        on=col("t.fund_account_id") == col("b.fund_account_id"),
        how="left"
    ).join(
        other=spark.table("ddw.t_ctp_branch_oa_rela").alias("c"),
        on=col("b.branch_id") == col("c.ctp_branch_id"),
        how="inner"
    ).filter(
        col("c.oa_branch_id").isNotNull()
    ).groupBy(
        "t.fund_account_id",
        "c.oa_branch_id"
    ).agg(
        sum("t.rights").alias("end_rights")
    ).select(
        col("t.fund_account_id"),
        col("c.oa_branch_id"),
        col("end_rights")
    )

    tmp_result = tmp_new.alias("t") \
        .groupBy(
        col("t.oa_branch_id")
    ).agg(
        sum("t.end_rights").alias("total_end_rights"),
        sum(when(
            col("t.is_new_flag") == "1",
            col("t.end_rights")
        ).otherwise(0)).alias("new_end_rights"),
        sum(when(
            col("t.is_new_flag") == "0",
            col("t.end_rights")
        ).otherwise(0)).alias("stock_end_rights")
    ).select(
        col("t.oa_branch_id"),
        col("total_end_rights"),
        col("new_end_rights"),
        col("stock_end_rights")
    )

    tmp_result1 = tmp_yoy.alias("t") \
        .groupBy(
        col("t.oa_branch_id")
    ).agg(
        sum("t.end_rights").alias("total_end_rights")
    ).select(
        col("t.oa_branch_id"),
        col("total_end_rights")
    )

    df_y = tmp_result.alias("t") \
        .join(
        other=tmp_result1.alias("a"),
        on=col("t.oa_branch_id") == col("a.oa_branch_id"),
        how="left"
    ).select(
        col("t.oa_branch_id"),
        col("t.stock_end_rights"),
        when(
            col("t.total_end_rights") != 0,
            col("t.stock_end_rights") / col("t.total_end_rights")
        ).otherwise(0).alias("STOCK_END_RIGHTS_PROP"),
        col("t.new_end_rights"),
        when(
            col("t.total_end_rights") != 0,
            col("t.new_end_rights") / col("t.total_end_rights")
        ).otherwise(0).alias("NEW_END_RIGHTS_PROP"),
        col("t.total_end_rights").alias("END_RIGHTS"),
        when(
            col("a.total_end_rights") != 0,
            col("t.total_end_rights") / col("a.total_end_rights") - 1
        ).otherwise(0).alias("END_RIGHTS_YOY")
    )

    df_d = update_dataframe(
        df_to_update=df_d,
        df_use_me=df_y,
        join_columns=["OA_BRANCH_ID"],
        update_columns=[
            "STOCK_END_RIGHTS",
            "STOCK_END_RIGHTS_PROP",
            "NEW_END_RIGHTS",
            "NEW_END_RIGHTS_PROP",
            "END_RIGHTS",
            "END_RIGHTS_YOY"
        ]
    )

    return_to_hive(
        spark=spark,
        df_result=df_d,
        target_table="ddw.T_COCKPIT_BUSI_ANALYSE_D",
        insert_mode="overwrite",
    )
