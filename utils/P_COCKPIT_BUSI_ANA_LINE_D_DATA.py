# *# -*- coding: utf-8 -*-
import logging

from pyspark.sql.functions import col, lit, sum, when

from config import Config
from utils.date_utils import get_previous_year_date, get_date_period_and_days
from utils.task_env import return_to_hive, update_dataframe

logger = Config().get_logger()


def p_cockpit_busi_ana_line_d_data(spark, busi_date):
    """
    经营分析-业务条线-单日期落地
    :param spark: SparkSession对象
    :param busi_date: 业务日期,格式为"YYYYMMDD"
    :return: None
    """
    logger.info("p_cockpit_busi_ana_line_d_data执行开始")

    # 当年第一天
    v_new_begin_date = busi_date[:4] + "0101"
    v_new_end_date = busi_date
    # 同比日期
    v_yoy_busi_date = get_previous_year_date(busi_date)
    # 同比日期前的最大交易日
    v_yoy_busi_date = get_date_period_and_days(
        spark=spark,
        end_date=v_yoy_busi_date,
        is_trade_day=True
    )[1]

    """
    初始化数据
    """
    logger.info("\033[91m初始化数据\033[0m")

    df_x = spark.table("ddw.T_business_line").alias("t") \
        .filter(
        col("t.if_use") == "1"
    ).select(
        lit(busi_date).alias("BUSI_DATE"),
        col("t.business_line_id")
    )

    return_to_hive(
        spark=spark,
        df_result=df_x,
        target_table="ddw.T_COCKPIT_BUSI_ANALYSE_LINE_D",
        insert_mode="overwrite",
    )

    df_x = spark.table("ddw.T_COCKPIT_BUSI_ANALYSE_LINE_D").filter(
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
    ).join(
        other=spark.table("ddw.T_OA_BRANCH").alias("d").filter(
            col("d.Business_Line_Id").isNotNull()
        ),
        on=col("c.oa_branch_id") == col("d.departmentid"),
        how="inner"
    ).groupBy(
        col("t.fund_account_id"),
        col("d.Business_Line_Id"),
        when(
            col("b.open_date").between(v_new_begin_date, v_new_end_date),
            lit("1")
        ).otherwise(lit("0")).alias("is_new_flag")
    ).agg(
        sum("t.rights").alias("end_rights"),
    ).select(
        col("t.fund_account_id"),
        col("is_new_flag"),
        col("d.Business_Line_Id"),
        col("end_rights")
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
    ).join(
        other=spark.table("ddw.T_OA_BRANCH").alias("d").filter(
            col("d.Business_Line_Id").isNotNull()
        ),
        on=col("c.oa_branch_id") == col("d.departmentid"),
        how="inner"
    ).groupBy(
        col("t.fund_account_id"),
        col("d.Business_Line_Id")
    ).agg(
        sum("t.rights").alias("end_rights"),
    ).select(
        col("t.fund_account_id"),
        col("d.Business_Line_Id"),
        col("end_rights")
    )

    tmp_result = tmp_new.alias("t") \
        .groupBy(
        col("t.Business_Line_Id")
    ).agg(
        sum("t.end_rights").alias("total_end_rights"),
        sum(when(col("t.is_new_flag") == 1, col("t.end_rights")).otherwise(0)).alias("new_end_rights"),
        sum(when(col("t.is_new_flag") == 0, col("t.end_rights")).otherwise(0)).alias("stock_end_rights")
    ).select(
        col("t.Business_Line_Id"),
        col("total_end_rights"),
        col("new_end_rights"),
        col("stock_end_rights")
    )

    tmp_result1 = tmp_yoy.alias("t") \
        .groupBy(
        col("t.Business_Line_Id")
    ).agg(
        sum("t.end_rights").alias("total_end_rights")
    ).select(
        col("t.Business_Line_Id"),
        col("total_end_rights")
    )

    df_y = tmp_result.alias("t") \
        .join(
        other=tmp_result1.alias("a"),
        on=col("t.Business_Line_Id") == col("a.Business_Line_Id"),
        how="left"
    ).select(
        col("t.Business_Line_Id"),
        (col("t.stock_end_rights") / 10000).alias("STOCK_END_RIGHTS"),
        when(
            col("t.total_end_rights") != 0,
            col("t.stock_end_rights") / col("t.total_end_rights") * 100
        ).otherwise(0).alias("STOCK_END_RIGHTS_PROP"),
        (col("t.new_end_rights") / 10000).alias("NEW_END_RIGHTS"),
        when(
            col("t.total_end_rights") != 0,
            col("t.new_end_rights") / col("t.total_end_rights") * 100
        ).otherwise(0).alias("NEW_END_RIGHTS_PROP"),
        (col("t.total_end_rights") / 10000).alias("END_RIGHTS"),
        when(
            col("a.total_end_rights") != 0,
            (col("t.total_end_rights") / col("a.total_end_rights") - 1) * 100
        ).otherwise(0).alias("END_RIGHTS_YOY")
    )

    update_dataframe(
        df_to_update=df_x,
        df_use_me=df_y,
        join_columns=["Business_Line_Id"],
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
        df_result=df_x,
        target_table="ddw.T_COCKPIT_BUSI_ANALYSE_LINE_D",
        insert_mode="overwrite"
    )

    logger.info("p_cockpit_busi_ana_line_d_data执行完成")
    logger.info("本次任务为:经营分析-业务条线-单日期落地")

