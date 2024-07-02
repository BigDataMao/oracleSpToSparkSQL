# -*- coding: utf-8 -*-

import logging

from pyspark.sql.functions import col, lit, sum, when

from config import Config
from utils.date_utils import *
from utils.io_utils.common_uitls import to_color_str
from utils.task_env import return_to_hive, update_dataframe

logger = Config().get_logger()


def p_cockpit_busi_anal_respons_d(spark, busi_date):
    """
    经营分析-分管部门-单日期落地
    :param spark: SparkSession对象
    :param busi_date: 业务日期,格式为"YYYYMMDD"
    :return: None
    """
    logger.info("p_cockpit_busi_anal_respons_d执行开始")

    v_new_begin_date = busi_date[:4] + "0101"
    v_new_end_date = busi_date

    v_yoy_busi_date = get_previous_year_date(busi_date)

    v_yoy_busi_date = get_date_period_and_days(
        spark=spark,
        end_date=v_yoy_busi_date,
        is_trade_day=True
    )[1]

    # 初始化数据
    df_d = spark.table("ddw.t_Respons_Line").alias("t") \
        .filter(col("t.if_use") == "1") \
        .select(
        lit(busi_date).alias("busi_date"),
        col("t.RESPONS_LINE_ID")
    )

    return_to_hive(
        spark=spark,
        df_result=df_d,
        target_table="ddw.T_COCKPIT_BUSI_ANAL_RESPONS_D",
        insert_mode="overwrite",
        partition_column="busi_date",
        partition_value=busi_date
    )

    df_d = spark.table("ddw.T_COCKPIT_BUSI_ANAL_RESPONS_D").alias("a") \
        .filter(col("a.busi_date") == busi_date)

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
        (col("t.busi_date") == busi_date)
    ).join(
        other=spark.table("edw.h12_fund_account").alias("b"),
        on=col("t.fund_account_id") == col("b.fund_account_id"),
        how="left"
    ).join(
        other=spark.table("ddw.t_ctp_branch_oa_rela").alias("c"),
        on=col("b.branch_id") == col("c.ctp_branch_id"),
        how="inner"
    ).join(
        other=spark.table("ddw.T_OA_BRANCH").alias("d"),
        on=col("c.oa_branch_id") == col("d.departmentid"),
        how="inner"
    ).filter(
        col("d.RESPONS_LINE_ID").isNotNull()
    ).groupBy(
        col("t.fund_account_id"),
        when(
            (col("b.open_date").between(v_new_begin_date, v_new_end_date)),
            lit(1)
        ).otherwise(lit(0)).alias("is_new_flag"),
        col("d.RESPONS_LINE_ID")
    ).agg(
        sum("t.rights").alias("end_rights")
    ).select(
        col("t.fund_account_id"),
        col("is_new_flag"),
        col("d.RESPONS_LINE_ID"),
        col("end_rights")
    )

    tmp_yoy = spark.table("edw.h15_client_sett").alias("t") \
        .filter(
        (col("t.busi_date") == lit(v_yoy_busi_date))
    ).join(
        other=spark.table("edw.h12_fund_account").alias("b"),
        on=col("t.fund_account_id") == col("b.fund_account_id"),
        how="left"
    ).join(
        other=spark.table("ddw.t_ctp_branch_oa_rela").alias("c"),
        on=col("b.branch_id") == col("c.ctp_branch_id"),
        how="inner"
    ).join(
        other=spark.table("ddw.T_OA_BRANCH").alias("d"),
        on=col("c.oa_branch_id") == col("d.departmentid"),
        how="inner"
    ).filter(
        col("d.RESPONS_LINE_ID").isNotNull()
    ).groupBy(
        col("t.fund_account_id"),
        col("d.RESPONS_LINE_ID")
    ).agg(
        sum("t.rights").alias("end_rights")
    ).select(
        col("t.fund_account_id"),
        col("d.RESPONS_LINE_ID"),
        col("end_rights")
    )

    tmp_result = tmp_new.alias("t") \
        .groupBy(
        col("t.RESPONS_LINE_ID")
    ).agg(
        sum("t.end_rights").alias("total_end_rights"),
        sum(
            when(
                col("t.is_new_flag") == "1",
                col("t.end_rights")
            ).otherwise(0)
        ).alias("new_end_rights"),
        sum(
            when(
                col("t.is_new_flag") == "0",
                col("t.end_rights")
            ).otherwise(0)
        ).alias("stock_end_rights")
    ).select(
        col("t.RESPONS_LINE_ID"),
        col("total_end_rights"),
        col("new_end_rights"),
        col("stock_end_rights")
    )

    tmp_result1 = tmp_yoy.alias("t") \
        .groupBy(
        col("t.RESPONS_LINE_ID")
    ).agg(
        sum("t.end_rights").alias("total_end_rights")
    ).select(
        col("t.RESPONS_LINE_ID"),
        col("total_end_rights")
    )

    df_y = tmp_result.alias("t") \
        .join(
        other=tmp_result1.alias("a"),
        on=col("t.RESPONS_LINE_ID") == col("a.RESPONS_LINE_ID"),
        how="left"
    ).select(
        col("t.RESPONS_LINE_ID"),
        (col("t.stock_end_rights") / 10000).alias("stock_end_rights"),
        when(
            col("t.total_end_rights") != 0,
            col("t.stock_end_rights") / col("t.total_end_rights") * 100
        ).otherwise(0).alias("stock_end_rights_prop"),
        (col("t.new_end_rights") / 10000).alias("new_end_rights"),
        when(
            col("t.total_end_rights") != 0,
            col("t.new_end_rights") / col("t.total_end_rights") * 100
        ).otherwise(0).alias("new_end_rights_prop"),
        (col("t.total_end_rights") / 10000).alias("end_rights"),
        when(
            col("a.total_end_rights") != 0,
            (col("t.total_end_rights") / col("a.total_end_rights") - 1) * 100
        ).otherwise(0).alias("end_rights_yoy")
    )

    # 更新数据
    update_dataframe(
        df_to_update=df_d,
        df_use_me=df_y,
        join_columns=["RESPONS_LINE_ID"],
        update_columns=[
            "stock_end_rights",
            "stock_end_rights_prop",
            "new_end_rights",
            "new_end_rights_prop",
            "end_rights",
            "end_rights_yoy"
        ]
    )

    return_to_hive(
        spark=spark,
        df_result=df_d,
        target_table="ddw.T_COCKPIT_BUSI_ANAL_RESPONS_D",
        insert_mode="overwrite"
    )

    logger.info(to_color_str("p_cockpit_busi_anal_respons_d计算完成", "green"))
    logger.info("本次任务为:经营分析-分管部门-单日期落地")
