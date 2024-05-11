# *# -*- coding: utf-8 -*-
import logging
from datetime import datetime

from pyspark.sql.functions import col, lit, sum, when, coalesce

from utils.date_utils import get_previous_year_date, get_date_period_and_days
from utils.io_utils.common_uitls import to_color_str
from utils.task_env import return_to_hive, update_dataframe

logger = logging.getLogger("logger")


def p_cockpit_busi_ana_line_m_data(spark, busi_date):
    """
    经营分析-业务条线-按月落地
    :param spark: SparkSession对象
    :param busi_date: 业务日期,格式为"YYYYMMDD"
    :return: None
    """
    logger.info("p_cockpit_busi_ana_line_m_data执行开始")

    v_begin_date = busi_date[:4] + "0101"
    i_month_id = busi_date[:6]
    # 获取当前日期字符串
    datetime_str = datetime.now().strftime("%Y%m%d")
    (
        v_end_date,
        v_now_begin_date,
        v_busi_trade_days
    ) = get_date_period_and_days(
        spark=spark,
        busi_month=i_month_id,
        end_date=datetime_str,
        is_trade_day=True
    )

    _, _, v_busi_trade_days = get_date_period_and_days(
        spark=spark,
        begin_date=v_begin_date,
        end_date=v_end_date,
        is_trade_day=True
    )

    v_new_begin_date = i_month_id + "01"
    v_new_end_date = v_end_date
    v_now_end_date = v_end_date

    # TODO: CF_BUSIMG.T_COCKPIT_BUSI_ANALYSE_LINE_M,分区字段,busi_month

    # 初始化数据
    logger.info(to_color_str("初始化数据", "red"))

    df_m = spark.table("ddw.T_business_line").alias("t") \
        .filter(
        col("t.if_use") == "1"
    ).select(
        lit(i_month_id).alias("BUSI_MONTH"),
        col("t.business_line_id"),
    )

    # 必须先写回hive表再重新读,否则没有完整元数据信息
    return_to_hive(
        spark=spark,
        df_result=df_m,
        target_table="ddw.T_COCKPIT_BUSI_ANALYSE_LINE_M",
        insert_mode="overwrite"
    )

    df_m = spark.table("ddw.T_COCKPIT_BUSI_ANALYSE_LINE_M") \
        .filter(
        col("BUSI_MONTH") == i_month_id
    )

    """
    财务指标
    收入结构
    业务指标-日均权益
    """
    logger.info(to_color_str("财务指标", "red"))

    tmp_new = spark.table("edw.h15_client_sett").alias("t") \
        .filter(
        (col("t.busi_date").between(v_begin_date, v_end_date))
    ).join(
        other=spark.table("edw.h12_fund_account").alias("b"),
        on=(col("t.fund_account_id") == col("b.fund_account_id")),
        how="left"
    ).join(
        other=spark.table("ddw.T_CTP_BRANCH_OA_RELA").alias("c"),
        on=(col("b.branch_id") == col("c.ctp_branch_id")),
        how="inner"
    ).join(
        other=spark.table("ddw.T_OA_BRANCH").alias("d"),
        on=(col("c.oa_branch_id") == col("d.departmentid")),
        how="inner"
    ).filter(
        col("d.business_line_id").isNotNull()
    ).groupBy(
        "t.fund_account_id",
        when(
            (col("b.open_date").between(v_new_begin_date, v_new_end_date)),
            lit("1")
        ).otherwise(lit("0")).alias("is_new_flag"),
        col("d.business_line_id")
    ).agg(
        sum("t.rights").alias("sum_rights")
    ).select(
        col("t.fund_account_id"),
        col("is_new_flag"),
        col("d.business_line_id"),
        col("sum_rights")
    )

    tmp_result = tmp_new.alias("t") \
        .select(
        col("t.business_line_id"),
        when(
            (col("t.is_new_flag") == "1") & (lit(v_busi_trade_days) != 0),
            col("t.sum_rights") / v_busi_trade_days
        ).otherwise(lit(0)).alias("AVG_RIGHTS_NEW"),
        when(
            (col("t.is_new_flag") == "0") & (lit(v_busi_trade_days) != 0),
            col("t.sum_rights") / v_busi_trade_days
        ).otherwise(lit(0)).alias("AVG_RIGHTS_STOCK"),
        when(
            (lit(v_busi_trade_days) != 0),
            col("t.sum_rights") / v_busi_trade_days
        ).otherwise(lit(0)).alias("sum_avg_rights")
    )

    df_y = tmp_result.alias("t") \
        .select(
        col("t.business_line_id"),
        col("t.AVG_RIGHTS_STOCK"),
        when(
            (col("t.sum_avg_rights") != 0),
            col("t.AVG_RIGHTS_STOCK") / col("t.sum_avg_rights")
        ).otherwise(lit(0)).alias("AVG_RIGHTS_STOCK_PROP"),
        col("t.AVG_RIGHTS_NEW"),
        when(
            (col("t.sum_avg_rights") != 0),
            col("t.AVG_RIGHTS_NEW") / col("t.sum_avg_rights")
        ).otherwise(lit(0)).alias("AVG_RIGHTS_NEW_PROP")
    )

    # 更新数据
    update_dataframe(
        df_to_update=df_m,
        df_use_me=df_y,
        join_columns=["business_line_id"],
        update_columns=[
            "AVG_RIGHTS_STOCK",
            "AVG_RIGHTS_STOCK_PROP",
            "AVG_RIGHTS_NEW",
            "AVG_RIGHTS_NEW_PROP"
        ]
    )

    # 写回hive,再重读分区
    return_to_hive(
        spark=spark,
        df_result=df_m,
        target_table="ddw.T_COCKPIT_BUSI_ANALYSE_LINE_M",
        insert_mode="overwrite"
    )

    df_m = spark.table("ddw.T_COCKPIT_BUSI_ANALYSE_LINE_M").filter(
        col("BUSI_MONTH") == i_month_id
    )

    """
    业务指标-成交量
    业务指标-成交额
    """
    logger.info(to_color_str("业务指标-成交量,成交额", "red"))

    tmp_new = spark.table("edw.h15_hold_balance").alias("t") \
        .filter(
        (col("t.busi_date").between(v_begin_date, v_end_date))
    ).join(
        other=spark.table("edw.h12_fund_account").alias("b"),
        on=(col("t.fund_account_id") == col("b.fund_account_id")),
        how="left"
    ).join(
        other=spark.table("ddw.T_CTP_BRANCH_OA_RELA").alias("c"),
        on=(col("b.branch_id") == col("c.ctp_branch_id")),
        how="inner"
    ).join(
        other=spark.table("ddw.T_OA_BRANCH").alias("d"),
        on=(col("c.oa_branch_id") == col("d.departmentid")),
        how="inner"
    ).filter(
        col("d.business_line_id").isNotNull()
    ).groupBy(
        "t.fund_account_id",
        when(
            (col("b.open_date").between(v_new_begin_date, v_new_end_date)),
            lit("1")
        ).otherwise(lit("0")).alias("is_new_flag"),
        col("d.business_line_id")
    ).agg(
        sum("t.done_amt").alias("sum_done_amt"),
        sum("t.done_sum").alias("sum_done_sum")
    ).select(
        col("t.fund_account_id"),
        col("is_new_flag"),
        col("d.business_line_id"),
        col("sum_done_amt"),
        col("sum_done_sum")
    )

    tmp_result = tmp_new.alias("t") \
        .select(
        col("t.business_line_id"),
        when(
            (col("t.is_new_flag") == "1") & (lit(v_busi_trade_days) != 0),
            col("t.sum_done_amt")
        ).otherwise(lit(0)).alias("DONE_AMOUNT_NEW"),
        when(
            (col("t.is_new_flag") == "0") & (lit(v_busi_trade_days) != 0),
            col("t.sum_done_amt")
        ).otherwise(lit(0)).alias("DONE_AMOUNT_STOCK"),
        when(
            (col("t.is_new_flag") == "1") & (lit(v_busi_trade_days) != 0),
            col("t.sum_done_sum")
        ).otherwise(lit(0)).alias("DONE_MONEY_NEW"),
        when(
            (col("t.is_new_flag") == "0") & (lit(v_busi_trade_days) != 0),
            col("t.sum_done_sum")
        ).otherwise(lit(0)).alias("DONE_MONEY_STOCK"),
        col("t.sum_done_amt").alias("SUM_DONE_AMOUNT"),
        col("t.sum_done_sum").alias("SUM_DONE_MONEY")
    )

    df_y = tmp_result.alias("t") \
        .select(
        col("t.business_line_id"),
        col("t.DONE_AMOUNT_STOCK"),
        when(
            (col("t.SUM_DONE_AMOUNT") != 0),
            col("t.DONE_AMOUNT_STOCK") / col("t.SUM_DONE_AMOUNT")
        ).otherwise(lit(0)).alias("DONE_AMOUNT_STOCK_PROP"),
        col("t.DONE_AMOUNT_NEW"),
        when(
            (col("t.SUM_DONE_AMOUNT") != 0),
            col("t.DONE_AMOUNT_NEW") / col("t.SUM_DONE_AMOUNT")
        ).otherwise(lit(0)).alias("DONE_AMOUNT_NEW_PROP"),
        col("t.DONE_MONEY_STOCK"),
        when(
            (col("t.SUM_DONE_MONEY") != 0),
            col("t.DONE_MONEY_STOCK") / col("t.SUM_DONE_MONEY")
        ).otherwise(lit(0)).alias("DONE_MONEY_STOCK_PROP"),
        col("t.DONE_MONEY_NEW"),
        when(
            (col("t.SUM_DONE_MONEY") != 0),
            col("t.DONE_MONEY_NEW") / col("t.SUM_DONE_MONEY")
        ).otherwise(lit(0)).alias("DONE_MONEY_NEW_PROP")
    )

    # 更新数据
    update_dataframe(
        df_to_update=df_m,
        df_use_me=df_y,
        join_columns=["business_line_id"],
        update_columns=[
            "DONE_AMOUNT_STOCK",
            "DONE_AMOUNT_STOCK_PROP",
            "DONE_AMOUNT_NEW",
            "DONE_AMOUNT_NEW_PROP",
            "DONE_MONEY_STOCK",
            "DONE_MONEY_STOCK_PROP",
            "DONE_MONEY_NEW",
            "DONE_MONEY_NEW_PROP"
        ]
    )

    # 写回hive,再重读分区
    return_to_hive(
        spark=spark,
        df_result=df_m,
        target_table="ddw.T_COCKPIT_BUSI_ANALYSE_LINE_M",
        insert_mode="overwrite"
    )

    df_m = spark.table("ddw.T_COCKPIT_BUSI_ANALYSE_LINE_M").filter(
        col("BUSI_MONTH") == i_month_id
    )

    """
    市场成交量，成交额
    取中期协月度交易数据
    """
    logger.info(to_color_str("市场成交量,成交额", "red"))

    (
        v_total_done_amount,
        v_total_done_money
    ) = spark.table("ddw.T_COCKPIT_INDUSTRY_TRAD").alias("t") \
        .filter(
        col("t.etl_month") == i_month_id
    ).agg(
        coalesce(sum(col("t.trad_num")), lit(0)) * 2,
        coalesce(sum(col("t.trad_amt")), lit(0)) * 2 * 100000000
    ).collect()[0]

    """
    市场客户权益-日均
    """

    tmp = spark.table("ddw.T_COCKPIT_INDUSTRY_MANAGE").alias("t") \
        .filter(
        (col("t.etl_month") == i_month_id) &
        (col("t.index_name") == "客户权益")
    ).agg(
        (coalesce(sum(col("t.index_value")), lit(0)) * 100000000).alias("rights")
    ).select(
        col("rights")
    )

    v_total_rights = tmp.alias("t") \
        .select(
        when(
            (lit(lit(v_busi_trade_days)) != 0),
            col("t.rights") / lit(v_busi_trade_days)
        ).otherwise(lit(0)).alias("rights")
    ).first()["rights"]

    """
    市场手续费收入
    """

    v_total_index_value = spark.table("ddw.T_COCKPIT_INDUSTRY_MANAGE").alias("t") \
        .filter(
        (col("t.etl_month") == i_month_id) &
        (col("t.index_name") == "手续费收入")
    ).agg(
        (coalesce(sum(col("t.index_value")), lit(0)) * 100000000).alias("index_value")
    ).select(
        col("index_value")
    ).first()["index_value"]

    """
    市场地位 --日均权益，手续费
    """

    tmp = spark.table("edw.h15_client_sett").alias("t") \
        .filter(
        (col("t.busi_date").between(v_now_begin_date, v_now_end_date))
    ).join(
        other=spark.table("edw.h12_fund_account").alias("b"),
        on=(col("t.fund_account_id") == col("b.fund_account_id")),
        how="left"
    ).join(
        other=spark.table("ddw.T_CTP_BRANCH_OA_RELA").alias("c"),
        on=(col("b.branch_id") == col("c.ctp_branch_id")),
        how="inner"
    ).join(
        other=spark.table("ddw.T_OA_BRANCH").alias("d"),
        on=(col("c.oa_branch_id") == col("d.departmentid")),
        how="inner"
    ).filter(
        col("d.business_line_id").isNotNull()
    ).groupBy(
        col("t.fund_account_id"),
        col("d.business_line_id")
    ).agg(
        sum("t.rights").alias("rights"),
        sum(
            coalesce(col("t.transfee"), lit(0)) +
            coalesce(col("t.delivery_transfee"), lit(0)) +
            coalesce(col("t.strikefee"), lit(0))
        ).alias("transfee")
    ).select(
        col("t.fund_account_id"),
        col("d.business_line_id"),
        col("rights"),
        col("transfee")
    )

    tmp1 = tmp.alias("t") \
        .groupBy(
        col("t.business_line_id")
    ).agg(
        sum(
            when(
                (lit(v_busi_trade_days) != 0),
                col("t.rights") / lit(v_busi_trade_days)
            ).otherwise(lit(0))
        ).alias("AVG_RIGHTS"),
        sum(col("t.transfee")).alias("TRANSFEE")
    ).select(
        col("t.business_line_id"),
        col("AVG_RIGHTS"),
        col("TRANSFEE")
    )

    df_y = tmp1.alias("t") \
        .select(
        col("t.business_line_id"),
        when(
            (lit(v_total_rights) != 0),
            col("t.AVG_RIGHTS") / v_total_rights
        ).otherwise(lit(0)).alias("AVG_RIGHTS_MARKET_RATE"),
        when(
            (lit(v_total_index_value) != 0),
            col("t.TRANSFEE") / v_total_index_value
        ).otherwise(lit(0)).alias("FUTU_TRANS_INCOME_MARKET_RATE")
    )

    # 更新数据
    update_dataframe(
        df_to_update=df_m,
        df_use_me=df_y,
        join_columns=["business_line_id"],
        update_columns=[
            "AVG_RIGHTS_MARKET_RATE",
            "FUTU_TRANS_INCOME_MARKET_RATE"
        ]
    )

    # 写回hive,再重读分区
    return_to_hive(
        spark=spark,
        df_result=df_m,
        target_table="ddw.T_COCKPIT_BUSI_ANALYSE_LINE_M",
        insert_mode="overwrite"
    )

    df_m = spark.table("ddw.T_COCKPIT_BUSI_ANALYSE_LINE_M").filter(
        col("BUSI_MONTH") == i_month_id
    )

    """
    市场地位 --日均权益，手续费
    """
    logger.info(to_color_str("市场地位 --日均权益，手续费", "red"))

    tmp = spark.table("edw.h15_hold_balance").alias("t") \
        .filter(
        (col("t.busi_date").between(v_now_begin_date, v_now_end_date))
    ).join(
        other=spark.table("edw.h12_fund_account").alias("b"),
        on=(col("t.fund_account_id") == col("b.fund_account_id")),
        how="left"
    ).join(
        other=spark.table("ddw.T_CTP_BRANCH_OA_RELA").alias("c"),
        on=(col("b.branch_id") == col("c.ctp_branch_id")),
        how="inner"
    ).join(
        other=spark.table("ddw.T_OA_BRANCH").alias("d"),
        on=(col("c.oa_branch_id") == col("d.departmentid")),
        how="inner"
    ).filter(
        col("d.business_line_id").isNotNull()
    ).groupBy(
        col("t.fund_account_id"),
        col("d.business_line_id")
    ).agg(
        sum("t.done_amt").alias("done_amount"),
        sum("t.done_sum").alias("done_money")
    ).select(
        col("t.fund_account_id"),
        col("d.business_line_id"),
        col("done_amount"),
        col("done_money")
    )

    tmp1 = tmp.alias("t") \
        .groupBy(
        col("t.business_line_id")
    ).agg(
        sum(col("t.done_amount")).alias("done_amount"),
        sum(col("t.done_money")).alias("done_money")
    ).select(
        col("t.business_line_id"),
        col("done_amount"),
        col("done_money")
    )

    df_y = tmp1.alias("t") \
        .select(
        col("t.business_line_id"),
        when(
            (lit(v_total_done_amount) != 0),
            col("t.done_amount") / v_total_done_amount
        ).otherwise(lit(0)).alias("DONE_AMOUNT_MARKET_RATE"),
        when(
            (lit(v_total_done_money) != 0),
            col("t.done_money") / v_total_done_money
        ).otherwise(lit(0)).alias("DONE_MONEY_MAREKT_RATE")
    )

    # 更新数据
    update_dataframe(
        df_to_update=df_m,
        df_use_me=df_y,
        join_columns=["business_line_id"],
        update_columns=[
            "DONE_AMOUNT_MARKET_RATE",
            "DONE_MONEY_MAREKT_RATE"
        ]
    )

    logger.info("p_cockpit_busi_ana_line_m_data执行完成")
    logger.info("本次任务为: 经营分析-业务条线-按月落地")
