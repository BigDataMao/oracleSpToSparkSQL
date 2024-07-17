# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, sum, lit, regexp_replace, when, row_number

from config import Config
from utils.date_utils import get_date_period_and_days
from utils.io_utils.common_uitls import to_color_str
from utils.task_env import log, return_to_hive

logger = Config().get_logger()


@log
def p_cockpit_00179_data(spark: SparkSession, i_month_id):
    """
    驻点人员营销统计数据表（最终呈现表）-落地数据
    :param spark: SparkSession对象
    :param i_month_id: 月份ID,格式为"YYYYMM"
    :return:
    """

    v_begin_trade_date, v_end_trade_date, v_trade_days = get_date_period_and_days(
        spark=spark, busi_month=i_month_id, is_trade_day=True
    )

    v_begin_date, v_end_date = get_date_period_and_days(
        spark=spark, busi_month=i_month_id, is_trade_day=False
    )[:2]

    # 根据查询日期获取业务人员关系数据
    logger.info("根据查询日期获取业务人员关系数据")

    df_tmp_110_1 = spark.table("ods.T_DS_CRM_BROKER_INVESTOR_RELA").alias("a") \
        .join(
        spark.table("ods.T_DS_CRM_BROKER").alias("b"),
        col("a.broker_id") == col("b.broker_id")
    ).join(
        spark.table("ods.T_DS_MDP_DEPT00").alias("f"),
        col("b.department_id") == col("f.chdeptcode")
    ).join(
        spark.table("ods.T_DS_DC_INVESTOR").alias("c"),
        col("a.investor_id") == col("c.investor_id")
    ).join(
        spark.table("edw.h12_fund_account").alias("x"),
        col("c.investor_id") == col("x.fund_account_id"),
        "left"
    ).join(
        spark.table("edw.h11_branch").alias("x1"),
        col("x.branch_id") == col("x1.branch_id"),
        "left"
    ).join(
        spark.table("ddw.t_ctp_branch_oa_rela").alias("x2"),
        col("x1.branch_id") == col("x2.ctp_branch_id"),
        "left"
    ).where(
        (col("b.broker_id").like("ZD%")) &
        (col("a.RELA_STS") == "A") &
        (col("a.APPROVE_STS") == "0")
    ).select(
        col("a.broker_id"),
        col("b.broker_nam").alias("broker_name"),
        col("a.investor_id").alias("fund_account_id"),
        col("c.investor_nam").alias("client_name"),
        regexp_replace(col("a.ST_DT"), "-", "").alias("begin_date"),
        regexp_replace(col("a.END_DT"), "-", "").alias("end_date"),
        when(
            col("a.BROKER_RELA_TYP") == "301", "居间关系"
        ).when(
            col("a.BROKER_RELA_TYP") == "001", "开发关系"
        ).when(
            col("a.BROKER_RELA_TYP") == "002", "服务关系"
        ).when(
            col("a.BROKER_RELA_TYP") == "003", "维护关系"
        ).otherwise("-").alias("broker_rela_type"),
        col("a.data_pct"),
        when(
            col("a.RELA_STS") == "A", "有效"
        ).when(
            col("a.RELA_STS") == "S", "停止使用"
        ).otherwise("-").alias("rela_status"),
        col("a.APPROVE_DT"),
        when(
            col("a.APPROVE_STS") == "0", "审核通过"
        ).when(
            col("a.APPROVE_STS") == "1", "审核不通过"
        ).when(
            col("a.APPROVE_STS") == "2", "等待审核"
        ).otherwise("-").alias("approve_sts"),
        col("a.comment_desc"),
        col("a.check_comments"),
        when(
            col("a.ST_DT") < lit(v_begin_date), lit(v_begin_date)
        ).when(
            col("a.ST_DT") >= lit(v_begin_date), col("a.ST_DT")
        ).otherwise("").alias("real_begin_date"),
        when(
            col("a.END_DT") <= lit(v_end_date), col("a.END_DT")
        ).when(
            col("a.END_DT") > lit(v_end_date), lit(v_end_date)
        ).otherwise("").alias("real_end_date")
    ).alias("t").where(
        (col("t.real_begin_date") <= col("t.real_end_date")) &
        (regexp_replace(col("t.approve_dt"), "-", "").substr(1, 10) <= lit(v_end_date))
    )

    # 获取有关系的客户数
    logger.info("获取有关系的客户数")

    df_tmp_110_2 = df_tmp_110_1.select(
        col("fund_account_id"),
        col("real_begin_date"),
        col("real_end_date")
    ).dropDuplicates(
        ["fund_account_id", "real_begin_date", "real_end_date"]
    )

    # 按照日期计算 期末权益  日均权益
    logger.info(to_color_str("按照日期计算 期末权益  日均权益", "blue"))

    df_tmp_110_3 = spark.table("edw.h15_client_sett").alias("t") \
        .join(
        df_tmp_110_2.alias("a"),
        (col("t.fund_account_id") == col("a.fund_account_id")) &
        (col("t.busi_date").between(col("a.real_begin_date"), col("a.real_end_date")))
    ).where(
        col("t.busi_date").between(lit(v_begin_date), lit(v_end_date))
    ).groupBy(
        col("t.fund_account_id")
    ).agg(
        sum(
            when(
                col("t.busi_date") == lit(v_end_trade_date), col("t.rights")
            ).otherwise(0)
        ).alias("end_rights"),
        sum(
            when(
                col("t.busi_date").between(lit(v_begin_date), lit(v_end_date)), col("t.rights")
            ).otherwise(0)
        ).alias("avg_rights")
    )

    # 按照日期计算 成交手数 成交金额
    logger.info(to_color_str("按照日期计算 成交手数 成交金额", "blue"))

    df_tmp_110_4 = spark.table("edw.h15_hold_balance").alias("t") \
        .where(
        col("t.busi_date").between(lit(v_begin_date), lit(v_end_date))
    ).groupBy(
        col("t.busi_date"),
        col("t.fund_account_id")
    ).agg(
        sum(col("t.done_amt")).alias("done_amount"),
        sum(col("t.done_sum")).alias("done_money")
    ).alias("t").join(
        df_tmp_110_2.alias("a"),
        (col("t.fund_account_id") == col("a.fund_account_id")) &
        (col("t.busi_date").between(col("a.real_begin_date"), col("a.real_end_date")))
    ).groupBy(
        col("t.fund_account_id")
    ).agg(
        sum(col("t.done_amount")).alias("done_amount"),
        sum(col("t.done_money")).alias("done_money")
    )

    # 产品对应的客户总销售份额和业务人员对应的销售占比
    logger.info(to_color_str("产品对应的客户总销售份额和业务人员对应的销售占比", "blue"))

    tmp = spark.table("ddw.T_COCKPIT_00096").alias("a") \
        .where(
        col("a.busi_date").between(lit(v_begin_date), lit(v_end_date))
    ).groupBy(
        col("a.filing_code"),
        col("a.product_name"),
        col("a.client_name"),
        col("a.id_no")
    ).agg(
        sum(col("a.confirm_share")).alias("client_confirm_share")
    )

    tmp2 = spark.table("ddw.T_COCKPIT_00095").alias("t") \
        .select(
        col("t.month_id"),
        col("t.filing_code"),
        col("t.product_name"),
        col("t.PRODUCT_TOTAL_SHARE"),
        row_number().over(
            Window.partitionBy("t.filing_code").orderBy(col("t.month_id").desc())
        ).alias("rn")
    ).where(
        col("rn") == 1
    ).select(
        col("t.filing_code"),
        col("t.PRODUCT_TOTAL_SHARE")
    )

    tmp3 = spark.table("ddw.T_COCKPIT_00097").alias("t") \
        .join(
        tmp.alias("a"),
        (col("t.client_name") == col("a.client_name")) &
        (col("t.filing_code") == col("a.filing_code"))
    ).join(
        tmp2.alias("b"),
        col("t.filing_code") == col("b.filing_code")
    ).select(
        col("t.filing_code"),
        col("t.product_name"),
        col("b.product_total_share"),
        col("t.client_name"),
        col("a.id_no"),
        col("a.client_confirm_share"),
        col("t.oa_branch_id"),
        col("t.oa_branch_name"),
        col("t.oa_broker_name"),
        col("t.oa_broker_name_rate")
    )

    tmp4 = tmp3.alias("t").groupBy(
        col("t.filing_code"),
        col("t.oa_branch_id"),
        col("t.oa_broker_name"),
        col("t.product_total_share")
    ).agg(
        sum(col("t.client_confirm_share")).alias("client_confirm_share"),
        sum(col("t.client_confirm_share") * col("t.oa_broker_name_rate")).alias("oa_broker_product_share")
    )

    df_tmp_110_6 = tmp4.alias("t").select(
        col("t.filing_code"),
        col("t.oa_branch_id"),
        col("t.oa_broker_name"),
        col("t.product_total_share"),
        col("t.client_confirm_share"),
        when(
            col("t.product_total_share") != 0,
            col("t.oa_broker_product_share") / col("t.product_total_share")
        ).otherwise(0).alias("oa_broker_name_prop")
    )

    # 110_7
    logger.info(to_color_str("110_7", "blue"))

    tmp = spark.table("edw.h15_client_sett").alias("t") \
        .join(
        spark.table("ddw.T_COCKPIT_00095_1").alias("a"),
        (col("t.fund_account_id") == col("a.fund_account_id")) &
        (col("a.month_id") == lit(i_month_id))
    ).where(
        col("t.busi_date").between(lit(v_begin_date), lit(v_end_date))
    ).groupBy(
        col("a.filing_code"),
        col("a.product_name")
    ).agg(
        sum(
            when(
                lit(v_trade_days) > 0, col("t.rights") / lit(v_trade_days)
            ).otherwise(0)
        ).alias("avg_rights")
    )

    tmp1 = spark.table("ddw.T_COCKPIT_00100").alias("t") \
        .join(
        tmp.alias("a"),
        col("t.filing_code") == col("a.filing_code"),
        "left"
    ).join(
        df_tmp_110_6.alias("b"),
        (col("t.FILING_CODE") == col("b.filing_code")) &
        (col("t.OA_BRANCH_ID") == col("b.oa_branch_id")) &
        (col("t.OA_BROKER_NAME") == col("b.oa_broker_name")),
        "left"
    ).select(
        col("t.FILING_CODE"),
        col("t.PRODUCT_NAME"),
        col("t.OA_BRANCH_ID"),
        col("t.OA_BRANCH_NAME"),
        col("t.OA_BROKER_ID"),
        col("t.OA_BROKER_NAME"),
        (
            col("a.avg_rights") * (1 - col("t.OA_BRANCH_ADVISOR_RATE")) *
            col("b.oa_broker_name_prop")
        ).alias("sales_product_avg_rights"),
        (
            col("a.avg_rights") *
            col("t.OA_BRANCH_ADVISOR_RATE") *
            col("t.OA_BROKER_ADVISOR_RATE")
        ).alias("advisor_avg_rights"),
        col("b.client_confirm_share").alias("sales_share"),
        col("t.ADVISOR_NAME"),
        col("a.avg_rights").alias("fof_avg_rights")
    ).fillna(0)

    df_tmp_110_7 = tmp1.alias("t").select(
        col("t.FILING_CODE"),
        col("t.PRODUCT_NAME"),
        col("t.OA_BRANCH_ID"),
        col("t.OA_BRANCH_NAME"),
        col("t.OA_BROKER_ID"),
        col("t.OA_BROKER_NAME"),
        col("t.sales_product_avg_rights"),
        col("t.advisor_avg_rights"),
        (col("t.sales_product_avg_rights") + col("t.advisor_avg_rights")).alias("sum_avg_rights"),
        col("t.sales_share"),
        col("t.ADVISOR_NAME"),
        col("t.fof_avg_rights")
    )

    # 按照业务人员汇总数据
    logger.info(to_color_str("按照业务人员汇总数据", "blue"))

    tmp = df_tmp_110_1.alias("t").withColumn(
        "DATA_PCT", when(col("t.data_pct").isNull(), 1).otherwise(col("t.data_pct"))
    ).alias("t").join(
        df_tmp_110_3.alias("b"),
        col("t.fund_account_id") == col("b.fund_account_id"),
        "left"
    ).join(
        df_tmp_110_4.alias("c"),
        col("t.fund_account_id") == col("c.fund_account_id"),
        "left"
    ).join(
        spark.table("edw.h12_fund_account").alias("d"),
        col("t.fund_account_id") == col("d.fund_account_id"),
        "left"
    ).join(
        spark.table("ddw.t_ctp_branch_oa_rela").alias("e"),
        col("d.branch_id") == col("e.ctp_branch_id"),
        "inner"
    ).where(
        col("e.oa_branch_id").isNotNull()
    ).select(
        lit(
            v_begin_date + "-" + v_end_date
        ).alias("busi_date"),
        col("e.oa_branch_id"),
        col("t.fund_account_id"),
        (col("b.end_rights") * col("t.data_pct")).alias("end_rights"),
        (col("b.avg_rights") * col("t.data_pct")).alias("avg_rights"),
        (col("c.done_amount") * col("t.data_pct")).alias("done_amount"),
        (col("c.done_money") * col("t.data_pct")).alias("done_money")
    ).fillna(0)

    tmp1 = tmp.alias("t").groupBy(
        col("t.oa_branch_id")
    ).agg(
        sum(col("t.end_rights")).alias("end_rights"),
        sum(col("t.avg_rights")).alias("avg_rights"),
        sum(col("t.done_amount")).alias("done_amount"),
        sum(col("t.done_money")).alias("done_money")
    )

    tmp2 = df_tmp_110_7.alias("t").groupBy(
        col("t.oa_branch_id")
    ).agg(
        sum(col("t.sum_avg_rights")).alias("sum_avg_rights")
    )

    df_result = tmp1.alias("t").join(
        tmp2.alias("a"),
        col("t.oa_branch_id") == col("a.oa_branch_id"),
        "left"
    ).select(
        lit(i_month_id).alias("month_id"),
        col("t.oa_branch_id"),
        col("t.end_rights"),
        (col("t.avg_rights") + col("a.sum_avg_rights")).alias("avg_rights"),
        col("t.done_amount"),
        col("t.done_money")
    ).fillna(0)

    return_to_hive(
        spark=spark,
        df_result=df_result,
        target_table="ddw.T_COCKPIT_00179",
        insert_mode="overwrite"
    )
