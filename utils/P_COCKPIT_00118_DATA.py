# -*- coding: utf-8 -*-
"""
收入分配表(最终呈现表)FOF产品  汇总数据 数据落地，到月份
"""
import logging

from pyspark.sql.functions import col, lit, max, count, sum, when, round

from utils.task_env import return_to_hive, update_dataframe


def p_cockpit_00118_data(spark, busi_date):
    logging.basicConfig(level=logging.INFO)
    i_month_id = busi_date[:6]
    v_busi_year = busi_date[:4]

    df_date_trade = spark.table("edw.t10_pub_date") \
        .filter(
        (col("busi_date").substr(1, 6) == i_month_id) &
        (col("market_no") == "1") &
        (col("trade_flag") == "1")
    ).agg(
        count("busi_date").alias("v_trade_days"),
        max("busi_date").alias("v_end_date")
    )
    first_row_trade = df_date_trade.first()
    v_trade_days = first_row_trade["v_trade_days"]
    v_end_date = first_row_trade["v_end_date"]

    logging.info(
        u"""
        v_trade_days: %s
        v_end_date:   %s
        参数处理完毕
        """,
        v_trade_days, v_end_date
    )

    # 2024-04-01
    # v_busi_date = v_busi_year + "-" + busi_date[4:8] + "-" + "01"
    v_busi_date = v_busi_year + "-" + busi_date[6:8] + "-" + "01"

    data_tax_interest = spark.table("ddw.T_COCKPIT_00118_1").filter(
        (lit(i_month_id).between(col("begin_month"), col("end_month")))
    ).select("tax_rate", "interest_rate").collect()

    # 获取税率和利率
    v_tax_rate = data_tax_interest[0][0] if data_tax_interest else '1'
    v_interest_rate = data_tax_interest[0][1] if data_tax_interest else '1'

    if v_tax_rate is None:
        v_tax_rate = '1'

    if v_interest_rate is None:
        v_interest_rate = '1'

    # 插入季度参数表数据增量更新
    # CF_BUSIMG.T_COCKPIT_00118_QUARTER
    # 更新表ddw.t_cockpit_00118_quarter的数据，改成全量更新

    # TODO: 维护季度参数表
    # quarter_data = [(v_busi_year, 1, "第一季度"), (v_busi_year, 2, "第二季度"), (v_busi_year, 3, "第三季度"),
    #                 (v_busi_year, 4, "第四季度")]
    # return_to_hive(
    #     spark,
    #     quarter_data,
    #     "ddw.t_cockpit_00118_quarter",
    #     "overwrite"
    # )

    # 读取数据表 ods.ctp63_T_DS_ADM_INVESTOR_VALUE 和 ods.ctp63_T_DS_DC_INVESTOR
    df_a = spark.table("ods.ctp63_T_DS_ADM_INVESTOR_VALUE")
    df_b = spark.table("ods.ctp63_T_DS_DC_INVESTOR")

    # 转换并计算字段
    t_cockpit_00118_jzgx = df_a.join(df_b, col("df_a.investor_id") == col("df_b.investor_id")) \
        .filter(col("df_a.date_dt") == v_busi_date) \
        .groupby(col("df_a.investor_id")) \
        .agg(
        round(sum(when(
            lit(v_trade_days) > 0,
            col("df_a.all_ri_amt") / v_trade_days
        ).otherwise(0)), 2).alias("avg_rights"),
        round(sum(when(
            df_a.date_dt == lit(v_end_date),
            col("df_a.today_ri_amt")
        ).otherwise(0)), 2).alias("end_rights"),
        round(sum(col("df_a.all_ri_amt")), 2).alias("SUM_RIGHTS"),
        round(sum(col("df_a.subsistence_fee_amt")), 2).alias("remain_transfee"),
        round(sum(col("df_a.calint_amt")), 2).alias("interest_base"),
        round(sum(col("df_a.int_amt")), 2).alias("accrued_interest"),
        round(sum(col("df_a.exchangeret_amt")), 2).alias("market_ret"),
        round(sum(col("df_a.i_int_amt")), 2).alias("client_Interest_sett"),
        round(sum(col("df_a.i_exchangeret_amt")), 2).alias("client_market_ret")
    ).alais("t_cockpit_00118_jzgx")

    return_to_hive(
        spark=spark,
        df_result=t_cockpit_00118_jzgx,
        target_table="ddw.T_COCKPIT_00118_JZGX",
        insert_mode="overwrite"
    )

    # TODO CF_BUSIMG.T_COCKPIT_00118按busi_month分区
    # 读取ddw.T_COCKPIT_00095表的数据
    df_95 = spark.table("ddw.T_COCKPIT_00095")

    # df_t_cockpit_00118 = spark.table("ddw.T_COCKPIT_00118") \
    #     .filter(col("busi_month") == i_month_id)

    # 初始化数据
    df_t_cockpit_00118 = df_95.alais("t") \
        .filter(
        (col("t.month_id") == i_month_id) &
        (col("t.product_name").like("%FOF%"))
    ).select(
        col("t.month_id").alias("busi_month"),
        col("t.filing_code"),
        col("t.product_name"),
    )

    # 从Hive中读取数据
    df_a = t_cockpit_00118_jzgx
    df_b = spark.table("ddw.T_COCKPIT_00095_1")

    """
    更新
    END_RIGHTS 当月期末权益
    AVG_RIGHTS 日均权益 - 元
    SUM_RIGHTS 总权益
    INTEREST_BASE 利息积数 - 元
    CLEAR_REMAIN_TRANSFEE 净留存手续费 - 元
    MARKET_REDUCT 交易所减免 - 元
    CLIENT_RET_MARREDUCT 交易所减免 - 元
    CLIENT_RET_INTEREST 客户返还 - 利息收入 - 元
    """
    df_y = df_a.join(df_b, col("df_a.fund_account_id") == col("df_b.fund_account_id")) \
        .filter(col("df_b.month_id") == i_month_id) \
        .groupBy(col("df_b.filing_code")) \
        .agg(
        sum(col("df_a.end_rights")).alias("end_rights"),
        sum(col("df_a.avg_rights")).alias("avg_rights"),
        sum(col("df_a.sum_rights")).alias("sum_rights"),
        sum(col("df_a.interest_base")).alias("interest_base"),
        sum(col("df_a.REMAIN_TRANSFEE")).alias("CLEAR_REMAIN_TRANSFEE"),
        sum(col("df_a.MARKET_RET")).alias("MARKET_REDUCT"),
        sum(col("df_a.CLIENT_MARKET_RET")).alias("CLIENT_RET_MARREDUCT"),
        sum(col("df_a.CLIENT_INTEREST_SETT")).alias("CLIENT_RET_INTEREST")
    ).select(
        lit(i_month_id).alias("busi_month"),

    )

    df_t_cockpit_00118 = update_dataframe(
        df_to_update=df_t_cockpit_00118,
        df_use_me=df_y,
        join_columns=["filing_code"],
        update_columns=["END_RIGHTS",
                        "AVG_RIGHTS",
                        "SUM_RIGHTS",
                        "INTEREST_BASE",
                        "CLEAR_REMAIN_TRANSFEE",
                        "MARKET_REDUCT",
                        "CLIENT_RET_MARREDUCT",
                        "CLIENT_RET_INTEREST"]
    )

    """
    --更新
    MANAGE_FEE_INCOME当期管理费收入 - 元（+业绩报酬）
    SALES_INCENTIVES 销售奖励
    """
    # 从Hive中读取数据
    df_116 = spark.table("ddw.T_COCKPIT_00116")
    df_y = df_116.alias("t") \
        .filter((col("t.busi_month") == i_month_id)) \
        .select(
        col("t.busi_month"),
        col("t.filing_code"),
        col("t.MANAGE_FEE_INCOME").alias("MANAGE_FEE_INCOME"),
        col("t.SALES_INCENTIVES").alias("SALES_INCENTIVES"),
    )

    df_t_cockpit_00118 = update_dataframe(
        df_to_update=df_t_cockpit_00118,
        df_use_me=df_y,
        join_columns=["filing_code"],
        update_columns=["MANAGE_FEE_INCOME", "SALES_INCENTIVES"]
    )

    """
    更新
    CLIENT_RET_MARREDUCT_EXTAX 客户返还 - 交易所减免 - 元不含税
    INTEREST_INCOME 利息收入 - 元
    MARKET_REDUCT_EXTAX 交易所减免 - 元不含税
    CLEAR_REMAIN_TRANSFEE_EXTAX净留存手续费 - 元不含税
    MANAGE_FEE_INCOME_EXTAX 当期管理费收入 - 元不含税
    """
    df_t_cockpit_00118 = df_t_cockpit_00118.alias("a") \
        .withColumn(
        "CLIENT_RET_MARREDUCT_EXTAX",
        col("a.CLIENT_RET_MARREDUCT") / (1 + lit(v_tax_rate))
    ).withColumn(
        "INTEREST_INCOME",
        col("a.INTEREST_BASE") * (1 + lit(v_interest_rate)) / 360
    ).withColumn(
        "MARKET_REDUCT_EXTAX",
        col("a.MARKET_REDUCT") / (1 + lit(v_tax_rate))
    ).withColumn(
        "CLEAR_REMAIN_TRANSFEE_EXTAX",
        col("a.CLEAR_REMAIN_TRANSFEE ") / (1 + lit(v_tax_rate))
    ).withColumn(
        "MANAGE_FEE_INCOME_EXTAX",
        col("a.MANAGE_FEE_INCOME") / (1 + lit(v_tax_rate))
    )

    """
    更新
    TOTAL_FUTU_INCOME 经纪业务总收入-元
    """
    df_t_cockpit_00118 = df_t_cockpit_00118.withColumn(
        "TOTAL_FUTU_INCOME",
        col("CLEAR_REMAIN_TRANSFEE_EXTAX") +
        col("MARKET_REDUCT_EXTAX") +
        col("INTEREST_INCOME") -
        col("CLIENT_RET_MARREDUCT_EXTAX") -
        col("CLIENT_RET_INTEREST")
    )

    return_to_hive(
        spark=spark,
        df_result=df_t_cockpit_00118,
        target_table="ddw.T_COCKPIT_00118",
        insert_mode="overwrite",
        partition_column="busi_month",
        partition_value=i_month_id
    )

    logging.info("ddw.T_COCKPIT_00118写入完成")