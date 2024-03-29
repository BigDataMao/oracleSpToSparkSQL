# -*- coding: utf-8 -*-
"""
经纪业务收入、权益情况-数据落地
"""
import logging

from pyspark.sql.functions import col, lit, min, max, count, sum, coalesce, when

from utils.task_env import return_to_hive, update_dataframe


def p_cockpit_00133_data(spark, busi_date):
    logging.basicConfig(level=logging.INFO)

    i_month_id = busi_date[:6]

    df_date_trade = spark.table("edw.t10_pub_date") \
        .filter(
        (col("busi_date").substr(1, 6) == i_month_id) &
        (col("market_no") == "1") &
        (col("trade_flag") == "1")
    ).agg(
        min("busi_date").alias("v_begin_trade_date"),
        max("busi_date").alias("v_end_trade_date"),
        count("*").alias("v_trade_days")
    )

    first_row_trade = df_date_trade.first()
    v_begin_trade_date = first_row_trade["v_begin_trade_date"]
    v_end_trade_date = first_row_trade["v_end_trade_date"]
    v_trade_days = first_row_trade["v_trade_days"]

    df_date = spark.table("edw.t10_pub_date") \
        .filter(
        (col("busi_date").substr(1, 6) == i_month_id) &
        (col("market_no") == "1")
    ).agg(
        min("busi_date").alias("v_begin_date"),
        max("busi_date").alias("v_end_date")
    )

    first_row = df_date.first()
    v_begin_date = first_row["v_begin_date"]
    v_end_date = first_row["v_end_date"]

    logging.info(
        u"""
        v_begin_trade_date: %s
        v_end_trade_date:   %s
        v_trade_days:       %s
        v_begin_date:       %s
        v_end_date:         %s
        参数处理完毕
        """,
        v_begin_trade_date, v_end_trade_date, v_trade_days, v_begin_date, v_end_date
    )

    # ------------------- 参数处理完毕 -------------------

    df_113_m = spark.table("ddw.v_oa_branch") \
        .withColumn("busi_month", lit(i_month_id))

    return_to_hive(
        spark=spark,
        df_result=df_113_m,
        target_table="ddw.T_COCKPIT_00133",
        insert_mode="overwrite",
        partition_column="busi_month",
        partition_value=i_month_id
    )

    df_113_m = spark.table("ddw.T_COCKPIT_00133") \
        .filter(col("busi_month") == i_month_id)

    """
    财务内核表-调整前 cf_busimg.t_cockpit_00127
    经纪业务收入-留存手续费收入 REMAIN_TRANSFEE_INCOME
    经纪业务收入-交易所减免净收入 MARKET_REDUCE_INCOME
    经纪业务收入-利息净收入 CLEAR_INTEREST_INCOME
    """

    df_127 = spark.table("ddw.t_cockpit_00127").alais("df_127")
    df_b = spark.table("ddw.T_YY_BRANCH_OA_RELA").alais("df_b")

    df_y = df_127.join(df_b, col("df_127.book_id") == col("df_b.yy_book_id")) \
        .filter(col("month_id") == i_month_id) \
        .groupby(col("df_b.oa_branch_id"), col("df_b.oa_branch_name")) \
        .agg(
        sum("b6").alias("REMAIN_TRANSFEE_INCOME"),
        sum("b8").alias("MARKET_REDUCE_INCOME"),
        sum("b7").alias("CLEAR_INTEREST_INCOME")
    ).alais("df_y")

    df_113_m = update_dataframe(
        df_to_update=df_113_m,
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=["REMAIN_TRANSFEE_INCOME", "MARKET_REDUCE_INCOME", "CLEAR_INTEREST_INCOME"]
    )

    """
    经纪业务收入=留存手续费收入+交易所手续费减免净收入+利息净收入
    """

    df_113_m = df_113_m \
        .withColumn("FUTU_INCOME_new",
                    coalesce(col("REMAIN_TRANSFEE_INCOME"), lit(0)) +
                    coalesce(col("MARKET_REDUCE_INCOME"), lit(0)) +
                    coalesce(col("CLEAR_INTEREST_INCOME")), lit(0)
                    ) \
        .drop("FUTU_INCOME") \
        .withColumnRenamed("FUTU_INCOME_new", "FUTU_INCOME")

    """更新
    CF_BUSIMG.T_COCKPIT_00122  投资咨询基本信息维护参数表-主表
    CF_BUSIMG.T_COCKPIT_00122_1  投资咨询基本信息-内核分配比例-表1
    交易咨询-交易咨询收入   TRANS_CONSULT_INCOME  (投资咨询服务费总额：)
    交易咨询-销售收入  SALES_INCOME (投资咨询内核表——销售部门收入，业务单位作为销售部门时的收入)
    """

    df_122 = spark.table("ddw.t_cockpit_00122") \
        .filter(col("busi_month") == i_month_id).alias("t")
    df_122_1 = spark.table("ddw.t_cockpit_00122_1") \
        .filter(col("busi_month") == i_month_id).alias("a")
    df_fund_account = spark.table("edw.h12_fund_account").alias("b")
    df_branch_oa = spark.table("ddw.t_ctp_branch_oa_rela").alias("c")

    df_y = df_122.join(
        other=df_122_1,
        on=["client_id", "product_name"],
        how="left"
    ).join(
        other=df_fund_account,
        on=col("t.client_id") == col("b.fund_account_id"),
        how="left"
    ).join(
        other=df_branch_oa,
        on=col("b.branch_id") == col("c.CTP_BRANCH_ID"),
        how="inner"
    ).groupby(
        "c.oa_branch_id", "c.oa_branch_name"
    ).agg(
        sum(col("t.INVEST_TOTAL_SERVICE_FEE")).alias("TRANS_CONSULT_INCOME"),
        sum(
            when(
                condition=col("a.ALLOCA_OA_BRANCH_TYPE") == lit("1"),
                value=col("t.INVEST_TOTAL_SERVICE_FEE") * col("t.kernel_total_rate") * col("a.alloca_kernel_rate")
            ).otherwise(lit(0))
        ).alias("SALES_INCOME")
    )

    df_113_m = update_dataframe(
        df_to_update=df_113_m,
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=["TRANS_CONSULT_INCOME", "SALES_INCOME"]
    )

    """更新
    产品销售收入 PRODUCT_SALES_INCOME 未开发 默认未0
    """

    """更新
    CF_BUSIMG.T_COCKPIT_00126   场外协同清算台账参数表
    场外期权收入  OUT_OPTION_INCOME（ 场外期权-场外协同清算台账-合计收入 TOTAL_INCOME）
    名义本金  NOTIONAL_PRINCIPAL
    场外期权规模-权利金  ROYALTY
    """

    df_126 = spark.table("ddw.t_cockpit_00126").alias("t")

    df_y = df_126.join(
        other=df_fund_account,
        on=col("t.client_id") == col("b.fund_account_id"),
        how="left"
    ).join(
        other=df_branch_oa,
        on=col("b.branch_id") == col("c.CTP_BRANCH_ID"),
        how="inner"
    ).groupby(
        "c.oa_branch_id", "c.oa_branch_name"
    ).agg(
        sum(col("t.TOTAL_INCOME")).alias("OUT_OPTION_INCOME"),
        sum(col("t.NOTIONAL_PRINCIPAL")).alias("NOTIONAL_PRINCIPAL"),
        sum(col("t.TOTAL_ABSOLUTE_ROYALTY")).alias("ROYALTY")
    )

    df_113_m = update_dataframe(
        df_to_update=df_113_m,
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=["OUT_OPTION_INCOME", "NOTIONAL_PRINCIPAL", "ROYALTY"]
    )

    """更新
    未开发 默认未0
    IB协同/驻点业务协同收入
    自有资金项目参与收入
    日均权益
    考核日均权益
    """

    """更新
    经纪业务规模-期末权益
    期末权益 END_RIGHTS
    """

    df_sett = spark.table("edw.h15_client_sett") \
        .alias("df_sett") \
        .filter(col("df_sett.busi_date") == v_end_trade_date)

    df_y = df_sett.join(
        other=df_fund_account,
        on=col("df_sett.client_id") == col("b.fund_account_id"),
        how="left"
    ).join(
        other=df_branch_oa,
        on=col("b.branch_id") == col("c.CTP_BRANCH_ID"),
        how="inner"
    ).groupby(
        "c.oa_branch_id", "c.oa_branch_name"
    ).agg(
        sum(col("df_sett.rights")).alias("END_RIGHTS")
    ).select(
        "c.oa_branch_id", "c.oa_branch_name", "END_RIGHTS"
    )

    update_dataframe(
        df_to_update=df_113_m,
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=["END_RIGHTS"]
    )

    """
    产品销售规模 保有量 PRODUCT_INVENTORY
    产品销售规模 新增量 PRODUCT_NEW 
    """

    df_96 = spark.table("ddw.t_cockpit_00096") \
        .filter(col("busi_date").between(v_begin_trade_date, v_end_trade_date)) \
        .alias("df_96")

    df_y = df_96.join(
        other=df_fund_account,
        on=(
            (col("df_96.id_no") == col("b.id_no")) &
            (col("df_96.client_name") == col("b.client_name"))
        ),
        how="inner"
    ).join(
        other=df_branch_oa,
        on=col("b.branch_id") == col("c.CTP_BRANCH_ID"),
        how="inner"
    ).groupby(
        "c.oa_branch_id", "c.oa_branch_name"
    ).agg(
        sum(col("df_96.confirm_share")).alias("PRODUCT_INVENTORY"),
        sum(
            when(
                col("a.wh_trade_type").isin(["0", "1"]),
                col("a.confirm_share")
            ).otherwise(0)
        ).alias("PRODUCT_NEW")
    ).select(
        "c.oa_branch_id", "c.oa_branch_name", "PRODUCT_INVENTORY", "PRODUCT_NEW"
    )

    update_dataframe(
        df_to_update=df_113_m,
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=["PRODUCT_INVENTORY", "PRODUCT_NEW"]
    )

    """
    IB协同/驻点业务规模-期末权益 IB_END_RIGHTS  “IB协同统计汇总表——期末权益”与“驻点人员营销统计数据表——期末权益”之和
    IB协同/驻点业务规模-日均权益 IB_AVG_RIGHTS  “IB协同统计汇总表——日均权益”与“驻点人员营销统计数据表——日均权益”之和
    """

    # IB协同统计汇总表——期末权益”

    df_tmp = spark.table("ddw.t_cockpit_00107").alias("t") \
        .join(
            df_branch_oa.alias("c"),
            col("t.branch_id") == col("c.CTP_BRANCH_ID"),
            "inner"
        ).filter(
            (col("t.confirm_date").between(v_begin_trade_date, v_end_trade_date))
        ).groupby(
            "t.confirm_date", "t.fund_account_id", "c.OA_BRANCH_ID"
        ).select(
            col("t.confirm_date").alias("busi_date"), "t.fund_account_id", "c.OA_BRANCH_ID"
        )

    df_result = spark.table("cf_sett.t_client_sett").alias("t") \
        .join(df_tmp.alias("a"),
              (col("t.busi_date") == col("a.confirm_date")) & (col("t.fund_account_id") == col("a.fund_account_id")),
              "inner") \
        .filter((col("t.busi_date").between(v_begin_trade_date, v_end_trade_date))) \
        .groupby("a.OA_BRANCH_ID") \
        .agg(
        sum(col("a.end_rights")).alias("end_rights"),
        sum(when(col("v_trade_days") > 0, col("a.end_rights") / v_trade_days).otherwise(0)).alias("avg_rights")
    )

    # 将结果插入到目标表中
    return_to_hive(
        spark=spark,
        df_result=df_result,
        target_table="cf_busimg.tmp_cockpit_00133_1",
        insert_mode="overwrite",
        partition_column=None,  # 没有分区
        partition_value=None
    )

