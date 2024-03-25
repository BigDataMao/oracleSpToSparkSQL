# -*- coding: utf-8 -*-

from pyspark.sql.functions import col, sum, expr, round, when, coalesce, lit

from utils.task_env import return_to_hive, return_to_hive_partitioned


def p_cockpit_00120_data(spark, busi_date):
    i_busi_month = busi_date[:6]
    # 查询符合条件的数据
    date_df = spark.table("edw.t10_pub_date").filter(
        (col("busi_date").substr(1, 6) == i_busi_month) &
        (col("market_no") == "1") &
        (col("trade_flag") == "1")
    )

    # 统计记录数量和最大业务日期
    v_trade_days = date_df.count()
    v_end_date = date_df.agg({"busi_date": "max"}).collect()[0][0]
    v_busi_date = i_busi_month[:4] + '-' + i_busi_month[4:6] + '-01'

    data_tax_interest = spark.table("ddw.T_COCKPIT_00118_1").filter(
        (lit(i_busi_month).between(col("begin_month"), col("end_month")))
    ).select("tax_rate", "interest_rate").collect()

    # 获取税率和利率
    v_tax_rate = data_tax_interest[0][0] if data_tax_interest else '1'
    v_interest_rate = data_tax_interest[0][1] if data_tax_interest else '1'

    if v_tax_rate is None:
        v_tax_rate = '1'

    if v_interest_rate is None:
        v_interest_rate = '1'

    # 读取数据表 ods.ctp63_T_DS_ADM_INVESTOR_VALUE 和 ods.ctp63_T_DS_DC_INVESTOR
    df_a = spark.table("ods.ctp63_T_DS_ADM_INVESTOR_VALUE")
    df_b = spark.table("ods.ctp63_T_DS_DC_INVESTOR")

    # 转换并计算字段
    t_cockpit_00120_jzgx = df_a.join(df_b, df_a.investor_id == df_b.investor_id) \
        .filter(df_a.date_dt == v_busi_date) \
        .groupBy(df_a.investor_id) \
        .agg(
        round(sum(when(lit(v_trade_days) > 0, df_a.all_ri_amt / v_trade_days)
                  .otherwise(0)), 2).alias("avg_rights"),
        round(sum(when(df_a.date_dt == lit(v_end_date), df_a.today_ri_amt)
                  .otherwise(0)), 2).alias("end_rights"),
        round(sum(df_a.all_ri_amt), 2).alias("SUM_RIGHTS"),
        round(sum(df_a.subsistence_fee_amt), 2).alias("remain_transfee"),
        round(sum(df_a.calint_amt), 2).alias("interest_base"),
        round(sum(df_a.int_amt), 2).alias("accrued_interest"),
        round(sum(df_a.exchangeret_amt), 2).alias("market_ret"),
        round(sum(df_a.i_int_amt), 2).alias("client_Interest_sett"),
        round(sum(df_a.i_exchangeret_amt), 2).alias("client_market_ret")
    ).withColumnRenamed("investor_id", "fund_account_id")

    return_to_hive(
        spark,
        t_cockpit_00120_jzgx,
        "ddw.T_COCKPIT_00120_JZGX",
        "overwrite"
    )

    # 读取ddw.T_COCKPIT_00095表的数据
    t_cockpit_00095 = spark.table("ddw.T_COCKPIT_00095")

    # 初始化数据
    t_cockpit_00120 = t_cockpit_00095.filter(
        (col("month_id") == i_busi_month) &
        ~(col("product_name").like("%FOF%"))
    ).withColumnRenamed("month_id", "busi_month")

    # 从Hive中读取数据
    df_a = t_cockpit_00120_jzgx
    df_b = spark.table("ddw.T_COCKPIT_00095_1")

    # 执行JOIN和GROUP BY操作
    merged_df = df_a.join(df_b, df_a.fund_account_id == df_b.fund_account_id) \
        .filter(df_b.month_id == i_busi_month) \
        .groupBy(lit(i_busi_month).alias("busi_month"), "filing_code") \
        .agg(
        sum("end_rights").alias("end_rights"),
        sum("avg_rights").alias("avg_rights"),
        sum("sum_rights").alias("sum_rights"),
        sum("interest_base").alias("interest_base"),
        sum("REMAIN_TRANSFEE").alias("CLEAR_REMAIN_TRANSFEE"),
        sum("MARKET_RET").alias("MARKET_REDUCT"),
        sum("CLIENT_MARKET_RET").alias("CLIENT_RET_MARREDUCT"),
        sum("CLIENT_INTEREST_SETT").alias("CLIENT_RET_INTEREST")
    )

    # 更新目标表
    target_df = merged_df \
        .join(t_cockpit_00120,
              (merged_df.busi_month == i_busi_month) & (
                      merged_df.filing_code == t_cockpit_00120.filing_code),
              "outer") \
        .select(
            coalesce(merged_df.busi_month, t_cockpit_00120.busi_month).alias("busi_month"),
            coalesce(merged_df.filing_code, t_cockpit_00120.filing_code).alias("filing_code"),
            coalesce(merged_df.end_rights, lit(0)).alias("end_rights"),
            coalesce(merged_df.avg_rights, lit(0)).alias("avg_rights"),
            coalesce(merged_df.sum_rights, lit(0)).alias("sum_rights"),
            coalesce(merged_df.interest_base, lit(0)).alias("interest_base"),
            coalesce(merged_df.CLEAR_REMAIN_TRANSFEE, lit(0)).alias("CLEAR_REMAIN_TRANSFEE"),
            coalesce(merged_df.MARKET_REDUCT, lit(0)).alias("MARKET_REDUCT"),
            coalesce(merged_df.CLIENT_RET_MARREDUCT, lit(0)).alias("CLIENT_RET_MARREDUCT"),
            coalesce(merged_df.CLIENT_RET_INTEREST, lit(0)).alias("CLIENT_RET_INTEREST")
        )

    # 更新字段值
    target_df = target_df \
        .withColumn("CLIENT_RET_MARREDUCT_EXTAX", expr("CLIENT_RET_MARREDUCT / (1 + {0})".format(v_tax_rate))) \
        .withColumn("INTEREST_INCOME", expr("INTEREST_BASE * (1 + {0}) / 360".format(v_interest_rate))) \
        .withColumn("MARKET_REDUCT_EXTAX", expr("MARKET_REDUCT / (1 + {0})".format(v_tax_rate))) \
        .withColumn("CLEAR_REMAIN_TRANSFEE_EXTAX", expr("CLEAR_REMAIN_TRANSFEE / (1 + {0})".format(v_tax_rate)))

    # 根据条件筛选并更新
    df_final = target_df.filter(target_df["busi_month"] == i_busi_month)

    df_final = df_final.withColumn("total_futu_income",
                                   col("clear_remain_transfee_extax") +
                                   col("market_reduct_extax") +
                                   col("interest_income") -
                                   col("client_ret_marreduct_extax") -
                                   col("client_ret_interest")
                                   )

    return_to_hive_partitioned(
        spark,
        df_final,
        "ddw.T_COCKPIT_00120",
        "i_busi_month",
        i_busi_month,
        "overwrite"
    )
