# -*- coding: utf-8 -*-

from utils.task_env import return_to_hive_partitioned, return_to_hive
from pyspark.sql.functions import col, sum, expr, round, when, lit


def p_cockpit_00120_data(spark, busi_date):
    i_busi_month = busi_date[:6]
    # 查询符合条件的数据
    date_df = spark.table("cf_sett.t_pub_date").filter(
        (col("busi_date").substr(1, 6) == i_busi_month) &
        (col("market_no") == "1") &
        (col("trade_flag") == "1")
    )

    # 统计记录数量和最大业务日期
    v_trade_days = date_df.count()
    v_end_date = date_df.agg({"busi_date": "max"}).collect()[0][0]
    v_busi_date = i_busi_month[:4] + '-' + i_busi_month[4:6] + '-01'

    data_tax_interest = spark.table("cf_busimg.T_COCKPIT_00118_1").filter(
        (col("i_busi_month").between(col("begin_month"), col("end_month")))
    ).select("tax_rate", "interest_rate").collect()

    # 获取税率和利率
    v_tax_rate = data_tax_interest[0][0] if data_tax_interest else '1'
    v_interest_rate = data_tax_interest[0][1] if data_tax_interest else '1'

    if v_tax_rate is None:
        v_tax_rate = '1'

    if v_interest_rate is None:
        v_interest_rate = '1'

    # 读取数据表 CTP63.T_DS_ADM_INVESTOR_VALUE 和 CTP63.T_DS_DC_INVESTOR
    df_a = spark.table("CTP63.T_DS_ADM_INVESTOR_VALUE").alias("a")
    df_b = spark.table("CTP63.T_DS_DC_INVESTOR").alias("b")

    # 转换并计算字段
    result_df = df_a.join(df_b, df_a.investor_id == df_b.investor_id) \
        .filter(df_a.date_dt == v_busi_date) \
        .groupBy(df_a["a.investor_id"]) \
        .agg(
        round(sum(when(v_trade_days > 0, col("a.all_ri_amt") / col("b.v_trade_days"))
                  .otherwise(0)), 2).alias("avg_rights"),
        round(sum(when(col("a.date_dt") == v_end_date, col("a.today_ri_amt"))
                  .otherwise(0)), 2).alias("end_rights"),
        round(sum(col("a.all_ri_amt")), 2).alias("SUM_RIGHTS"),
        round(sum(col("a.subsistence_fee_amt")), 2).alias("remain_transfee"),
        round(sum(col("a.calint_amt")), 2).alias("interest_base"),
        round(sum(col("a.int_amt")), 2).alias("accrued_interest"),
        round(sum(col("a.exchangeret_amt")), 2).alias("market_ret"),
        round(sum(col("a.i_int_amt")), 2).alias("client_Interest_sett"),
        round(sum(col("a.i_exchangeret_amt")), 2).alias("client_market_ret")
    )
    return_to_hive(
        spark,
        result_df,
        "CF_BUSIMG.T_COCKPIT_00120_JZGX",
        "overwrite"
    )

    # 读取CF_BUSIMG.T_COCKPIT_00095表的数据
    t_cockpit_00095 = spark.table("CF_BUSIMG.T_COCKPIT_00095")

    # 进行筛选操作，选择符合条件的数据
    data_to_insert = t_cockpit_00095.filter(
        (t_cockpit_00095.month_id == i_busi_month) & ~(t_cockpit_00095.product_name.like("%FOF%")))

    # 选择需要的列，并重命名列名
    data_to_insert = data_to_insert.select(
        t_cockpit_00095.month_id.alias("busi_month"),
        t_cockpit_00095.filing_code.alias("FILING_CODE"),
        t_cockpit_00095.product_name.alias("PRODUCT_NAME")
    )

    # 从Hive中读取数据
    df_t_cockpit_00120_jzgx = spark.table("CF_BUSIMG.T_COCKPIT_00120_JZGX")
    df_t_cockpit_00095_1 = spark.table("CF_BUSIMG.T_COCKPIT_00095_1")

    # 进行关联和聚合操作
    df_agg = df_t_cockpit_00120_jzgx.join(df_t_cockpit_00095_1,
                                          (df_t_cockpit_00120_jzgx["fund_account_id"] == df_t_cockpit_00095_1[
                                              "FUND_ACCOUNT_ID"])
                                          & (df_t_cockpit_00095_1["month_id"] == "i_busi_month")
                                          ) \
        .groupBy("i_busi_month", "b.filing_code") \
        .agg(sum("end_rights").alias("end_rights"),
             sum("avg_rights").alias("avg_rights"),
             sum("sum_rights").alias("sum_rights"),
             sum("interest_base").alias("interest_base"),
             sum("REMAIN_TRANSFEE").alias("CLEAR_REMAIN_TRANSFEE"),
             sum("MARKET_RET").alias("MARKET_REDUCT"),
             sum("CLIENT_MARKET_RET").alias("CLIENT_RET_MARREDUCT"),
             sum("CLIENT_INTEREST_SETT").alias("CLIENT_RET_INTEREST"))

    # 读取表格test.t_cockpit_00120中的数据
    df = spark.table("test.t_cockpit_00120")

    # 更新字段值
    df_updated = df.withColumn("CLIENT_RET_MARREDUCT_EXTAX", expr("CLIENT_RET_MARREDUCT / (1 + v_tax_rate)")) \
        .withColumn("INTEREST_INCOME", expr("INTEREST_BASE * (1 + v_interest_rate) / 360")) \
        .withColumn("MARKET_REDUCT_EXTAX", expr("MARKET_REDUCT / (1 + v_tax_rate)")) \
        .withColumn("CLEAR_REMAIN_TRANSFEE_EXTAX", expr("CLEAR_REMAIN_TRANSFEE / (1 + v_tax_rate)")) \
 \
        # 根据条件筛选并更新
    df_final = df_updated.filter(df_updated["busi_month"] == i_busi_month)

    # 加载表格test.t_cockpit_00120并进行更新操作
    cockpit = spark.table("test.t_cockpit_00120")
    updated_cockpit = cockpit.withColumn("total_futu_income",
                                         col("clear_remain_transfee_extax") +
                                         col("market_reduct_extax") +
                                         col("interest_income") -
                                         col("client_ret_marreduct_extax") -
                                         col("client_ret_interest"))
