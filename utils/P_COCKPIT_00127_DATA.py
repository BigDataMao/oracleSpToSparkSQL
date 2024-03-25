# -*- coding: utf-8 -*-
import logging

from pyspark.sql.functions import col, min, max, when, sum, lit, coalesce


def p_cockpit_00127_data(spark, busi_date):
    logging.info("开始执行p_cockpit_00127_data.py,接收到busi_date: %s", busi_date)
    i_month_id = busi_date[:6]
    df_date = spark.table("edw.t10_pub_date") \
        .filter(
        (col("busi_date").substr(1, 6) == i_month_id) &
        (col("market_no") == "1") &
        (col("trade_flag") == "1")
    ).agg(
        min("busi_date").alias("v_begin_trade_date"),
        max("busi_date").alias("v_end_trade_date")
    )

    v_begin_trade_date = df_date.collect()[0]["v_begin_trade_date"]
    v_end_trade_date = df_date.collect()[0]["v_end_trade_date"]
    logging.info("v_begin_trade_date: %s, v_end_trade_date: %s", v_begin_trade_date, v_end_trade_date)

    v_begin_month = i_month_id[:4] + '01'
    v_end_month = i_month_id
    logging.info("v_begin_month: %s, v_end_month: %s", v_begin_month, v_end_month)

    t_cockpit_00127 = spark.table("FUTURES.T_BOOK_INFO") \
        .filter(col("book_id").isNotNull()) \
        .select(col("month_id"), col("book_id"))

    # 从Hive表中获取数据并聚合
    """
    一、营业收入
        b6     手续费及佣金净收入（净损失以“－”号填列）    6021手续费及佣金收入-6421手续费及佣金支出
        b7     利息净收入（净损失以“－”号填列）            6011利息收入-6411利息支出
        b8     手续费减免返还收入(投资收益（净损失以“－” 号填列）)             6111投资收益
        b9     管理费收入 公允价值变动收益                              6101公允价值变动损益
        b10    投资收益（ 汇兑收益（净损失以“－”号填列）               6061汇兑损益
        b11     其他业务收入                                  6051其他业务收入
    """
    df_branch = spark.table("CF_BUSIMG.T_HYNC65_INDEX_RESULT_BRANCH")
    df_y = df_branch \
        .filter((col("busi_month") == i_month_id) & (col("index_para_name") == "营业收入")) \
        .groupBy("busi_month", "book_id") \
        .agg(
            sum(when(col("index_id").isin('6021', '6421'), col("index_value")).otherwise(0)).alias("b6"),
            sum(when(col("index_id").isin('6011', '6411'), col("index_value")).otherwise(0)).alias("b7"),
            sum(when(col("index_id") == lit('6111'), col("index_value")).otherwise(0)).alias("b8"),
            sum(when(col("index_id") == lit('6101'), col("index_value")).otherwise(0)).alias("b9"),
            sum(when(col("index_id") == lit('6061'), col("index_value")).otherwise(0)).alias("b10"),
            sum(when(col("index_id") == lit('6051'), col("index_value")).otherwise(0)).alias("b11")
        ) \
        .withColumnRenamed("busi_month", "month_id")

    t_cockpit_00127 = t_cockpit_00127.join(df_y, (df_y["book_id"] == t_cockpit_00127["book_id"]) &
                                           (df_y["month_id"] == t_cockpit_00127["month_id"]), "left") \
        .withColumn("b6", coalesce(df_y["b6"], t_cockpit_00127["b6_y"])) \
        .withColumn("b7", coalesce(df_y["b7"], t_cockpit_00127["b7_y"])) \
        .withColumn("b8", coalesce(df_y["b8"], t_cockpit_00127["b8_y"])) \
        .withColumn("b9", coalesce(df_y["b9"], t_cockpit_00127["b9_y"])) \
        .withColumn("b10", coalesce(df_y["b10"], t_cockpit_00127["b10_y"])) \
        .withColumn("b11", coalesce(df_y["b11"], t_cockpit_00127["b11_y"]))

    """
    二、营业支出
        b13  应返佣金支出   0
        b14  税金及附加　   ROUND(B6*0.0072,2)
        b15  业务及管理费　 6601+6404
        b16  资产减值损失　 6701
        b17  其他业务成本   6402
    """

    t_cockpit_00127 = t_cockpit_00127.withColumn("b14", round(t_cockpit_00127["b6"] * 0.0072, 2))

    df_y = df_branch \
        .filter((col("busi_month") == i_month_id) & (col("index_para_name") == "营业支出")) \
        .groupBy("busi_month", "book_id") \
        .agg(
            sum(when(col("index_id").isin('6601', '6404'), col("index_value")).otherwise(0)).alias("b15"),
            sum(when(col("index_id") == lit('6701'), col("index_value")).otherwise(0)).alias("b16"),
            sum(when(col("index_id") == lit('6402'), col("index_value")).otherwise(0)).alias("b17")
        ) \
        .withColumn("b13", 0) \
        .withColumnRenamed("busi_month", "month_id")

    t_cockpit_00127 = t_cockpit_00127.join(df_y, (df_y["book_id"] == t_cockpit_00127["book_id"]) &
                                           (df_y["month_id"] == t_cockpit_00127["month_id"]), "left") \
        .withColumn("b13", coalesce(df_y["b13"], t_cockpit_00127["b13"])) \
        .withColumn("b15", coalesce(df_y["b15"], t_cockpit_00127["b15"])) \
        .withColumn("b16", coalesce(df_y["b16"], t_cockpit_00127["b16"])) \
        .withColumn("b17", coalesce(df_y["b17"], t_cockpit_00127["b17"]))

    """
    三、营业利润
        b19  加：营业外收入　  6301
        b20  减：营业外支出　  6711
    """

    df_y = df_branch \
        .filter((col("busi_month") == i_month_id) & (col("index_para_name") == "利润总额")) \
        .groupBy("busi_month", "book_id") \
        .agg(
            sum(when(col("index_id").isin('6301', '630101'), col("index_value")).otherwise(0)).alias("b19"),
            sum(when(col("index_id").isin('6711', '671101'), col("index_value")).otherwise(0)).alias("b20")
        ) \
        .withColumnRenamed("busi_month", "month_id")

    t_cockpit_00127 = t_cockpit_00127.join(df_y, (df_y["book_id"] == t_cockpit_00127["book_id"]) &
                                           (df_y["month_id"] == t_cockpit_00127["month_id"]), "left") \
        .withColumn("b19", coalesce(df_y["b19"], t_cockpit_00127["b19"])) \
        .withColumn("b20", coalesce(df_y["b20"], t_cockpit_00127["b20"]))

    """
    四、利润总额
        b22    减：所得税费用　  6801
    """

    t_cockpit_book = spark.table("cf_busimg.T_BOOK_INFO")
    df_balance = spark.table("futures.t_hync65_account_balance")
    df_y = df_balance.filter((col("account_period") == i_month_id) &
                             (col("account_code").like('6801%'))) \
        .groupBy("book_code") \
        .agg(coalesce(sum(col("local_credit_sum"))).alias("b22")) \
        .withColumn("month_id", lit(i_month_id)) \
        .join(t_cockpit_book, df_y["book_code"] == t_cockpit_book["book_code"], "left_anti")  # 去除无效的book_code

    t_cockpit_00127 = t_cockpit_00127.join(df_y, t_cockpit_00127["book_id"] == df_y["book_code"], "left") \
        .withColumn("b22", coalesce(df_y["b22"], t_cockpit_00127["b22"]))

    """
    本月数据最后一步
        b5=b6+67+b8+b9+b10+b11
        b12=b13+b14+b15+b16+b17
        b18=b5-b12
        b21=B18+B19-B20
        b23=B21-B22
    """

    t_cockpit_00127 = t_cockpit_00127 \
        .withColumn("b5", t_cockpit_00127["b6"] + t_cockpit_00127["b7"] + t_cockpit_00127["b8"] + t_cockpit_00127["b9"] + t_cockpit_00127["b10"] + t_cockpit_00127["b11"]) \
        .withColumn("b12", t_cockpit_00127["b13"] + t_cockpit_00127["b14"] + t_cockpit_00127["b15"] + t_cockpit_00127["b16"] + t_cockpit_00127["b17"]) \
        .withColumn("b18", t_cockpit_00127["b5"] - t_cockpit_00127["b12"]) \
        .withColumn("b21", t_cockpit_00127["b18"] + t_cockpit_00127["b19"] - t_cockpit_00127["b20"]) \
        .withColumn("b23", t_cockpit_00127["b21"] - t_cockpit_00127["b22"])

    # ----------------------------------考核调出金额 begin--------------------------------------

    """
    --更新数据
        一、营业收入
        6   手续费及佣金净收入　  6021-6421-642102
        7  利息净收入             6011-6411
        8  手续费减免返还收入     6111
        9  管理费收入             6101
        10  投资收益              6061
        11  其他业务收入          6051
        二、营业支出
        13    应返佣金支出        660143
        14  税金及附加　          6403
        15  业务及管理费　        9999999
        16  资产减值损失　        6701
        17  其他业务成本          6402
        三、营业利润
        19  加：营业外收入　      6301
        20  减：营业外支出　      6711
        四、利润总额
        22 减：所得税费用　       6801
    """


