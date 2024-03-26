# -*- coding: utf-8 -*-
import logging

from pyspark.sql.functions import col, min, max, when, sum, lit, coalesce

from utils.task_env import return_to_hive, drop_duplicate_columns


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

    t_cockpit_00127 = spark.table("ods.FUTURES_T_BOOK_INFO") \
        .filter(col("book_id").isNotNull()) \
        .select(lit(i_month_id).alias("month_id"), col("book_id"))

    return_to_hive(
        spark=spark,
        df_result=t_cockpit_00127,
        target_table="ddw.T_COCKPIT_00127",
        insert_mode="overwrite",
        partition_column="month_id",
        partition_value=i_month_id
    )

    t_cockpit_00127 = spark.table("ddw.T_COCKPIT_00127")

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
    df_branch = spark.table("ddw.T_HYNC65_INDEX_RESULT_BRANCH")
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
        .withColumn("b6", coalesce(df_y["b6"], t_cockpit_00127["b6"])) \
        .withColumn("b7", coalesce(df_y["b7"], t_cockpit_00127["b7"])) \
        .withColumn("b8", coalesce(df_y["b8"], t_cockpit_00127["b8"])) \
        .withColumn("b9", coalesce(df_y["b9"], t_cockpit_00127["b9"])) \
        .withColumn("b10", coalesce(df_y["b10"], t_cockpit_00127["b10"])) \
        .withColumn("b11", coalesce(df_y["b11"], t_cockpit_00127["b11"]))

    logging.info("第一阶段数据处理完成")
    t_cockpit_00127 = drop_duplicate_columns(t_cockpit_00127)

    """
    二、营业支出
        b13  应返佣金支出   0
        b14  税金及附加　   ROUND(B6*0.0072,2)
        b15  业务及管理费　 6601+6404
        b16  资产减值损失　 6701
        b17  其他业务成本   6402
    """

    logging.info("t_cockpit_00127所有的列: %s", t_cockpit_00127.columns)
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

    logging.info("第二阶段数据处理完成")
    t_cockpit_00127 = drop_duplicate_columns(t_cockpit_00127)

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

    logging.info("第三阶段数据处理完成")
    t_cockpit_00127 = drop_duplicate_columns(t_cockpit_00127)

    """
    四、利润总额
        b22    减：所得税费用　  6801
    """

    t_cockpit_book = spark.table("ddw.T_BOOK_INFO")
    df_balance = spark.table("ods.FUTURES_t_hync65_account_balance")
    df_y = df_balance.filter((col("account_period") == i_month_id) &
                             (col("account_code").like('6801%'))) \
        .groupBy("book_code") \
        .agg(coalesce(sum(col("local_credit_sum"))).alias("b22")) \
        .withColumn("month_id", lit(i_month_id)) \
        .join(t_cockpit_book, df_y["book_code"] == t_cockpit_book["book_code"], "left_anti")  # 去除无效的book_code

    t_cockpit_00127 = t_cockpit_00127.join(df_y, (df_y["book_id"] == t_cockpit_00127["book_id"]) &
                                           (df_y["month_id"] == t_cockpit_00127["month_id"]), "left") \
        .withColumn("b22", coalesce(df_y["b22"], t_cockpit_00127["b22"]))

    logging.info("第四阶段数据处理完成")
    t_cockpit_00127 = drop_duplicate_columns(t_cockpit_00127)

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

    logging.info("最后一步数据处理完成")
    t_cockpit_00127 = drop_duplicate_columns(t_cockpit_00127)

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

    df_y = df_branch \
        .filter((col("busi_month") == lit(i_month_id)) &
                (col("index_para_name").isin('营业收入-考核调出金额',
                                             '营业支出-考核调出金额',
                                             '营业利润-考核调出金额',
                                             '利润总额-考核调出金额'))) \
        .groupBy("busi_month", "book_id") \
        .agg(sum(when(col("index_id").isin('6021', '6421', '642102'), col("index_value")).otherwise(0)).alias("c6"),
             sum(when(col("index_id").isin('6011', '6411'), col("index_value")).otherwise(0)).alias("c7"),
             sum(when(col("index_id") == lit('6111'), col("index_value")).otherwise(0)).alias("c8"),
             sum(when(col("index_id") == lit('6101'), col("index_value")).otherwise(0)).alias("c9"),
             sum(when(col("index_id") == lit('6061'), col("index_value")).otherwise(0)).alias("c10"),
             sum(when(col("index_id") == lit('6051'), col("index_value")).otherwise(0)).alias("c11"),
             sum(when(col("index_id") == lit('660143'), col("index_value")).otherwise(0)).alias("c13"),
             sum(when(col("index_id") == lit('6403'), col("index_value")).otherwise(0)).alias("c14"),
             sum(when(col("index_id") == lit('9999999'), col("index_value")).otherwise(0)).alias("c15"),
             sum(when(col("index_id") == lit('6701'), col("index_value")).otherwise(0)).alias("c16"),
             sum(when(col("index_id") == lit('6402'), col("index_value")).otherwise(0)).alias("c17"),
             sum(when(col("index_id") == lit('6301'), col("index_value")).otherwise(0)).alias("c19"),
             sum(when(col("index_id") == lit('6711'), col("index_value")).otherwise(0)).alias("c20"),
             sum(when(col("index_id") == lit('6801'), col("index_value")).otherwise(0)).alias("c22")) \
        .withColumnRenamed("busi_month", "month_id")

    t_cockpit_00127 = t_cockpit_00127.join(df_y, (df_y["book_id"] == t_cockpit_00127["book_id"]) &
                                           (df_y["month_id"] == t_cockpit_00127["month_id"]), "left") \
        .withColumn("c6", coalesce(df_y["c6"], t_cockpit_00127["c6"])) \
        .withColumn("c7", coalesce(df_y["c7"], t_cockpit_00127["c7"])) \
        .withColumn("c8", coalesce(df_y["c8"], t_cockpit_00127["c8"])) \
        .withColumn("c9", coalesce(df_y["c9"], t_cockpit_00127["c9"])) \
        .withColumn("c10", coalesce(df_y["c10"], t_cockpit_00127["c10"])) \
        .withColumn("c11", coalesce(df_y["c11"], t_cockpit_00127["c11"])) \
        .withColumn("c13", coalesce(df_y["c13"], t_cockpit_00127["c13"])) \
        .withColumn("c14", coalesce(df_y["c14"], t_cockpit_00127["c14"])) \
        .withColumn("c15", coalesce(df_y["c15"], t_cockpit_00127["c15"])) \
        .withColumn("c16", coalesce(df_y["c16"], t_cockpit_00127["c16"])) \
        .withColumn("c17", coalesce(df_y["c17"], t_cockpit_00127["c17"])) \
        .withColumn("c19", coalesce(df_y["c19"], t_cockpit_00127["c19"])) \
        .withColumn("c20", coalesce(df_y["c20"], t_cockpit_00127["c20"])) \
        .withColumn("c22", coalesce(df_y["c22"], t_cockpit_00127["c22"]))

    logging.info("考核调出金额数据处理完成")
    t_cockpit_00127 = drop_duplicate_columns(t_cockpit_00127)

    # ----------------------------------考核调出金额 end----------------------------------------

    # ----------------------------------考核调入金额 begin--------------------------------------

    """
    一、营业收入
    d6   手续费及佣金净收入　  6021-6421-642102
    d7  利息净收入             6011-6411
    d8  手续费减免返还收入     6111
    d9  管理费收入             6101
    d10  投资收益              6061
    d11  其他业务收入          6051
    二、营业支出
    d13    应返佣金支出        660143
    d14  税金及附加　          6403
    d15  业务及管理费　        9999999
    d16  资产减值损失　        6701
    d17  其他业务成本          6402
    三、营业利润
    d19  加：营业外收入　      6301
    d20  减：营业外支出　      6711
    四、利润总额
    d22 减：所得税费用　       6801
    """

    df_y = df_branch \
        .filter((col("busi_month") == lit(i_month_id)) &
                (col("index_para_name").isin('营业收入-考核调入金额',
                                             '营业支出-考核调入金额',
                                             '营业利润-考核调入金额',
                                             '利润总额-考核调入金额'))) \
        .groupBy("busi_month", "book_id") \
        .agg(sum(when(col("index_id").isin('6021', '6421', '642102'), col("index_value")).otherwise(0)).alias("d6"),
             sum(when(col("index_id").isin('6011', '6411'), col("index_value")).otherwise(0)).alias("d7"),
             sum(when(col("index_id") == lit('6111'), col("index_value")).otherwise(0)).alias("d8"),
             sum(when(col("index_id") == lit('6101'), col("index_value")).otherwise(0)).alias("d9"),
             sum(when(col("index_id") == lit('6061'), col("index_value")).otherwise(0)).alias("d10"),
             sum(when(col("index_id") == lit('6051'), col("index_value")).otherwise(0)).alias("d11"),
             sum(when(col("index_id") == lit('660143'), col("index_value")).otherwise(0)).alias("d13"),
             sum(when(col("index_id") == lit('6403'), col("index_value")).otherwise(0)).alias("d14"),
             sum(when(col("index_id") == lit('9999999'), col("index_value")).otherwise(0)).alias("d15"),
             sum(when(col("index_id") == lit('6701'), col("index_value")).otherwise(0)).alias("d16"),
             sum(when(col("index_id") == lit('6402'), col("index_value")).otherwise(0)).alias("d17"),
             sum(when(col("index_id") == lit('6301'), col("index_value")).otherwise(0)).alias("d19"),
             sum(when(col("index_id") == lit('6711'), col("index_value")).otherwise(0)).alias("d20"),
             sum(when(col("index_id") == lit('6801'), col("index_value")).otherwise(0)).alias("d22")) \
        .withColumnRenamed("busi_month", "month_id")

    t_cockpit_00127 = t_cockpit_00127.join(df_y, (df_y["book_id"] == t_cockpit_00127["book_id"]) &
                                           (df_y["month_id"] == t_cockpit_00127["month_id"]), "left") \
        .withColumn("d6", coalesce(df_y["d6"], t_cockpit_00127["d6"])) \
        .withColumn("d7", coalesce(df_y["d7"], t_cockpit_00127["d7"])) \
        .withColumn("d8", coalesce(df_y["d8"], t_cockpit_00127["d8"])) \
        .withColumn("d9", coalesce(df_y["d9"], t_cockpit_00127["d9"])) \
        .withColumn("d10", coalesce(df_y["d10"], t_cockpit_00127["d10"])) \
        .withColumn("d11", coalesce(df_y["d11"], t_cockpit_00127["d11"])) \
        .withColumn("d13", coalesce(df_y["d13"], t_cockpit_00127["d13"])) \
        .withColumn("d14", coalesce(df_y["d14"], t_cockpit_00127["d14"])) \
        .withColumn("d15", coalesce(df_y["d15"], t_cockpit_00127["d15"])) \
        .withColumn("d16", coalesce(df_y["d16"], t_cockpit_00127["d16"])) \
        .withColumn("d17", coalesce(df_y["d17"], t_cockpit_00127["d17"])) \
        .withColumn("d19", coalesce(df_y["d19"], t_cockpit_00127["d19"])) \
        .withColumn("d20", coalesce(df_y["d20"], t_cockpit_00127["d20"])) \
        .withColumn("d22", coalesce(df_y["d22"], t_cockpit_00127["d22"]))

    logging.info("考核调入金额数据处理完成")
    t_cockpit_00127 = drop_duplicate_columns(t_cockpit_00127)

    # ----------------------------------考核调入金额 end----------------------------------------

    # 调整合计=考核调出金额+考核调入金额
    t_cockpit_00127 = t_cockpit_00127 \
        .filter((col("month_id") == i_month_id)) \
        .withColumn("e5", col("c5") + col("d5")) \
        .withColumn("e6", col("c6") + col("d6")) \
        .withColumn("e7", col("c7") + col("d7")) \
        .withColumn("e8", col("c8") + col("d8")) \
        .withColumn("e9", col("c9") + col("d9")) \
        .withColumn("e10", col("c10") + col("d10")) \
        .withColumn("e11", col("c11") + col("d11")) \
        .withColumn("e12", col("c12") + col("d12")) \
        .withColumn("e13", col("c13") + col("d13")) \
        .withColumn("e14", col("c14") + col("d14")) \
        .withColumn("e15", col("c15") + col("d15")) \
        .withColumn("e16", col("c16") + col("d16")) \
        .withColumn("e17", col("c17") + col("d17")) \
        .withColumn("e18", col("c18") + col("d18")) \
        .withColumn("e19", col("c19") + col("d19")) \
        .withColumn("e20", col("c20") + col("d20")) \
        .withColumn("e21", col("c21") + col("d21")) \
        .withColumn("e22", col("c22") + col("d22")) \
        .withColumn("e23", col("c23") + col("d23"))

    # 实际金额=本月金额+调整合计
    t_cockpit_00127 = t_cockpit_00127 \
        .withColumn("f5", col("b5") + col("e5")) \
        .withColumn("f6", col("b6") + col("e6")) \
        .withColumn("f7", col("b7") + col("e7")) \
        .withColumn("f8", col("b8") + col("e8")) \
        .withColumn("f9", col("b9") + col("e9")) \
        .withColumn("f10", col("b10") + col("e10")) \
        .withColumn("f11", col("b11") + col("e11")) \
        .withColumn("f12", col("b12") + col("e12")) \
        .withColumn("f13", col("b13") + col("e13")) \
        .withColumn("f14", col("b14") + col("e14")) \
        .withColumn("f15", col("b15") + col("e15")) \
        .withColumn("f16", col("b16") + col("e16")) \
        .withColumn("f17", col("b17") + col("e17")) \
        .withColumn("f18", col("b18") + col("e18")) \
        .withColumn("f19", col("b19") + col("e19")) \
        .withColumn("f20", col("b20") + col("e20")) \
        .withColumn("f21", col("b21") + col("e21")) \
        .withColumn("f22", col("b22") + col("e22")) \
        .withColumn("f23", col("b23") + col("e23"))

    logging.info("实际金额数据处理完成")
    t_cockpit_00127 = drop_duplicate_columns(t_cockpit_00127)

    # ----------------------------------本年金额 begin--------------------------------------

    """
    本年金额为，从本年1月累计到当前月份的金额合计
    """

    # 必须先落一次表
    return_to_hive(spark,
                   t_cockpit_00127,
                   "ddw.T_COCKPIT_00127",
                   "overwrite",
                   partition_column="month_id",
                   partition_value=i_month_id
                   )

    t_cockpit_00127 = spark.table("ddw.T_COCKPIT_00127")

    df_y = t_cockpit_00127.filter((col("month_id").between(v_begin_month, v_end_month))) \
        .groupBy("book_id") \
        .agg(sum(col("b5")).alias("g5"),
             sum(col("g6")).alias("g6"),
             sum(col("g7")).alias("g7"),
             sum(col("g8")).alias("g8"),
             sum(col("g9")).alias("g9"),
             sum(col("g10")).alias("g10"),
             sum(col("g11")).alias("g11"),
             sum(col("g12")).alias("g12"),
             sum(col("g13")).alias("g13"),
             sum(col("g14")).alias("g14"),
             sum(col("g15")).alias("g15"),
             sum(col("g16")).alias("g16"),
             sum(col("g17")).alias("g17"),
             sum(col("g18")).alias("g18"),
             sum(col("g19")).alias("g19"),
             sum(col("g20")).alias("g20"),
             sum(col("g21")).alias("g21"),
             sum(col("g22")).alias("g22"),
             sum(col("g23")).alias("g23"))

    t_cockpit_00127 = t_cockpit_00127.join(df_y, (df_y["book_id"] == t_cockpit_00127["book_id"]) &
                                           (df_y["month_id"] == t_cockpit_00127["month_id"]), "left") \
        .withColumn("g5", col("g5") + col("e5")) \
        .withColumn("g6", coalesce(df_y["g6"], t_cockpit_00127["g6"])) \
        .withColumn("g7", coalesce(df_y["g7"], t_cockpit_00127["g7"])) \
        .withColumn("g8", coalesce(df_y["g8"], t_cockpit_00127["g8"])) \
        .withColumn("g9", coalesce(df_y["g9"], t_cockpit_00127["g9"])) \
        .withColumn("g10", coalesce(df_y["g10"], t_cockpit_00127["g10"])) \
        .withColumn("g11", coalesce(df_y["g11"], t_cockpit_00127["g11"])) \
        .withColumn("g12", coalesce(df_y["g12"], t_cockpit_00127["g12"])) \
        .withColumn("g13", coalesce(df_y["g13"], t_cockpit_00127["g13"])) \
        .withColumn("g14", coalesce(df_y["g14"], t_cockpit_00127["g14"])) \
        .withColumn("g15", coalesce(df_y["g15"], t_cockpit_00127["g15"])) \
        .withColumn("g16", coalesce(df_y["g16"], t_cockpit_00127["g16"])) \
        .withColumn("g17", coalesce(df_y["g17"], t_cockpit_00127["g17"])) \
        .withColumn("g18", coalesce(df_y["g18"], t_cockpit_00127["g18"])) \
        .withColumn("g19", coalesce(df_y["g19"], t_cockpit_00127["g19"])) \
        .withColumn("g20", coalesce(df_y["g20"], t_cockpit_00127["g20"])) \
        .withColumn("g21", coalesce(df_y["g21"], t_cockpit_00127["g21"])) \
        .withColumn("g22", coalesce(df_y["g22"], t_cockpit_00127["g22"])) \
        .withColumn("g23", coalesce(df_y["g23"], t_cockpit_00127["g23"])) \

    logging.info("本年金额数据处理完成")
    t_cockpit_00127 = drop_duplicate_columns(t_cockpit_00127)

    # ----------------------------------本年金额 end--------------------------------------

    # 重新落表
    return_to_hive(spark,
                   t_cockpit_00127,
                   "ddw.T_COCKPIT_00127",
                   "overwrite",
                   partition_column="month_id",
                   partition_value=i_month_id
                   )

    logging.info("p_cockpit_00127_data.py执行完成")


