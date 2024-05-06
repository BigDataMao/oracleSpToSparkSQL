# -*- coding: utf-8 -*-
import logging

from pyspark.sql.functions import col, min, max, when, sum, lit, coalesce, round

from utils.task_env import return_to_hive, update_dataframe, log


@log
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

    t_cockpit_00127 = spark.table("edw.H16_BOOK_INFO") \
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

    t_cockpit_00127 = spark.table("ddw.T_COCKPIT_00127").alias("t")

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
    df_branch = spark.table("edw.H16_HYNC65_INDEX_RESULT_BRANCH")
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
        .withColumnRenamed("busi_month", "month_id") \
        .alias("y")

    t_cockpit_00127 = update_dataframe(
        df_to_update=t_cockpit_00127,
        df_use_me=df_y,
        join_columns=["book_id", "month_id"],
        update_columns=["b6", "b7", "b8", "b9", "b10", "b11"]
    )

    logging.info("第一阶段数据处理完成")

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
        .withColumn("b13", lit(0)) \
        .withColumnRenamed("busi_month", "month_id")\
        .alias("y")

    t_cockpit_00127 = update_dataframe(
        df_to_update=t_cockpit_00127,
        df_use_me=df_y,
        join_columns=["book_id", "month_id"],
        update_columns=["b13", "b15", "b16", "b17"]
    )

    logging.info("第二阶段数据处理完成")

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
        .withColumnRenamed("busi_month", "month_id")\
        .alias("y")

    t_cockpit_00127 = update_dataframe(
        df_to_update=t_cockpit_00127,
        df_use_me=df_y,
        join_columns=["book_id", "month_id"],
        update_columns=["b19", "b20"]
    )

    logging.info("第三阶段数据处理完成")

    """
    四、利润总额
        b22    减：所得税费用　  6801
    """

    t_cockpit_book = spark.table("ddw.t_cockpit_book")
    df_balance = spark.table("edw.h16_hync65_account_balance")
    df_y = df_balance.filter((col("account_period") == i_month_id) &
                             (col("account_code").like('6801%'))) \
        .groupBy("book_code") \
        .agg(coalesce(sum(col("local_credit_sum"))).alias("b22")) \
        .withColumn("month_id", lit(i_month_id)) \
        .join(t_cockpit_book, df_balance["book_code"] == t_cockpit_book["book_code"], "left_anti")\
        .withColumnRenamed("book_code", "book_id")\
        .alias("y")

    t_cockpit_00127 = update_dataframe(
        df_to_update=t_cockpit_00127,
        df_use_me=df_y,
        join_columns=["book_id", "month_id"],
        update_columns=["b22"]
    )

    logging.info("第四阶段数据处理完成")

    """
    本月数据最后一步
        b5=b6+67+b8+b9+b10+b11
        b12=b13+b14+b15+b16+b17
        b18=b5-b12
        b21=B18+B19-B20
        b23=B21-B22
    """

    t_cockpit_00127 = t_cockpit_00127 \
        .withColumn("b5_new", t_cockpit_00127["b6"] + t_cockpit_00127["b7"]
                    + t_cockpit_00127["b8"] + t_cockpit_00127["b9"]
                    + t_cockpit_00127["b10"] + t_cockpit_00127["b11"]) \
        .withColumn("b12_new", t_cockpit_00127["b13"] + t_cockpit_00127["b14"]
                    + t_cockpit_00127["b15"] + t_cockpit_00127["b16"]
                    + t_cockpit_00127["b17"]) \
        .withColumn("b18_new", t_cockpit_00127["b5"] - t_cockpit_00127["b12"]) \
        .withColumn("b21_new", t_cockpit_00127["b18"] + t_cockpit_00127["b19"]
                    - t_cockpit_00127["b20"]) \
        .withColumn("b23_new", t_cockpit_00127["b21"] - t_cockpit_00127["b22"]) \
        .drop("b5", "b12", "b18", "b21", "b23") \
        .withColumnRenamed("b5_new", "b5") \
        .withColumnRenamed("b12_new", "b12") \
        .withColumnRenamed("b18_new", "b18") \
        .withColumnRenamed("b21_new", "b21") \
        .withColumnRenamed("b23_new", "b23")

    logging.info("最后一步数据处理完成")

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
        .withColumnRenamed("busi_month", "month_id")\
        .alias("y")

    t_cockpit_00127 = update_dataframe(
        df_to_update=t_cockpit_00127,
        df_use_me=df_y,
        join_columns=["book_id", "month_id"],
        update_columns=["c6", "c7", "c8", "c9", "c10", "c11", "c13", "c14", "c15", "c16", "c17", "c19", "c20", "c22"]
    )
    logging.info("考核调出金额数据处理完成")

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
        .withColumnRenamed("busi_month", "month_id")\
        .alias("y")

    t_cockpit_00127 = update_dataframe(
        df_to_update=t_cockpit_00127,
        df_use_me=df_y,
        join_columns=["book_id", "month_id"],
        update_columns=["d6", "d7", "d8", "d9", "d10", "d11", "d13", "d14", "d15", "d16", "d17", "d19", "d20", "d22"]
    )

    logging.info("考核调入金额数据处理完成")

    # ----------------------------------考核调入金额 end----------------------------------------
    str_nums = [str(num) for num in range(5, 24)]

    for i in str_nums:
        t_cockpit_00127 = t_cockpit_00127 \
            .withColumn("e"+i+"_new", col("c"+i) + col("d"+i))\
            .drop("e"+i)\
            .withColumnRenamed("e"+i+"_new", "e"+i)\
            .withColumn("f"+i+"_new", col("b"+i) + col("e"+i))\
            .drop("f"+i)\
            .withColumnRenamed("f"+i+"_new", "f"+i)

    logging.info("实际金额数据处理完成")

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
             sum(col("g23")).alias("g23"))\
        .withColumn("month_id", lit(i_month_id))\
        .alias("y")

    t_cockpit_00127 = update_dataframe(
        df_to_update=t_cockpit_00127,
        df_use_me=df_y,
        join_columns=["book_id", "month_id"],
        update_columns=["g5", "g6", "g7", "g8", "g9", "g10", "g11", "g12", "g13", "g14", "g15", "g16", "g17",
                        "g18", "g19", "g20", "g21", "g22", "g23"]
    ).withColumn("g5_new", col("g5") + col("e5"))\
        .drop("g5")\
        .withColumnRenamed("g5_new", "g5")

    logging.info("本年金额数据处理完成")

    # ----------------------------------本年金额 end--------------------------------------

    # 重新落表
    return_to_hive(spark,
                   t_cockpit_00127,
                   "ddw.T_COCKPIT_00127",
                   "overwrite",
                   partition_column="month_id",
                   partition_value=i_month_id
                   )
