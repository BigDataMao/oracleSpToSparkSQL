# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, lit

from config import Config
from utils.P_COCKPIT_INDEX_MID_VALUE import p_cockpit_index_mid_value
from utils.date_utils import get_month_str
from utils.io_utils.common_uitls import to_color_str
from utils.task_env import log, return_to_hive

logger = Config().get_logger()


@log
def p_index_result_data_branch(spark: SparkSession, busi_date):
    v_busi_month = busi_date[:6]
    v_begin_month = get_month_str(v_busi_month, -3)

    def get_tmp_month():
        months = spark.table("edw.t10_pub_date").where(
            (col("market_no") == "1") &
            (col("trade_flag") == "1") &
            (substring(col("busi_date"), 1, 6).between(
                lit(v_begin_month), lit(v_busi_month)
            ))
        ).select(
            substring(col("busi_date"), 1, 6).alias("busi_month")
        ).distinct().collect()

        out = [item["busi_month"] for item in months]
        out.sort()
        return out

    def get_tmp_index(index_para_name: list):
        tmp_index_rows = spark.table("ddw.t_cockpit_account_code").where(
            col("index_para_name").isin(index_para_name)
        ).select(
            "index_para_name",
            "index_id",
            "index_name",
            "cal_direction"
        ).collect()

        return [row.asDict() for row in tmp_index_rows]

    def insert_data(month, index_row: dict, df_in):
        df_out = df_in.alias('t').select(
            col("t.book_id"),
            lit(month).alias("busi_month"),
            lit(index_row["index_para_name"]).alias("index_para_name"),
            lit(index_row["index_id"]).alias("index_id"),
            lit(index_row["index_name"]).alias("index_name"),
            col("t.index_value")
        )

        return_to_hive(
            spark=spark,
            df_result=df_out,
            target_table="ddw.t_hync65_index_result_branch",
            insert_mode="append"
        )

    tmp_month_list = get_tmp_month()

    # 删除分区数据
    for tmp_month in tmp_month_list:
        spark.sql(f"ALTER TABLE ddw.t_hync65_index_result_branch DROP IF EXISTS PARTITION (busi_month='{tmp_month}')")

    # 营业收入
    logger.info(to_color_str("开始执行营业收入", "blue"))
    tmp_index_list = get_tmp_index(["营业收入"])
    for tmp_month in tmp_month_list:
        for tmp_index in tmp_index_list:
            logger.info(to_color_str("开始执行tmp_month: %s, tmp_index: %s", "blue"), tmp_month, tmp_index["index_id"])
            df_mid = p_cockpit_index_mid_value(spark, tmp_month, tmp_index["index_id"], tmp_index["cal_direction"])
            insert_data(tmp_month, tmp_index, df_mid)

    # 营业支出
    logger.info(to_color_str("开始执行营业支出", "blue"))
    tmp_index_list = get_tmp_index(["营业支出"])
    for tmp_month in tmp_month_list:
        for tmp_index in tmp_index_list:
            logger.info(to_color_str("开始执行tmp_month: %s, tmp_index: %s", "blue"), tmp_month, tmp_index["index_id"])
            df_mid = p_cockpit_index_mid_value(spark, tmp_month, tmp_index["index_id"], tmp_index["cal_direction"])
            insert_data(tmp_month, tmp_index, df_mid)

    # 利润总额
    logger.info(to_color_str("开始执行利润总额", "blue"))
    tmp_index_list = get_tmp_index(["利润总额"])
    for tmp_month in tmp_month_list:
        for tmp_index in tmp_index_list:
            logger.info(to_color_str("开始执行tmp_month: %s, tmp_index: %s", "blue"), tmp_month, tmp_index["index_id"])
            df_mid = p_cockpit_index_mid_value(spark, tmp_month, tmp_index["index_id"], tmp_index["cal_direction"])
            insert_data(tmp_month, tmp_index, df_mid)

    # 考核调出金额
    logger.info(to_color_str("开始执行考核调出金额", "blue"))
    tmp_index_list = get_tmp_index(["营业收入-考核调出金额", "营业支出-考核调出金额", "营业利润-考核调出金额", "利润总额-考核调出金额"])
    for tmp_month in tmp_month_list:
        for tmp_index in tmp_index_list:
            logger.info(to_color_str("开始执行tmp_month: %s, tmp_index: %s", "blue"), tmp_month, tmp_index["index_id"])
            df_mid = p_cockpit_index_mid_value(spark, tmp_month, tmp_index["index_id"], tmp_index["cal_direction"], "调减")
            insert_data(tmp_month, tmp_index, df_mid)

    # 考核调入金额
    logger.info(to_color_str("开始执行考核调入金额", "blue"))
    tmp_index_list = get_tmp_index(["营业收入-考核调入金额", "营业支出-考核调入金额", "营业利润-考核调入金额", "利润总额-考核调入金额"])
    for tmp_month in tmp_month_list:
        for tmp_index in tmp_index_list:
            logger.info(to_color_str("开始执行tmp_month: %s, tmp_index: %s", "blue"), tmp_month, tmp_index["index_id"])
            df_mid = p_cockpit_index_mid_value(spark, tmp_month, tmp_index["index_id"], tmp_index["cal_direction"], "调增")
            insert_data(tmp_month, tmp_index, df_mid)
