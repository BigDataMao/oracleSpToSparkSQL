# -*- coding: utf-8 -*-
import json
import logging

from pyspark.sql.functions import udf, broadcast
from pyspark.sql.types import BooleanType, StringType

from utils.P_COCKPIT_00092_DATA import p_cockpit_00092_data
from utils.P_COCKPIT_00120_DATA import p_cockpit_00120_data
from utils.P_COCKPIT_00121_DATA import p_cockpit_00121_data
from utils.P_COCKPIT_00127_DATA import p_cockpit_00127_data
from utils.P_COCKPIT_00128_DATA import p_cockpit_00128_data
from utils.P_COCKPIT_00133_DATA import p_cockpit_00133_data
from utils.task_env import *
from utils.date_utils import *
from utils.parse_arguments import parse_arguments

logging.basicConfig(level=logging.INFO)


if __name__ == '__main__':
    spark = create_env()
    busi_date = parse_arguments()

    logging.info("接收到参数busi_date: %s", busi_date)

    # p_cockpit_00120_data(spark, busi_date)
    #
    # p_cockpit_00121_data(spark, busi_date)
    #
    # p_cockpit_00127_data(spark, busi_date)

    # p_cockpit_00128_data(spark, busi_date)

    # p_cockpit_00092_data(spark, busi_date)
    pub_date_table = "edw.t10_pub_date"
    df_pub_date = spark.table(pub_date_table) \
        .filter(
        (col("market_no") == "1") &
        (col("trade_flag") == "1")
    ).select(
        col("busi_date")
    )

    # 将DataFrame转换为JSON字符串并收集为列表
    json_list_pub_date = df_pub_date.toJSON().collect()

    # 解析JSON字符串并转换为字典列表
    dict_list_pub_date = [json.loads(json_str) for json_str in json_list_pub_date]

    # 提取字典列表中的"busi_date"值
    list_pub_date = [x["busi_date"] for x in dict_list_pub_date]

    # 注册UDF
    is_trade_day_check_udf = udf(lambda x: is_trade_day_check(list_pub_date, x), BooleanType())
    get_trade_date_udf = udf(lambda x, n: get_trade_date(list_pub_date, x, n), StringType())
    # 创建测试DataFrame
    test_df = spark.createDataFrame([
        ("20240420",),
        ("20240421",),
        ("20240422",),
        ("20240423",),
        ("20240424",)
    ], ["busi_date"])

    # 添加交易日期列
    result_df = test_df.withColumn(
        "trade_date",
        is_trade_day_check_udf(col("busi_date"))
    )

    result_df = result_df.withColumn(
        "trade_date_add_1",
        get_trade_date_udf(col("busi_date"), lit(1))
    )
    result_df.show()
