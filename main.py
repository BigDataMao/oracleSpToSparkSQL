# -*- coding: utf-8 -*-
import json
import logging
import os

from pyspark.sql.functions import udf, broadcast
from pyspark.sql.types import BooleanType, StringType

from utils.P_COCKPIT_00092_DATA import p_cockpit_00092_data
from utils.P_COCKPIT_00093_DATA import p_cockpit_00093_data
from utils.P_COCKPIT_00120_DATA import p_cockpit_00120_data
from utils.P_COCKPIT_00121_DATA import p_cockpit_00121_data
from utils.P_COCKPIT_00127_DATA import p_cockpit_00127_data
from utils.P_COCKPIT_00128_DATA import p_cockpit_00128_data
from utils.P_COCKPIT_00133_DATA import p_cockpit_00133_data
from utils.task_env import *
from utils.date_utils import *
from utils.parse_arguments import parse_arguments

logger = logging.getLogger('logger')
log_dir = '/opt/spark/logs'

if not os.path.exists(log_dir):
    os.makedirs(log_dir)
log_file = os.path.join(log_dir, 'oracleSpToSparkSQL.log')

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
filehandler = logging.FileHandler(log_file)

filehandler.setLevel(logging.INFO)
filehandler.setFormatter(formatter)

logger.addHandler(filehandler)


if __name__ == '__main__':
    spark = create_env()
    busi_date = parse_arguments()

    logger.info("接收到参数busi_date: %s", busi_date)
    pub_date_table = "edw.t10_pub_date"

    def get_pub_date_list():
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
        pub_date_list = [x["busi_date"] for x in dict_list_pub_date]
        pub_date_list.sort()
        logger.info("pub_date_list最后3个: %s", pub_date_list[-3:])
        return pub_date_list

    list_pub_date = get_pub_date_list()

    # 千万工程指标数据落地
    p_cockpit_00092_data(spark, busi_date)
    # 开户时间区间落地数据
    # p_cockpit_00093_data(spark, busi_date)
    # p_cockpit_00110_1_data(spark, busi_date)
    # p_cockpit_00110_before_data(spark, busi_date)
    # p_cockpit_00118_data(spark, busi_date)
    # p_cockpit_00119_data(spark, busi_date)
    # p_cockpit_00120_data(spark, busi_date)
    # p_cockpit_00121_data(spark, busi_date)
    # p_cockpit_00127_data(spark, busi_date)
    # p_cockpit_00128_data(spark, busi_date)
    # p_cockpit_00133_data(spark, busi_date)
    # p_cockpit_00135_data(spark, busi_date)
    # p_cockpit_00136_data(spark, busi_date)
    # p_cockpit_00137_data(spark, busi_date)
    # p_cockpit_00138_data(spark, busi_date)

    logger.info("测试日志")