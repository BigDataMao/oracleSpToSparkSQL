# -*- coding: utf-8 -*-
import json
import sys
from imp import reload

from config import Config
from utils.P_BRP_06008_FGG_DATA_HY import p_brp_06008_fgg_data_hy
from utils.P_BRP_06009_data_fgg_hy import p_brp_06009_data_fgg_hy
from utils.P_BRP_06010_DATA import p_brp_06010_data
from utils.P_BRP_06010_D_DATA import p_brp_06010_d_data
from utils.date_utils import *
from utils.io_utils.path_utils import get_project_path
from utils.parse_arguments import parse_arguments
from utils.task_env import create_env

reload(sys)
sys.setdefaultencoding("utf-8")


if __name__ == '__main__':
    spark = create_env()
    busi_date = parse_arguments()
    config = Config()
    logger = config.get_logger()
    start_time = datetime.now()

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
        logger.info("最大交易日期为: %s", pub_date_list[-1:])
        return pub_date_list

    list_pub_date = get_pub_date_list()

    v_busi_month = busi_date[0:6]
    v_begin_date = v_busi_month + "01"
    v_end_date = busi_date

    v_end_date1 = get_date_period_and_days(spark=spark, busi_month=v_busi_month, is_trade_day=True)[1]
    v_end_date2 = get_date_period_and_days(spark=spark, busi_month=v_busi_month, is_trade_day=False)[1]

    if v_end_date != v_end_date1:
        v_sett_end_date = v_end_date
    else:
        v_sett_end_date = v_end_date2

    v_sett_end_date_d = get_trade_date(list_pub_date, busi_date, 1)
    v_nature_date = get_date_period_and_days(spark=spark, begin_date=busi_date, end_date=v_sett_end_date_d, is_trade_day=False)[1]
    if not v_nature_date:
        v_nature_date = busi_date

    # 资金对账表日和落地数据
    p_brp_06008_fgg_data_hy(spark, list_pub_date, v_end_date, v_nature_date)

    # 交易统计表日和落地数据
    p_brp_06010_d_data(spark, list_pub_date, busi_date)

    # 交易统计表月度数据落地
    p_brp_06010_data(spark, list_pub_date, v_begin_date, v_end_date)

    # 资金对账表月度数据落地
    p_brp_06009_data_fgg_hy(spark, list_pub_date, v_begin_date, v_sett_end_date)

    end_time = datetime.now()
    duration = end_time - start_time
    duration = divmod(duration.seconds, 60)
    logger.info("函数 %s 执行时间: %s 分 %s 秒", "main_hy1", duration[0], duration[1])


