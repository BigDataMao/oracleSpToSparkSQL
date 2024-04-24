# -*- coding: utf-8 -*-
import logging

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

    print("前一个工作日", get_trade_date(spark, busi_date, -1))
    print("后一个工作日", get_trade_date(spark, busi_date, 1))
    print("对于非交易日的过去最近的交易日", get_trade_date(spark, busi_date, 0))

    logging.info("任务执行完成")
