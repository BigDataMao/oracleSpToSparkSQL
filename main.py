# -*- coding: utf-8 -*-
import logging

from utils.P_COCKPIT_00120_DATA import p_cockpit_00120_data
from utils.P_COCKPIT_00121_DATA import p_cockpit_00121_data
from utils.P_COCKPIT_00127_DATA import p_cockpit_00127_data
from utils.P_COCKPIT_00128_DATA import p_cockpit_00128_data
from utils.task_env import create_env
from utils.parse_arguments import parse_arguments

logging.basicConfig(level=logging.INFO)


if __name__ == '__main__':
    spark = create_env()
    busi_date = parse_arguments()

    logging.info("接收到参数busi_date: ", busi_date)

    p_cockpit_00120_data(spark, busi_date)

    p_cockpit_00121_data(spark, busi_date)

    p_cockpit_00127_data(spark, busi_date)

    p_cockpit_00128_data(spark, busi_date)

    logging.info("任务执行完成")
