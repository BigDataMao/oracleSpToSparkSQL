# -*- coding: utf-8 -*-

from utils.P_COCKPIT_00120_DATA import p_cockpit_00120_data
from utils.P_COCKPIT_00121_DATA import p_cockpit_00121_data
from utils.task_env import create_env
from utils.parse_arguments import parse_arguments

if __name__ == '__main__':
    spark = create_env()
    busi_date = parse_arguments()

    print("busi_date: ", busi_date)

    p_cockpit_00120_data(spark, busi_date)

    p_cockpit_00121_data(spark, busi_date)
