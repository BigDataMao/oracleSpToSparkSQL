# -*- coding: utf-8 -*-
import sys
from imp import reload

from utils.P_COCKPIT_00092_DATA import p_cockpit_00092_data
from utils.P_COCKPIT_00093_DATA import p_cockpit_00093_data
from utils.P_COCKPIT_00110_1_DATA import p_cockpit_00110_1_data
from utils.P_COCKPIT_00118_DATA import p_cockpit_00118_data
from utils.P_COCKPIT_00119_DATA import p_cockpit_00119_data
from utils.P_COCKPIT_00120_DATA import p_cockpit_00120_data
from utils.P_COCKPIT_00121_DATA import p_cockpit_00121_data
from utils.P_COCKPIT_00127_DATA import p_cockpit_00127_data
from utils.P_COCKPIT_00128_DATA import p_cockpit_00128_data
from utils.P_COCKPIT_00133_DATA import p_cockpit_00133_data
from utils.P_COCKPIT_00135_DATA import p_cockpit_00135_data
from utils.P_COCKPIT_00136_DATA import p_cockpit_00136_data
from utils.P_COCKPIT_00137_DATA import p_cockpit_00137_data
from utils.P_COCKPIT_00138_DATA import p_cockpit_00138_data
from utils.parse_arguments import parse_arguments
from utils.task_env import *


config = Config(get_project_path() + "/config.json")


if __name__ == '__main__':
    spark = create_env()
    busi_date = parse_arguments()
    logger = config.get_logger()
    start_time = datetime.datetime.now()

    # 千万工程”指标落地
    p_cockpit_00092_data(spark, busi_date)

    # 千万工程“开户时间区间落地
    p_cockpit_00093_data(spark, busi_date)

    # 业务人员营业部维护 初始化
    p_cockpit_00110_1_data(spark, busi_date)

    # 收入分配表(最终呈现表)FOF产品 汇总数据 数据落地，到月份
    p_cockpit_00118_data(spark, busi_date)

    # 收入分配表(最终呈现表) foF产品 明细数据 数据落地，到月份
    p_cockpit_00119_data(spark, busi_date)

    # 收入分配表(最终呈现表) 普通产品 汇总数据 数据落地，到月份
    p_cockpit_00120_data(spark, busi_date)

    # 收入分配表(最终呈现表) 普通产品 明细数据 数据落地，到月份
    p_cockpit_00121_data(spark, busi_date)

    # 财务内核表-调整前数据落地
    p_cockpit_00127_data(spark, busi_date)

    # 投资者保障基金调整表-初始化数据生成
    p_cockpit_00128_data(spark, busi_date)

    # 经纪业务收入、权益情况-数据落地
    p_cockpit_00133_data(spark, busi_date)

    # 年度任务目标填报表-校验
    p_cockpit_00135_data(spark, busi_date)

    # 经营目标责任书-数据生成
    p_cockpit_00136_data(spark, busi_date)

    # 经营目标完成情况-数据落地
    p_cockpit_00137_data(spark, busi_date)

    # 经营目标考核情况-数据落地
    p_cockpit_00138_data(spark, busi_date)

    # 生成日志消息
    end_time = datetime.datetime.now()
    duration = end_time - start_time
    # 转成分秒,整数
    duration = divmod(duration.seconds, 60)
    logger.info("任务全部完成,执行耗时: %s 分 %s 秒", duration[0], duration[1])





