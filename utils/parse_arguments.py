# -*- coding: utf-8 -*-
import argparse


def parse_arguments():
    # 解析命令行所传参数
    parser = argparse.ArgumentParser()  # 创建解析参数的对象
    parser.add_argument('--busi_date', help='business date parameter', default=None)  # 添加参数细节
    args = parser.parse_args()  # 获取参数
    return args.busi_date
