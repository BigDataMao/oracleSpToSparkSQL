# -*- coding: utf-8 -*-

"""
专门处理日期参数的工具类
"""


def get_last_year_begin_month(busi_date):
    """
    获取上一年的开始月份
    :param busi_date: 业务日期,可以是任意日期,但必须是纯数字的字符串
    """

    year = busi_date[:4]
    last_year = str(int(year) - 1)
    return last_year + "01"


def get_last_year_end_month(busi_date):
    """
    获取上一年的结束月份
    """

    year = busi_date[:4]
    last_year = str(int(year) - 1)
    return last_year + "12"


