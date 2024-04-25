# -*- coding: utf-8 -*-

"""
专门处理日期参数的工具类
"""
from datetime import datetime, timedelta

from pyspark.sql.functions import col, min, max, count, lit

pub_date_table = "edw.t10_pub_date"


def get_date_period_and_days(
        spark,
        begin_date=None,
        end_date=None,
        begin_month=None,
        end_month=None,
        busi_year=None,
        busi_month=None,
        is_trade_day=True
):
    """
    获取给定日期范围内的开始日期、结束日期和总天数
    :param spark:
    :param begin_date:
    :param end_date:
    :param begin_month:
    :param end_month:
    :param busi_year:
    :param busi_month:
    :param is_trade_day:
    :return:
    """
    # 基于开始和结束日期进行过滤
    if begin_date and end_date:
        date_filter = (col("busi_date").between(begin_date, end_date))
    elif begin_date:
        date_filter = (col("busi_date") >= begin_date)
    elif end_date:
        date_filter = (col("busi_date") <= end_date)
    else:
        date_filter = lit(True)

    # 基于开始和结束月份进行过滤
    if begin_month and end_month:
        month_range_filter = (
                (col("busi_date").substr(1, 6) >= begin_month) &
                (col("busi_date").substr(1, 6) <= end_month)
        )
    elif begin_month:
        month_range_filter = (col("busi_date").substr(1, 6) >= begin_month)
    elif end_month:
        month_range_filter = (col("busi_date").substr(1, 6) <= end_month)
    else:
        month_range_filter = lit(True)

    # 基于单独的月份进行过滤
    if busi_month:
        month_filter = (col("busi_date").substr(1, 6) == busi_month)
    else:
        month_filter = lit(True)

    # 基于单独的年份进行过滤
    if busi_year:
        year_filter = (col("busi_date").substr(1, 4) == busi_year)
    else:
        year_filter = lit(True)

    # 基于交易日进行过滤
    trade_day_filter = ((col("trade_flag") == "1") if is_trade_day else lit(True))

    # Apply all filters
    df_date = spark.table(pub_date_table) \
        .filter(
        date_filter &
        month_range_filter &
        month_filter &
        trade_day_filter &
        year_filter &
        (col("market_no") == "1")
    ).agg(
        min("busi_date").alias("v_begin_date"),
        max("busi_date").alias("v_end_date"),
        count("*").alias("v_trade_days")
    )

    return (df_date.first()["v_begin_date"],
            df_date.first()["v_end_date"],
            df_date.first()["v_trade_days"])


def get_busi_week_int(busi_date):
    """
    获取给定日期所在年的第几周
    :param busi_date: 日期字符串, 格式为 'YYYYMMDD'
    :return: 第几周, 整数
    """
    # 将日期字符串转换为 datetime 对象
    date_object = datetime.strptime(busi_date, '%Y%m%d')
    # 获取给定日期所在年的第几周
    v_busi_week = date_object.isocalendar()[1]
    return v_busi_week


def get_mon_sun_str(busi_date):
    """
    获取给定日期所在周的星期一和星期日
    :param busi_date: 日期字符串, 格式为 'YYYYMMDD'
    :return: 星期一和星期日的日期字符串, 格式为 'YYYYMMDD'
    """
    # 将日期字符串转换为 datetime 对象
    date_object = datetime.strptime(busi_date, '%Y%m%d')
    # 找到给定日期所在周的星期一
    monday = date_object - timedelta(days=date_object.weekday())
    # 找到给定日期所在周的星期日
    sunday = monday + timedelta(days=6)
    # 将日期对象转换为字符串
    v_begin_date = monday.strftime('%Y%m%d')
    v_end_date = sunday.strftime('%Y%m%d')
    return v_begin_date, v_end_date


def get_quarter(busi_date):
    """
    获取给定日期所在季度
    :param busi_date: 日期字符串, 格式为 'YYYYMMDD'或者'YYYYMM'
    :return: 季度, 整数字符串
    """

    if len(busi_date) == 6:
        busi_date = busi_date + "01"
    # 将日期字符串转换为 datetime 对象
    date_object = datetime.strptime(busi_date, '%Y%m%d')
    # 获取给定日期所在季度
    v_quarter = (date_object.month - 1) // 3 + 1
    return str(v_quarter)


def get_previous_year_date(busi_date):
    """
    获取给定日期的前一年日期
    :param busi_date: 日期字符串, 格式为 'YYYYMMDD'
    :return: 前一年日期字符串, 格式为 'YYYYMMDD', 闰年2月29日返回2月28日
    """
    # 将输入日期字符串解析为 datetime 对象
    input_date = datetime.strptime(busi_date, '%Y%m%d')

    # 获取输入日期的年份和月份
    input_year = input_date.year
    input_month = input_date.month
    input_day = input_date.day

    # 计算前一年的日期
    previous_year = input_year - 1

    # 如果输入日期是闰年2月29日，而前一年不是闰年，返回2月28日
    if input_month == 2 and input_day == 29 and not is_leap_year(previous_year):
        return '{:04d}0228'.format(previous_year)

    # 否则返回前一年的相同日期
    return '{:04d}{:02d}{:02d}'.format(previous_year, input_month, input_day)


def is_leap_year(year):
    """
    判断给定年份是否是闰年
    :param year: 年份
    :return: True 如果是闰年，否则 False
    """
    return (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)


def is_trade_day_check(list_pub_date, busi_date):
    """
    判断给定日期是否是交易日
    :param list_pub_date: 包含交易日的列表
    :param busi_date: 日期字符串, 格式为 'YYYYMMDD'
    :return: bool, True 如果是交易日，否则 False
    """
    return busi_date in list_pub_date


def get_trade_date(list_pub_date, busi_date, n):
    """
    获取给定日期之后或之前的第 n 个交易日
    :param list_pub_date: 包含交易日的列表
    :param busi_date: 日期字符串, 格式为 'YYYYMMDD'
    :param n: 整数, 表示第 n 个交易日
    :return: 第 n 个交易日的日期字符串, 格式为 'YYYYMMDD'
    """
    # 将输入日期字符串解析为 datetime 对象
    input_date = datetime.strptime(busi_date, '%Y%m%d')

    # 初始化input_date为交易日(往前找)
    while not is_trade_day_check(list_pub_date, input_date.strftime('%Y%m%d')):
        input_date -= timedelta(days=1)

    # 计算第 n 个交易日的日期
    if n > 0:
        # 如果 n 大于 0, 则向后查找第 n 个交易日
        trade_date = input_date
        while n > 0:
            # 将日期加一天
            trade_date += timedelta(days=1)
            # 如果加一天后是交易日, 则 n 减一
            if is_trade_day_check(list_pub_date, trade_date.strftime('%Y%m%d')):
                n -= 1
    else:
        # 如果 n 小于 0, 则向前查找第 n 个交易日
        trade_date = input_date
        while n < 0:
            # 将日期减一天
            trade_date -= timedelta(days=1)
            # 如果减一天后是交易日, 则 n 加一
            if is_trade_day_check(list_pub_date, trade_date.strftime('%Y%m%d')):
                n += 1

    # 将结果格式化为字符串并返回
    return trade_date.strftime('%Y%m%d')
