# -*- coding: utf-8 -*-

"""
专门处理日期参数的工具类
"""
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

    if is_trade_day:
        return (df_date.first()["v_begin_date"],
                df_date.first()["v_end_date"],
                df_date.first()["v_trade_days"])
    else:
        return (df_date.first()["v_begin_date"],
                df_date.first()["v_end_date"])
