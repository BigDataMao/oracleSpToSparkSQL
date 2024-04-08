# -*- coding: utf-8 -*-
"""
宏源-千万工程”指标落地数据
逻辑依据: utils/P_COCKPIT_00092_DATA.sql
"""
import logging

from pyspark.sql.functions import col, sum, expr, round, when, coalesce, lit

from utils.task_env import return_to_hive
from date_utils import get_date_period_and_days

logging.basicConfig(level=logging.INFO)


def p_cockpit_00092_data(spark, busi_date):

    v_busi_year = busi_date[:4]
    v_begin_date = v_busi_year + "0101"
    v_end_date = busi_date
    v_begin_month = v_busi_year + "01"
    v_end_month = busi_date[:6]
    # TODO 下面4个需要核实逻辑
    v_last_begin_month = str(int(v_busi_year) - 1) + busi_date[4:6]
    v_last_end_month = str(int(v_busi_year) - 1) + "12"
    v_last_begin_date = v_last_begin_month + "01"
    v_last_end_date = v_last_end_month + "31"

    (
        _,
        _,
        v_trade_days
    ) = get_date_period_and_days(
        spark=spark,
        begin_date=v_begin_date,
        end_date=v_end_date,
        is_trade_day=True
    )


