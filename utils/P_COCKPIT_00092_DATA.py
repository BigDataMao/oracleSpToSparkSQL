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

    """
    本年度,上年度的千万工程客户
    """

    dict_data = {
        "this_year": {
            "suffix": "",
            "begin": v_begin_month,
            "end": v_end_month,
        },
        "last_year": {
            "suffix": "_LAST",
            "begin": v_last_begin_month,
            "end": v_last_end_month,
        }
    }

    # TODO CF_CRMMG.T_HIS_LABEL_CLIENT, cf_crmmg.t_label 需要采集
    for value in dict_data.values():
        df_92_qw = spark.table("ods.T_CRMMG_HIS_LABEL_CLIENT").alias("t") \
            .filter(
            col("t.months").between(value["begin"], value["end"])
        ).join(
            other=spark.table("ods.T_CRMMG_LABEL").alias("a"),
            on=(
                col("t.label_id") == col("a.label_id"),
                col("a.label_id") == lit("BQ4909")
            ),
            how="inner"
        ).join(
            other=spark.table("edw.h12_fund_account").alias("b"),
            on=(
                col("t.client_id") == col("b.fund_account_id")
            ),
            how="left"
        ).join(
            other=spark.table("ddw.t_ctp_branch_oa_rela").alias("c"),
            on=(
                col("b.branch_id") == col("c.branch_id")
            ),
            how="inner"
        ).groupBy(
            "b.fund_account_id",
            "c.oa_branch_id",
            "b.client_type",
            "b.open_date"
        ).select(
            "b.fund_account_id",
            "c.oa_branch_id",
            "b.client_type",
            "b.open_date"
        )

        return_to_hive(
            spark=spark,
            df_result=df_92_qw,
            target_table="ddw.T_BRP_00092_QW" + value["suffix"],
            insert_mode="overwrite",
        )

        logging.info("ddw.T_BRP_00092_QW%s[全量数据],写入完成", value["suffix"])

    """
    计算客户本年的净贡献,和上年的净贡献
    """

    dict_data = {
        "this_year": {
            "suffix": "jgx",
            "begin": v_begin_month,
            "end": v_end_month,
        },
        "last_year": {
            "suffix": "jgx_last",
            "begin": v_last_begin_month,
            "end": v_last_end_month,
        }
    }

    for value in dict_data.values():
        df_jgx = spark.table("ods.T_DS_ADM_INVESTOR_VALUE").alias("a") \
            .filter(
            col("a.date_dt").between(value["begin"], value["end"])  # 这里是循环唯一区别
        ).join(
            other=spark.table("ods.T_DS_ADM_BROKERDATA_DETAIL").alias("a2"),
            on=(
                col("a.date_dt") == col("a2.tx_dt") &
                col("a.investor_id") == col("a2.investor_id") &
                col("a2.rec_freq") == lit("M")
            ),
            how="left"
        ).groupBy(
            "a.investor_id"
        ).agg(
            (
                sum(col("a.subsistence_fee_amt")) +
                round(sum(col("a.int_amt")), 2) +
                round(sum(col("a.exchangeret_amt")), 2) +
                sum(col("a.oth_amt")) -
                round(sum(col("a.fd_amt")), 2) -
                round(sum(col("a.i_int_amt")), 2) -
                sum(col("a.i_exchangeret_amt")) -
                sum(col("a.broker_amt")) -
                sum(col("a.soft_amt")) -
                sum(col("a.i_oth_amt")) -
                round(sum(col("a.broker_int_amt")), 2) -
                round(sum(col("a.broker_eret_amt")), 2) -
                round(
                    sum(when(col("a2.ib_amt").isNull(), col("a.STAFF_AMT")).otherwise(col("a2.ib_amt"))), 2) -
                round(
                    sum(when(col("a2.STAFF_INT_AMT").isNull(), col("a.STAFF_INT_AMT")).otherwise(col("a2.STAFF_INT_AMT"))),
                    2) -
                round(sum(col("a.staff_eret_amt")), 2)
            ).alias("jgx")
        ).select(
            "a.investor_id",
            "jgx"
        )

        return_to_hive(
            spark=spark,
            df_result=df_jgx,
            target_table="ddw.TMP_COCKPIT_CLIENT_" + value["suffix"],  # 仅仅后缀不同
            insert_mode="overwrite",
        )

        logging.info("ddw.TMP_COCKPIT_CLIENT_%s[全量数据],写入完成", value["suffix"])

    """
    初始化CF_BUSIMG.T_BRP_00092
    """

    # TODO: CF_BUSIMG.T_BRP_00092 分区字段 Busi_Year

    df_92 = spark.table("ddw.T_OA_BRANCH").alias("b") \
        .filter(
        col("b.canceled").isNull()
    ).select(
        lit(v_busi_year).alias("busi_year"),
        col("b.departmentid").alias("oa_branch_id"),
        col("b.shortname").alias("oa_branch_name")
    )

    return_to_hive(
        spark=spark,
        df_result=df_92,
        target_table="ddw.T_BRP_00092",
        insert_mode="overwrite",
        partition_column="busi_year",
        partition_value=v_busi_year
    )

    logging.info("ddw.T_BRP_00092[初始化数据],写入完成")

    df_92 = spark.table("ddw.T_BRP_00092") \
        .filter(
        col("busi_year") == lit(v_busi_year)
    )

    logging.info("ddw.T_BRP_00092[本年分区]重新加载完成")

    """
    客户数
    """


