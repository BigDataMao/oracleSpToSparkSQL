# -*- coding: utf-8 -*-
"""
宏源-千万工程”指标落地数据
逻辑依据: utils/P_COCKPIT_00092_DATA.sql
"""

from pyspark.sql.functions import sum, round, count

from utils.date_utils import get_date_period_and_days
from utils.task_env import *

logger = logging.getLogger("logger")


@log
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

    for value in dict_data.values():
        df_92_tmp = spark.table("ods.T_CRMMG_HIS_LABEL_CLIENT").alias("t") \
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

        value["df_92_tmp"] = df_92_tmp

        return_to_hive(
            spark=spark,
            df_result=df_92_tmp,
            target_table="ddw.T_BRP_00092_QW" + value["suffix"],
            insert_mode="overwrite",
        )

        logging.info("ddw.T_BRP_00092_QW%s[全量数据],写入完成", value["suffix"])

    # 读取两个df
    df_qw = dict_data["this_year"]["df_92_tmp"]
    df_qw_last = dict_data["last_year"]["df_92_tmp"]

    """
    计算客户本年的净贡献,和上年的净贡献
    """

    dict_data = {
        "this_year": {
            "suffix": "jgx",
            "begin": v_begin_date,
            "end": v_end_date,
        },
        "last_year": {
            "suffix": "jgx_last",
            "begin": v_last_begin_date,
            "end": v_last_end_date,
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
                        sum(when(col("a2.STAFF_INT_AMT").isNull(), col("a.STAFF_INT_AMT")).otherwise(
                            col("a2.STAFF_INT_AMT"))),
                        2) -
                    round(sum(col("a.staff_eret_amt")), 2)
            ).alias("jgx")
        ).select(
            "a.investor_id",
            "jgx"
        )

        value["df_jgx"] = df_jgx

        return_to_hive(
            spark=spark,
            df_result=df_jgx,
            target_table="ddw.TMP_COCKPIT_CLIENT_" + value["suffix"],  # 仅仅后缀不同
            insert_mode="overwrite",
        )

        logging.info("ddw.TMP_COCKPIT_CLIENT_%s[全量数据],写入完成", value["suffix"])

    # 读取两个df
    df_jgx = dict_data["this_year"]["df_jgx"]
    df_jgx_last = dict_data["last_year"]["df_jgx"]

    """
    初始化CF_BUSIMG.T_BRP_00092
    """

    df_92_y = spark.table("ddw.T_OA_BRANCH").alias("b") \
        .filter(
        col("b.canceled").isNull()
    ).select(
        lit(v_busi_year).alias("busi_year"),
        col("b.departmentid").alias("oa_branch_id"),
        col("b.shortname").alias("oa_branch_name")
    )

    return_to_hive(
        spark=spark,
        df_result=df_92_y,
        target_table="ddw.T_BRP_00092",
        insert_mode="overwrite",
        partition_column="busi_year",
        partition_value=v_busi_year
    )

    logging.info("ddw.T_BRP_00092[初始化数据],写入完成")

    df_92_y = spark.table("ddw.T_BRP_00092") \
        .filter(
        col("busi_year") == lit(v_busi_year)
    )

    logging.info("ddw.T_BRP_00092[本年分区]重新加载完成")

    """
    日均权益
    """

    tmp1_grouped = spark.table("edw.h15_client_sett").alias("t") \
        .filter(
        col("t.busi_date").between(v_begin_date, v_end_date)
    ).join(
        other=df_qw.alias("a"),
        on=(
                col("t.fund_account_id") == col("a.fund_account_id")
        ),
        how="inner"
    ).groupBy(
        col("t.fund_account_id"),
        col("a.oa_branch_id"),
        col("a.client_type")
    )

    tmp1 = tmp1_grouped.agg(
        sum(col("rights")).alias("rights")
    ).select(
        "fund_account_id",
        "oa_branch_id",
        "client_type",
        "rights"
    )

    tmp2 = tmp1.alias("t") \
        .groupBy(
        col("t.oa_branch_id")
    ).agg(
        (
                sum(
                    when(col("t.client_type").isin("3", "4"), col("t.rights")).otherwise(0)
                ) / v_trade_days
        ).alias("avg_rights_3"),
        (
                sum(
                    when(col("t.client_type") == lit(1), col("t.rights")).otherwise(0)
                ) / v_trade_days
        ).alias("avg_rights_1"),
        (
                sum(
                    when(col("t.client_type") == lit(0), col("t.rights")).otherwise(0)
                ) / v_trade_days
        ).alias("avg_rights_0"),
        (
                sum(
                    col("t.rights")
                ) / v_trade_days
        ).alias("avg_rights_all")
    ).select(
        "t.oa_branch_id",
        "avg_rights_3",
        "avg_rights_1",
        "avg_rights_0",
        "avg_rights_all"
    )

    df_y = tmp2.alias("t") \
        .select(
        lit(v_busi_year).alias("busi_year"),
        col("t.oa_branch_id"),
        col("t.avg_rights_3"),
        when(
            col("t.avg_rights_all") != lit(0),
            col("t.avg_rights_3") / col("t.avg_rights_all")
        ).otherwise(lit(0)).alias("avg_rights_3_rate"),
        col("t.avg_rights_1"),
        when(
            col("t.avg_rights_all") != lit(0),
            col("t.avg_rights_1") / col("t.avg_rights_all")
        ).otherwise(lit(0)).alias("avg_rights_1_rate"),
        col("t.avg_rights_0"),
        when(
            col("t.avg_rights_all") != lit(0),
            col("t.avg_rights_0") / col("t.avg_rights_all")
        ).otherwise(lit(0)).alias("avg_rights_0_rate"),
    )

    df_92_y = update_dataframe(
        df_to_update=df_92_y,
        df_use_me=df_y,
        join_columns=["busi_year", "oa_branch_id"],
        update_columns=[
            "avg_rights_3",
            "avg_rights_3_rate",
            "avg_rights_1",
            "avg_rights_1_rate",
            "avg_rights_0",
            "avg_rights_0_rate",
        ]
    )

    """
    手续费
    """

    tmp1 = tmp1_grouped.agg(
        sum(
            col("transfee") + col("delivery_transfee") + col("strikefee")
        ).alias("transfee")
    ).select(
        "fund_account_id",
        "oa_branch_id",
        "client_type",
        "transfee"
    )

    tmp2 = tmp1.alias("t") \
        .groupBy(
        col("t.oa_branch_id")
    ).agg(
        (
                sum(
                    when(col("t.client_type").isin("3", "4"), col("t.transfee")).otherwise(0)
                ) / v_trade_days
        ).alias("transfee_3"),
        (
                sum(
                    when(col("t.client_type") == lit(1), col("t.transfee")).otherwise(0)
                ) / v_trade_days
        ).alias("transfee_1"),
        (
                sum(
                    when(col("t.client_type") == lit(0), col("t.transfee")).otherwise(0)
                ) / v_trade_days
        ).alias("transfee_0"),
        (
            sum(
                col("t.rights")
            )
        ).alias("transfee_all")
    ).select(
        "t.oa_branch_id",
        "transfee_3",
        "transfee_1",
        "transfee_0",
        "transfee_all"
    )

    df_y = tmp2.alias("t") \
        .select(
        lit(v_busi_year).alias("busi_year"),
        col("t.oa_branch_id"),
        col("t.transfee_3"),
        when(
            col("t.transfee_all") != lit(0),
            col("t.transfee_3") / col("t.transfee_all")
        ).otherwise(lit(0)).alias("transfee_3_rate"),
        col("t.transfee_1"),
        when(
            col("t.transfee_all") != lit(0),
            col("t.transfee_1") / col("t.transfee_all")
        ).otherwise(lit(0)).alias("transfee_1_rate"),
        col("t.transfee_0"),
        when(
            col("t.transfee_all") != lit(0),
            col("t.transfee_0") / col("t.transfee_all")
        ).otherwise(lit(0)).alias("transfee_0_rate"),
    )

    df_92_y = update_dataframe(
        df_to_update=df_92_y,
        df_use_me=df_y,
        join_columns=["busi_year", "oa_branch_id"],
        update_columns=[
            "transfee_3",
            "transfee_3_rate",
            "transfee_1",
            "transfee_1_rate",
            "transfee_0",
            "transfee_0_rate",
        ]
    )

    """
    客户数
    """

    tmp1 = spark.table("edw.h12_fund_account").alias("t") \
        .join(
        other=df_qw.alias("a"),
        on=(
                col("t.fund_account_id") == col("a.fund_account_id")
        ),
        how="inner"
    ).groupBy(
        col("t.fund_account_id"),
        col("a.oa_branch_id"),
        col("a.client_type")
    ).select(
        "t.fund_account_id",
        "a.oa_branch_id",
        "a.client_type"
    )

    tmp2 = tmp1.alias("t") \
        .groupBy(
        col("t.oa_branch_id")
    ).agg(
        sum(
            when(col("t.client_type").isin("3", "4"), lit(1)).otherwise(0)
        ).alias("client_num_3"),
        sum(
            when(col("t.client_type") == lit(1), lit(1)).otherwise(0)
        ).alias("client_num_1"),
        sum(
            when(col("t.client_type") == lit(0), lit(1)).otherwise(0)
        ).alias("client_num_0"),
    ).select(
        "t.oa_branch_id",
        "client_num_3",
        "client_num_1",
        "client_num_0",
    )

    df_y = tmp2.alias("t") \
        .select(
        lit(v_busi_year).alias("busi_year"),
        col("t.oa_branch_id"),
        col("t.client_num_3"),
        col("t.client_num_1"),
        col("t.client_num_0"),
    )

    df_92_y = update_dataframe(
        df_to_update=df_92_y,
        df_use_me=df_y,
        join_columns=["busi_year", "oa_branch_id"],
        update_columns=[
            "client_num_3",
            "client_num_1",
            "client_num_0",
        ]
    )

    """
    净贡献总和（万）
    """

    tmp1 = df_jgx.alias("t") \
        .join(
        other=df_qw.alias("a"),
        on=(
                col("t.client_id") == col("a.fund_account_id")
        ),
        how="inner"
    ).select(
        col("t.client_id").alias("fund_account_id"),
        "a.oa_branch_id",
        "a.client_type",
        "t.jgx"
    )

    tmp2 = tmp1.alias("t") \
        .groupBy(
        col("t.oa_branch_id")
    ).agg(
        sum(
            when(col("t.client_type").isin("3", "4"), col("t.jgx")).otherwise(0)
        ).alias("CLEAR_TRANSFEE_3"),
        sum(
            when(col("t.client_type") == lit(1), col("t.jgx")).otherwise(0)
        ).alias("CLEAR_TRANSFEE_1"),
        sum(
            when(col("t.client_type") == lit(0), col("t.jgx")).otherwise(0)
        ).alias("CLEAR_TRANSFEE_0"),
    ).select(
        "t.oa_branch_id",
        "CLEAR_TRANSFEE_3",
        "CLEAR_TRANSFEE_1",
        "CLEAR_TRANSFEE_0",
    )

    df_y = tmp2.alias("t") \
        .select(
        lit(v_busi_year).alias("busi_year"),
        col("t.oa_branch_id"),
        (col("t.CLEAR_TRANSFEE_3") / lit(10000)).alias("CLEAR_TRANSFEE_3"),
        (col("t.CLEAR_TRANSFEE_1") / lit(10000)).alias("CLEAR_TRANSFEE_1"),
        (col("t.CLEAR_TRANSFEE_0") / lit(10000)).alias("CLEAR_TRANSFEE_0"),
    )

    df_92_y = update_dataframe(
        df_to_update=df_92_y,
        df_use_me=df_y,
        join_columns=["busi_year", "oa_branch_id"],
        update_columns=[
            "CLEAR_TRANSFEE_3",
            "CLEAR_TRANSFEE_1",
            "CLEAR_TRANSFEE_0",
        ]
    )

    """
    单客户平均贡献值
    """

    df_92_y = df_92_y.alias("a") \
        .withColumn(
        "avg_clear_transfee_3",
        when(
            col("a.client_num_3") != lit(0),
            col("a.CLEAR_TRANSFEE_3") * lit(10000) / col("a.client_num_3")
        ).otherwise(lit(0))
    ).withColumn(
        "avg_clear_transfee_1",
        when(
            col("a.client_num_1") != lit(0),
            col("a.CLEAR_TRANSFEE_1") * lit(10000) / col("a.client_num_1")
        ).otherwise(lit(0))
    ).withColumn(
        "avg_clear_transfee_0",
        when(
            col("a.client_num_0") != lit(0),
            col("a.CLEAR_TRANSFEE_0") * lit(10000) / col("a.client_num_0")
        ).otherwise(lit(0))
    )

    """
    两年千万工程客户数
    """

    # 本年度符合标准千万工程客户数
    tmp = df_qw.alias("t") \
        .groupBy(
        col("t.oa_branch_id")
    ).agg(
        count("1").alias("QW_CLIENT_NUM_ALL")
    ).select(
        "t.oa_branch_id",
        "QW_CLIENT_NUM_ALL"
    )

    # 连续两年符合标准存量客户数
    tmp1 = df_qw.alias("t") \
        .join(
        other=df_qw_last.alias("a"),
        on=(
                col("t.fund_account_id") == col("a.fund_account_id")
        ),
        how="inner"
    ).groupBy(
        col("t.oa_branch_id")
    ).agg(
        count("1").alias("QW_CLIENT_NUM_ALL_LAST")
    ).select(
        "t.oa_branch_id",
        "QW_CLIENT_NUM_1"
    )

    # 连续两年符合标准新开户客户数
    tmp2 = df_qw.alias("t") \
        .filter(
        col("t.open_date").between(v_begin_date, v_end_date)
    ).join(
        other=df_qw_last.alias("a"),
        on=(
                col("t.fund_account_id") == col("a.fund_account_id")
        ),
        how="inner"
    ).groupBy(
        col("t.oa_branch_id")
    ).agg(
        count("1").alias("QW_CLIENT_NUM_2")
    ).select(
        "t.oa_branch_id",
        "QW_CLIENT_NUM_2"
    )

    # 新增符合标准存量客户数
    tmp3 = df_qw.alias("t") \
        .filter(
        col("t.open_date") < v_begin_date
    ).join(
        other=df_qw_last.alias("a"),
        on=(
                col("t.fund_account_id") == col("a.fund_account_id")
        ),
        how="left_anti"
    ).groupBy(
        col("t.oa_branch_id")
    ).agg(
        count("1").alias("QW_CLIENT_NUM_3")
    ).select(
        "t.oa_branch_id",
        "QW_CLIENT_NUM_3"
    )

    # 两年千万工程客户数-新增符合标准新开户客户数
    tmp4 = df_qw.alias("t") \
        .filter(
        col("t.open_date") >= v_begin_date
    ).join(
        other=df_qw_last.alias("a"),
        on=(
                col("t.fund_account_id") == col("a.fund_account_id")
        ),
        how="left_anti"
    ).groupBy(
        col("t.oa_branch_id")
    ).agg(
        count("1").alias("QW_CLIENT_NUM_4")
    ).select(
        "t.oa_branch_id",
        "QW_CLIENT_NUM_4"
    )

    df_y = df_92_y.alias("t") \
        .join(
        other=tmp1.alias("a"),
        on=(
                col("t.oa_branch_id") == col("a.oa_branch_id")
        ),
        how="left"
    ).join(
        other=tmp2.alias("b"),
        on=(
                col("t.oa_branch_id") == col("b.oa_branch_id")
        ),
        how="left"
    ).join(
        other=tmp3.alias("c"),
        on=(
                col("t.oa_branch_id") == col("c.oa_branch_id")
        ),
        how="left"
    ).join(
        other=tmp4.alias("d"),
        on=(
                col("t.oa_branch_id") == col("d.oa_branch_id")
        ),
        how="left"
    ).join(
        other=tmp.alias("e"),
        on=(
                col("t.oa_branch_id") == col("e.oa_branch_id")
        ),
        how="left"
    ).select(
        col("t.oa_branch_id"),
        coalesce(col("a.QW_CLIENT_NUM_1"), lit(0)).alias("QW_CLIENT_NUM_1"),
        when(
            coalesce(col("e.QW_CLIENT_NUM_ALL"), lit(0)) != lit(0),
            coalesce(col("a.QW_CLIENT_NUM_1"), lit(0)) / col("e.QW_CLIENT_NUM_ALL")
        ).otherwise(lit(0)).alias("QW_CLIENT_NUM_1_RATE"),
        coalesce(col("b.QW_CLIENT_NUM_2"), lit(0)).alias("QW_CLIENT_NUM_2"),
        when(
            coalesce(col("e.QW_CLIENT_NUM_ALL"), lit(0)) != lit(0),
            coalesce(col("b.QW_CLIENT_NUM_2"), lit(0)) / col("e.QW_CLIENT_NUM_ALL")
        ).otherwise(lit(0)).alias("QW_CLIENT_NUM_2_RATE"),
        coalesce(col("c.QW_CLIENT_NUM_3"), lit(0)).alias("QW_CLIENT_NUM_3"),
        when(
            coalesce(col("e.QW_CLIENT_NUM_ALL"), lit(0)) != lit(0),
            coalesce(col("c.QW_CLIENT_NUM_3"), lit(0)) / col("e.QW_CLIENT_NUM_ALL")
        ).otherwise(lit(0)).alias("QW_CLIENT_NUM_3_RATE"),
        coalesce(col("d.QW_CLIENT_NUM_4"), lit(0)).alias("QW_CLIENT_NUM_4"),
        when(
            coalesce(col("e.QW_CLIENT_NUM_ALL"), lit(0)) != lit(0),
            coalesce(col("d.QW_CLIENT_NUM_4"), lit(0)) / col("e.QW_CLIENT_NUM_ALL")
        ).otherwise(lit(0)).alias("QW_CLIENT_NUM_4_RATE"),
        coalesce(col("e.QW_CLIENT_NUM_ALL"), lit(0)).alias("QW_CLIENT_NUM_ALL"),
    )

    df_92_y = update_dataframe(
        df_to_update=df_92_y,
        df_use_me=df_y,
        join_columns=["busi_year", "oa_branch_id"],
        update_columns=[
            "QW_CLIENT_NUM_1",
            "QW_CLIENT_NUM_1_RATE",
            "QW_CLIENT_NUM_2",
            "QW_CLIENT_NUM_2_RATE",
            "QW_CLIENT_NUM_3",
            "QW_CLIENT_NUM_3_RATE",
            "QW_CLIENT_NUM_4",
            "QW_CLIENT_NUM_4_RATE",
            "QW_CLIENT_NUM_ALL",
        ]
    )

    """
    两年千万工程客户净贡献
    """

    # 本年度符合标准千万工程
    tmp = df_qw.alias("t") \
        .join(
        other=df_jgx.alias("a"),
        on=(
                col("t.fund_account_id") == col("a.client_id")
        ),
        how="inner"
    ).groupBy(
        col("t.oa_branch_id")
    ).agg(
        sum(
            col("a.jgx")
        ).alias("QW_CLEAR_TRANSFEE_ALL")
    ).select(
        "t.oa_branch_id",
        "QW_CLEAR_TRANSFEE_ALL"
    )

    # 连续两年符合标准存量客户净贡献
    tmp1 = df_qw.alias("t") \
        .join(
        other=df_qw_last.alias("a"),
        on=(
                col("t.fund_account_id") == col("a.fund_account_id")
        ),
        how="inner"
    ).join(
        other=df_jgx.alias("b"),
        on=(
                col("t.fund_account_id") == col("b.client_id")
        ),
        how="inner"
    ).groupBy(
        col("t.oa_branch_id")
    ).agg(
        sum(
            col("b.jgx")
        ).alias("QW_CLEAR_TRANSFEE_1")
    ).select(
        "t.oa_branch_id",
        "QW_CLEAR_TRANSFEE_1"
    )

    # 连续两年符合标准新开户客户数
    tmp2 = df_qw.alias("t") \
        .filter(
        col("t.open_date").between(v_begin_date, v_end_date)
    ).join(
        other=df_qw_last.alias("a"),
        on=(
                col("t.fund_account_id") == col("a.fund_account_id")
        ),
        how="inner"
    ).join(
        other=df_jgx.alias("b"),
        on=(
                col("t.fund_account_id") == col("b.client_id")
        ),
        how="inner"
    ).groupBy(
        col("t.oa_branch_id")
    ).agg(
        sum(
            col("b.jgx")
        ).alias("QW_CLEAR_TRANSFEE_2")
    ).select(
        "t.oa_branch_id",
        "QW_CLEAR_TRANSFEE_2"
    )

    # 新增符合标准存量客户数
    tmp3 = df_qw.alias("t") \
        .filter(
        col("t.open_date") < v_begin_date
    ).join(
        other=df_qw_last.alias("a"),
        on=(
                col("t.fund_account_id") == col("a.fund_account_id")
        ),
        how="left_anti"
    ).join(
        other=df_jgx.alias("b"),
        on=(
                col("t.fund_account_id") == col("b.client_id")
        ),
        how="inner"
    ).groupBy(
        col("t.oa_branch_id")
    ).agg(
        sum(
            col("b.jgx")
        ).alias("QW_CLEAR_TRANSFEE_3")
    ).select(
        "t.oa_branch_id",
        "QW_CLEAR_TRANSFEE_3"
    )

    # 两年千万工程客户数-新增符合标准新开户客户数
    tmp4 = df_qw.alias("t") \
        .filter(
        col("t.open_date") >= v_begin_date
    ).join(
        other=df_qw_last.alias("a"),
        on=(
                col("t.fund_account_id") == col("a.fund_account_id")
        ),
        how="left_anti"
    ).join(
        other=df_jgx.alias("b"),
        on=(
                col("t.fund_account_id") == col("b.client_id")
        ),
        how="inner"
    ).groupBy(
        col("t.oa_branch_id")
    ).agg(
        sum(
            col("b.jgx")
        ).alias("QW_CLEAR_TRANSFEE_4")
    ).select(
        "t.oa_branch_id",
        "QW_CLEAR_TRANSFEE_4"
    )

    df_y = df_92_y.alias("t") \
        .join(
        other=tmp1.alias("a"),
        on=(
                col("t.oa_branch_id") == col("a.oa_branch_id")
        ),
        how="left"
    ).join(
        other=tmp2.alias("b"),
        on=(
                col("t.oa_branch_id") == col("b.oa_branch_id")
        ),
        how="left"
    ).join(
        other=tmp3.alias("c"),
        on=(
                col("t.oa_branch_id") == col("c.oa_branch_id")
        ),
        how="left"
    ).join(
        other=tmp4.alias("d"),
        on=(
                col("t.oa_branch_id") == col("d.oa_branch_id")
        ),
        how="left"
    ).join(
        other=tmp.alias("e"),
        on=(
                col("t.oa_branch_id") == col("e.oa_branch_id")
        ),
        how="left"
    ).select(
        col("t.oa_branch_id"),
        coalesce(col("a.QW_CLEAR_TRANSFEE_1"), lit(0)).alias("QW_CLEAR_TRANSFEE_1"),
        when(
            coalesce(col("e.QW_CLEAR_TRANSFEE_ALL"), lit(0)) != lit(0),
            coalesce(col("a.QW_CLEAR_TRANSFEE_1"), lit(0)) / col("e.QW_CLEAR_TRANSFEE_ALL")
        ).otherwise(lit(0)).alias("QW_CLEAR_TRANSFEE_1_RATE"),
        coalesce(col("b.QW_CLEAR_TRANSFEE_2"), lit(0)).alias("QW_CLEAR_TRANSFEE_2"),
        when(
            coalesce(col("e.QW_CLEAR_TRANSFEE_ALL"), lit(0)) != lit(0),
            coalesce(col("b.QW_CLEAR_TRANSFEE_2"), lit(0)) / col("e.QW_CLEAR_TRANSFEE_ALL")
        ).otherwise(lit(0)).alias("QW_CLEAR_TRANSFEE_2_RATE"),
        coalesce(col("c.QW_CLEAR_TRANSFEE_3"), lit(0)).alias("QW_CLEAR_TRANSFEE_3"),
        when(
            coalesce(col("e.QW_CLEAR_TRANSFEE_ALL"), lit(0)) != lit(0),
            coalesce(col("c.QW_CLEAR_TRANSFEE_3"), lit(0)) / col("e.QW_CLEAR_TRANSFEE_ALL")
        ).otherwise(lit(0)).alias("QW_CLEAR_TRANSFEE_3_RATE"),
        coalesce(col("d.QW_CLEAR_TRANSFEE_4"), lit(0)).alias("QW_CLEAR_TRANSFEE_4"),
        when(
            coalesce(col("e.QW_CLEAR_TRANSFEE_ALL"), lit(0)) != lit(0),
            coalesce(col("d.QW_CLEAR_TRANSFEE_4"), lit(0)) / col("e.QW_CLEAR_TRANSFEE_ALL")
        ).otherwise(lit(0)).alias("QW_CLEAR_TRANSFEE_4_RATE"),
        coalesce(col("e.QW_CLEAR_TRANSFEE_ALL"), lit(0)).alias("QW_CLEAR_TRANSFEE_ALL"),
    )

    df_92_y = update_dataframe(
        df_to_update=df_92_y,
        df_use_me=df_y,
        join_columns=["busi_year", "oa_branch_id"],
        update_columns=[
            "QW_CLEAR_TRANSFEE_1",
            "QW_CLEAR_TRANSFEE_1_RATE",
            "QW_CLEAR_TRANSFEE_2",
            "QW_CLEAR_TRANSFEE_2_RATE",
            "QW_CLEAR_TRANSFEE_3",
            "QW_CLEAR_TRANSFEE_3_RATE",
            "QW_CLEAR_TRANSFEE_4",
            "QW_CLEAR_TRANSFEE_4_RATE",
            "QW_CLEAR_TRANSFEE_ALL",
        ]
    )

    """
    户均贡献值（万）
    """
    # TODO: 以下代码需要核实逻辑
    df_92_y = df_92_y.alias("a") \
        .withColumn(
        "QW_AVG_CLEAR_TRANSFEE1",
        when(
            col("a.QW_CLIENT_NUM_ALL") != lit(0),
            col("a.QW_CLEAR_TRANSFEE_ALL") / col("a.QW_CLIENT_NUM_ALL")
        ).otherwise(lit(0)) / lit(10000)
    ).withColumn(
        "QW_AVG_CLEAR_TRANSFEE2",
        when(
            col("a.QW_CLIENT_NUM_2") != lit(0),
            col("a.QW_CLEAR_TRANSFEE_2") / col("a.QW_CLIENT_NUM_2")
        ).otherwise(lit(0)) / lit(10000)
    ).withColumn(
        "QW_AVG_CLEAR_TRANSFEE3",
        when(
            col("a.QW_CLIENT_NUM_3") != lit(0),
            col("a.QW_CLEAR_TRANSFEE_3") / col("a.QW_CLIENT_NUM_3")
        ).otherwise(lit(0)) / lit(10000)
    ).withColumn(
        "QW_AVG_CLEAR_TRANSFEE4",
        when(
            col("a.QW_CLIENT_NUM_4") != lit(0),
            col("a.QW_CLEAR_TRANSFEE_4") / col("a.QW_CLIENT_NUM_4")
        ).otherwise(lit(0)) / lit(10000)
    )

    """
    流失客户数
    """

    # 上年度符合标准的客户数
    tmp = df_qw_last.alias("t") \
        .groupBy(
        col("t.oa_branch_id")
    ).agg(
        count("1").alias("LOSE_CLIENT_NUM1")
    ).select(
        "t.oa_branch_id",
        "LOSE_CLIENT_NUM1"
    )

    # 连续两年符合标准的客户总数
    tmp1 = df_qw.alias("t") \
        .join(
        other=df_qw_last.alias("a"),
        on=(
                col("t.fund_account_id") == col("a.fund_account_id")
        ),
        how="inner"
    ).groupBy(
        col("t.oa_branch_id")
    ).agg(
        count("1").alias("LOSE_CLIENT_NUM2")
    ).select(
        "t.oa_branch_id",
        "LOSE_CLIENT_NUM2"
    )

    # 上年度符合，本年度不符合总数
    tmp2 = df_qw_last.alias("t") \
        .join(
        other=df_qw.alias("a"),
        on=(
                col("t.fund_account_id") == col("a.fund_account_id")
        ),
        how="left_anti"
    ).groupBy(
        col("t.oa_branch_id")
    ).agg(
        count("1").alias("LOSE_CLIENT_NUM4")
    ).select(
        "t.oa_branch_id",
        "LOSE_CLIENT_NUM4"
    )

    df_y = df_92_y.alias("t") \
        .join(
        other=tmp.alias("a"),
        on=(
                col("t.oa_branch_id") == col("a.oa_branch_id")
        ),
        how="left"
    ).join(
        other=tmp1.alias("b"),
        on=(
                col("t.oa_branch_id") == col("b.oa_branch_id")
        ),
        how="left"
    ).join(
        other=tmp2.alias("c"),
        on=(
                col("t.oa_branch_id") == col("c.oa_branch_id")
        ),
        how="left"
    ).select(
        col("t.oa_branch_id"),
        coalesce(col("a.LOSE_CLIENT_NUM1"), lit(0)).alias("LOSE_CLIENT_NUM1"),
        coalesce(col("b.LOSE_CLIENT_NUM2"), lit(0)).alias("LOSE_CLIENT_NUM2"),
        when(
            coalesce(col("a.LOSE_CLIENT_NUM1"), lit(0)) != lit(0),
            coalesce(col("b.LOSE_CLIENT_NUM2"), lit(0)) / col("a.LOSE_CLIENT_NUM1")
        ).otherwise(lit(0)).alias("LOSE_CLIENT_NUM3"),
        coalesce(col("c.LOSE_CLIENT_NUM4"), lit(0)).alias("LOSE_CLIENT_NUM4"),
        when(
            coalesce(col("a.LOSE_CLIENT_NUM1"), lit(0)) != lit(0),
            coalesce(col("c.LOSE_CLIENT_NUM4"), lit(0)) / col("a.LOSE_CLIENT_NUM1")
        ).otherwise(lit(0)).alias("LOSE_CLIENT_NUM5"),
    )

    df_92_y = update_dataframe(
        df_to_update=df_92_y,
        df_use_me=df_y,
        join_columns=["busi_year", "oa_branch_id"],
        update_columns=[
            "LOSE_CLIENT_NUM1",
            "LOSE_CLIENT_NUM2",
            "LOSE_CLIENT_NUM3",
            "LOSE_CLIENT_NUM4",
            "LOSE_CLIENT_NUM5",
        ]
    )

    """
    流失客户数-净贡献
    """

    # 流失客户数-上年度符合客户净贡献
    tmp = df_qw_last.alias("t") \
        .join(
        other=df_jgx_last.alias("a"),
        on=(
                col("t.fund_account_id") == col("a.client_id")
        ),
        how="inner"
    ).groupBy(
        col("t.oa_branch_id")
    ).agg(
        sum(
            col("a.jgx")
        ).alias("LOSE_CLIENT_NUM6")
    ).select(
        "t.oa_branch_id",
        "LOSE_CLIENT_NUM6"
    )

    # 连续两年符合标准的客户净贡献
    tmp1 = df_qw.alias("t") \
        .join(
        other=df_qw_last.alias("a"),
        on=(
                col("t.fund_account_id") == col("a.fund_account_id")
        ),
        how="inner"
    ).join(
        other=df_jgx.alias("b"),
        on=(
                col("t.fund_account_id") == col("b.client_id")
        ),
        how="inner"
    ).groupBy(
        col("t.oa_branch_id")
    ).agg(
        sum(
            col("b.jgx")
        ).alias("LOSE_CLIENT_NUM7")
    ).select(
        "t.oa_branch_id",
        "LOSE_CLIENT_NUM7"
    )

    # 上年度符合，本年度不符合总数
    tmp2 = df_qw_last.alias("t") \
        .join(
        other=df_qw.alias("a"),
        on=(
                col("t.fund_account_id") == col("a.fund_account_id")
        ),
        how="left_anti"
    ).groupBy(
        col("t.oa_branch_id")
    ).agg(
        sum(
            col("a.jgx")
        ).alias("LOSE_CLIENT_NUM9")
    ).select(
        "t.oa_branch_id",
        "LOSE_CLIENT_NUM9"
    )

    df_y = df_92_y.alias("t") \
        .join(
        other=tmp.alias("a"),
        on=(
                col("t.oa_branch_id") == col("a.oa_branch_id")
        ),
        how="left"
    ).join(
        other=tmp1.alias("b"),
        on=(
                col("t.oa_branch_id") == col("b.oa_branch_id")
        ),
        how="left"
    ).join(
        other=tmp2.alias("c"),
        on=(
                col("t.oa_branch_id") == col("c.oa_branch_id")
        ),
        how="left"
    ).select(
        col("t.oa_branch_id"),
        coalesce(col("a.LOSE_CLIENT_NUM6"), lit(0)).alias("LOSE_CLIENT_NUM6"),
        coalesce(col("b.LOSE_CLIENT_NUM7"), lit(0)).alias("LOSE_CLIENT_NUM7"),
        when(
            coalesce(col("a.LOSE_CLIENT_NUM6"), lit(0)) != lit(0),
            col("b.LOSE_CLIENT_NUM7") / col("a.LOSE_CLIENT_NUM6")
        ).otherwise(lit(0)).alias("LOSE_CLIENT_NUM8"),
        coalesce(col("c.LOSE_CLIENT_NUM9"), lit(0)).alias("LOSE_CLIENT_NUM9"),
        when(
            coalesce(col("a.LOSE_CLIENT_NUM6"), lit(0)) != lit(0),
            col("c.LOSE_CLIENT_NUM9") / col("a.LOSE_CLIENT_NUM6")
        ).otherwise(lit(0)).alias("LOSE_CLIENT_NUM10"),
    )

    df_92_y = update_dataframe(
        df_to_update=df_92_y,
        df_use_me=df_y,
        join_columns=["busi_year", "oa_branch_id"],
        update_columns=[
            "LOSE_CLIENT_NUM6",
            "LOSE_CLIENT_NUM7",
            "LOSE_CLIENT_NUM8",
            "LOSE_CLIENT_NUM9",
            "LOSE_CLIENT_NUM10",
        ]
    )

    """
    流失客户数-本年全客户平均净贡献增长率
    "200个客户在2022年的净贡献，除以，200；假设为A
    180个客户在2021年的净贡献，除以，180；假设为B
    结果为：A/B—1
    """

    # 本年客户数
    tmp = df_qw.alias("t") \
        .groupBy(
        col("t.oa_branch_id")
    ).agg(
        count("1").alias("client_num")
    ).select(
        "t.oa_branch_id",
        "client_num"
    )

    # 本年净贡献
    tmp1 = df_qw.alias("t") \
        .join(
        other=df_jgx.alias("a"),
        on=(
                col("t.fund_account_id") == col("a.client_id")
        ),
        how="inner"
    ).groupBy(
        col("t.oa_branch_id")
    ).agg(
        sum(
            col("a.jgx")
        ).alias("jgx_sum")
    ).select(
        "t.oa_branch_id",
        "jgx_sum"
    )

    # 本年全客户平均净贡献
    tmp2 = tmp.alias("t") \
        .join(
        other=tmp1.alias("a"),
        on=(
                col("t.oa_branch_id") == col("a.oa_branch_id")
        ),
        how="left"
    ).select(
        "t.oa_branch_id",
        when(
            col("t.client_num") != lit(0),
            coalesce(col("a.jgx_sum"), lit(0)) / col("t.client_num")
        ).otherwise(lit(0)).alias("jgx_rate")
    )

    # 上一年客户数
    tmp3 = df_qw_last.alias("t") \
        .groupBy(
        col("t.oa_branch_id")
    ).agg(
        count("1").alias("client_num")
    ).select(
        "t.oa_branch_id",
        "client_num"
    )

    # 上一年净贡献
    tmp4 = df_qw_last.alias("t") \
        .join(
        other=df_jgx_last.alias("a"),
        on=(
                col("t.fund_account_id") == col("a.client_id")
        ),
        how="inner"
    ).groupBy(
        col("t.oa_branch_id")
    ).agg(
        sum(
            col("a.jgx")
        ).alias("jgx_sum")
    ).select(
        "t.oa_branch_id",
        "jgx_sum"
    )

    # 上一年全客户平均净贡献
    tmp5 = tmp3.alias("t") \
        .join(
        other=tmp4.alias("a"),
        on=(
                col("t.oa_branch_id") == col("a.oa_branch_id")
        ),
        how="left"
    ).select(
        "t.oa_branch_id",
        when(
            col("t.client_num") != lit(0),
            coalesce(col("a.jgx_sum"), lit(0)) / col("t.client_num")
        ).otherwise(lit(0)).alias("jgx_rate")
    )

    df_y = df_92_y.alias("t") \
        .join(
        other=tmp2.alias("a"),
        on=(
                col("t.oa_branch_id") == col("a.oa_branch_id")
        ),
        how="left"
    ).join(
        other=tmp5.alias("b"),
        on=(
                col("t.oa_branch_id") == col("b.oa_branch_id")
        ),
        how="left"
    ).select(
        col("t.oa_branch_id"),
        when(
            coalesce(col("b.jgx_rate"), lit(0)) != lit(0),
            coalesce(col("a.jgx_rate"), lit(0)) / coalesce(col("b.jgx_rate"), lit(0)) - 1
        ).otherwise(lit(0)).alias("LOSE_CLIENT_NUM11")
    )

    df_92_y = update_dataframe(
        df_to_update=df_92_y,
        df_use_me=df_y,
        join_columns=["busi_year", "oa_branch_id"],
        update_columns=[
            "LOSE_CLIENT_NUM11",
        ]
    )

    """
    流失客户数-本年未流失客户平均净贡献增长率
    150个客户在2022年的净贡献，除以，150；假设为C
    150个客户在2021年的净贡献，除以，150；假设为D
    结果为：C/D—1"
    """

    # 本年和上年同时千万工程
    tmp = df_qw.alias("t") \
        .join(
        other=df_qw_last.alias("a"),
        on=(
                col("t.fund_account_id") == col("a.fund_account_id")
        ),
        how="inner"
    ).groupBy(
        col("t.fund_account_id"),
        col("t.oa_branch_id")
    ).select(
        "t.fund_account_id",
        "t.oa_branch_id"
    )

    # 客户数
    tmp1 = tmp.alias("t") \
        .groupBy(
        col("t.oa_branch_id")
    ).agg(
        count("1").alias("client_num")
    ).select(
        "t.oa_branch_id",
        "client_num"
    )

    # 当年净贡献
    tmp2 = tmp.alias("t") \
        .join(
        other=df_jgx.alias("a"),
        on=(
                col("t.fund_account_id") == col("a.client_id")
        ),
        how="inner"
    ).groupBy(
        col("t.oa_branch_id")
    ).agg(
        sum(
            col("a.jgx")
        ).alias("jgx_sum")
    ).select(
        "t.oa_branch_id",
        "jgx_sum"
    )

    # 上一年净贡献
    tmp3 = tmp.alias("t") \
        .join(
        other=df_jgx_last.alias("a"),
        on=(
                col("t.fund_account_id") == col("a.client_id")
        ),
        how="inner"
    ).groupBy(
        col("t.oa_branch_id")
    ).agg(
        sum(
            col("a.jgx")
        ).alias("jgx_sum")
    ).select(
        "t.oa_branch_id",
        "jgx_sum"
    )

    # 本年平均净贡献
    tmp4 = tmp1.alias("t") \
        .join(
        other=tmp2.alias("a"),
        on=(
                col("t.oa_branch_id") == col("a.oa_branch_id")
        ),
        how="left"
    ).select(
        "t.oa_branch_id",
        when(
            col("t.client_num") != lit(0),
            coalesce(col("a.jgx_sum"), lit(0)) / col("t.client_num")
        ).otherwise(lit(0)).alias("jgx_rate")
    )

    # 上一年平均净贡献
    tmp5 = tmp1.alias("t") \
        .join(
        other=tmp3.alias("a"),
        on=(
                col("t.oa_branch_id") == col("a.oa_branch_id")
        ),
        how="left"
    ).select(
        "t.oa_branch_id",
        when(
            col("t.client_num") != lit(0),
            coalesce(col("a.jgx_sum"), lit(0)) / col("t.client_num")
        ).otherwise(lit(0)).alias("jgx_rate")
    )

    df_y = df_92_y.alias("t") \
        .join(
        other=tmp4.alias("a"),
        on=(
                col("t.oa_branch_id") == col("a.oa_branch_id")
        ),
        how="left"
    ).join(
        other=tmp5.alias("b"),
        on=(
                col("t.oa_branch_id") == col("b.oa_branch_id")
        ),
        how="left"
    ).select(
        col("t.oa_branch_id"),
        when(
            coalesce(col("b.jgx_rate"), lit(0)) != lit(0),
            coalesce(col("a.jgx_rate"), lit(0)) / coalesce(col("b.jgx_rate"), lit(0)) - 1
        ).otherwise(lit(0)).alias("LOSE_CLIENT_NUM12")
    )

    df_92_y = update_dataframe(
        df_to_update=df_92_y,
        df_use_me=df_y,
        join_columns=["busi_year", "oa_branch_id"],
        update_columns=[
            "LOSE_CLIENT_NUM12",
        ]
    )

    """
    流失客户数-增创净贡献
    150个客户在2022年的净贡献，减去，150个客户在2021年的净贡献
    """

    # 本年和上年同时千万工程
    tmp = df_qw.alias("t") \
        .join(
        other=df_qw_last.alias("a"),
        on=(
                col("t.fund_account_id") == col("a.fund_account_id")
        ),
        how="inner"
    ).groupBy(
        col("t.fund_account_id"),
        col("t.oa_branch_id")
    ).select(
        "t.fund_account_id",
        "t.oa_branch_id"
    )

    # 当年净贡献
    tmp1 = tmp.alias("t") \
        .join(
        other=df_jgx.alias("a"),
        on=(
                col("t.fund_account_id") == col("a.client_id")
        ),
        how="inner"
    ).groupBy(
        col("t.oa_branch_id")
    ).agg(
        sum(
            col("a.jgx")
        ).alias("jgx_sum")
    ).select(
        "t.oa_branch_id",
        "jgx_sum"
    )

    # 上一年净贡献
    tmp2 = tmp.alias("t") \
        .join(
        other=df_jgx_last.alias("a"),
        on=(
                col("t.fund_account_id") == col("a.client_id")
        ),
        how="inner"
    ).groupBy(
        col("t.oa_branch_id")
    ).agg(
        sum(
            col("a.jgx")
        ).alias("jgx_sum")
    ).select(
        "t.oa_branch_id",
        "jgx_sum"
    )

    df_y = df_92_y.alias("t") \
        .join(
        other=tmp2.alias("a"),
        on=(
                col("t.oa_branch_id") == col("a.oa_branch_id")
        ),
        how="left"
    ).join(
        other=tmp3.alias("b"),
        on=(
                col("t.oa_branch_id") == col("b.oa_branch_id")
        ),
        how="left"
    ).select(
        col("t.oa_branch_id"),
        (
            coalesce(col("a.jgx_sum"), lit(0)) - coalesce(col("b.jgx_sum"), lit(0))
        ).alias("LOSE_CLIENT_NUM13")
    )

    df_92_y = update_dataframe(
        df_to_update=df_92_y,
        df_use_me=df_y,
        join_columns=["busi_year", "oa_branch_id"],
        update_columns=[
            "LOSE_CLIENT_NUM13",
        ]
    )

    return_to_hive(
        spark=spark,
        df_result=df_92_y,
        target_table="ddw.t_brp_00092",
        insert_mode="overwrite",
        partition_column=["busi_year"],
        partition_value=v_busi_year
    )
