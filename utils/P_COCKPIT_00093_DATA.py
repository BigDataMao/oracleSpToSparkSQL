# -*- coding: utf-8 -*-
import logging

from pyspark.sql.functions import col, lit, count, when, regexp_replace, coalesce, length, substring, sum, round

from utils.task_env import return_to_hive, log


@log
def p_cockpit_00093_data(spark, busi_date):
    """
    宏源-千万工程“开户时间区间落地数据
    """
    # 日期处理
    v_busi_year = busi_date[:4]
    v_begin_date = v_busi_year + '0101'
    v_end_date = busi_date

    v_begin_month = v_busi_year + '01'
    v_end_month = busi_date[:6]

    # 计算客户的净贡献
    # decode(1, 2, a2.pct, 3, a2.data_pct, 1) 返回1，所以都忽略
    df_ds_adm_investor_value = spark.table("ods.t_ds_adm_investor_value")
    df_ds_adm_brokerdata_detail = spark.table("ods.t_ds_adm_brokerdata_detail")
    df_y = df_ds_adm_investor_value.alias("a") \
        .filter(
        regexp_replace(col("a.date_dt"), pattern="-", replacement="").between(lit(v_begin_date), lit(v_end_date))
    ).join(
        other=df_ds_adm_brokerdata_detail.alias("a2"),
        on=(
                col("a.date_dt") == col("a2.tx_dt") &
                col("a.investor_id") == col("a2.investor_id") &
                col("a2.rec_freq") == lit("M")
        ),
        how="left"
    ).groupBy(
        col("a.investor_id")
    ).agg(
        (
                sum(col("a.subsistence_fee_amt")) * round(sum(col("a.int_amt")), 2) +
                round(sum(col("a.exchangeret_amt")), 2) +
                sum(col("a.oth_amt")) - round(sum(col("a.fd_amt")), 2) - round(sum(col("a.i_int_amt")), 2) -
                sum(col("a.i_exchangeret_amt")) - sum(col("a.broker_amt")) - sum(col("a.soft_amt")) -
                sum(col("a.i_oth_amt")) - round(sum(col("a.broker_int_amt")), 2) - round(sum(col("a.broker_eret_amt")),2) -
                round(sum(coalesce(col("a2.ib_amt"), col("a.staff_amt"))), 2) -
                round(sum(coalesce(col("a2.STAFF_INT_AMT"), col("a.STAFF_INT_AMT"))), 2) -
                round(sum(col("a.staff_eret_amt")), 2)
        ).alias("jgx")
    ).select(
        col("a.investor_id").alias("client_id")
    )

    return_to_hive(
        spark=spark,
        df_result=df_y,
        target_table="ddw.tmp_cockpit_client_jgx",
        insert_mode="overwrite"
    )

    df_cockpit_client_jgx = spark.table("ddw.tmp_cockpit_client_jgx")
    df_his_label_client = spark.table("ods.t_crmmg_his_label_client")
    df_label = spark.table("ods.t_crmmg_label")
    df_fund_account = spark.table("edw.h12_fund_account")
    df_ctp_branch_oa = spark.table("ddw.t_ctp_branch_oa_rela")

    df_tmp = df_his_label_client.alias("t") \
        .join(
        other=df_label.alias("a"),
        on=(
                col("t.label_id") == col("a.label_id") &
                col("a.label_id") == lit("BQ4909")  # 千万工程
        ),
        how="inner"
    ).join(
        other=df_fund_account.alias("b"),
        on=(
                col("t.client_id") == col("b.fund_account_id")

        ),
        how="left"
    ).join(
        other=df_ctp_branch_oa.alias("c"),
        on=(
                col("b.branch_id") == col("c.ctp_branch_id")

        ),
        how="inner"
    ).join(
        other=df_cockpit_client_jgx.alias("d"),
        on=(
                col("t.client_id") == col("d.client_id")

        ),
        how="left"
    ).filter(
        col("c.oa_branch_id").isNotNull() &
        col("t.months").between(lit(v_begin_month), lit(v_end_month)) &
        col("b.client_type") == lit("0")  # 自然人
    ).groupBy(
        col("b.fund_account_id"),
        col("b.open_date"),
        col("c.oa_branch_id"),
        col("c.oa_branch_name"),
        (
            when(
                condition=length(col("b.id_no")) == lit(18),
                value=substring(col("b.id_no"), -12, 8)
            ).when(
                condition=length(col("b.id_no")) == lit(15),
                value=lit("19") + substring(col("b.id_no"), -9, 6)
            ).otherwise(lit(0))
        ).alias("birth"),
        col("d.jgx")
    ).select(
        col("b.fund_account_id"),
        col("b.open_date"),
        col("c.oa_branch_id"),
        col("c.oa_branch_name"),
        (
            when(
                condition=length(col("b.id_no")) == lit(18),
                value=substring(col("b.id_no"), -12, 8)
            ).when(
                condition=length(col("b.id_no")) == lit(15),
                value=lit("19") + substring(col("b.id_no"), -9, 6)
            ).otherwise(lit(0))
        ).alias("birth"),
        coalesce(col("d.jgx"), lit(0)).alias("jgx")
    ).alias("df_tmp")

    df_tmp1 = df_tmp.alias("t") \
        .groupBy(
        col("t.oa_branch_id"),
        col("t.oa_branch_name"),
        col("t.open_date"),
        col("t.birth")
    ).agg(
        count("*").alias("client_num"),
        sum(col("t.jgx")).alias("jgx_sum")
    ).select(
        col("t.oa_branch_id"),
        col("t.oa_branch_name"),
        # --开户时间(0:2018年及前/1:2019年/2:2020年/3:2021年/4:2022年及以后)
        (
            when(
                condition=length(col("t.open_date")) <= lit("20181231"),
                value=lit("0")
            ).when(
                condition=length(col("t.open_date")).between(lit("20190101"), lit("20191231")),
                value=lit("1")
            ).when(
                condition=length(col("t.open_date")).between(lit("20200101"), lit("20201231")),
                value=lit("2")
            ).when(
                condition=length(col("t.open_date")).between(lit("20210101"), lit("20211231")),
                value=lit("3")
            ).when(
                condition=length(col("t.open_date")) >= lit("20220101"),
                value=lit("4")
            ).otherwise(lit(""))
        ).alias("open_date_flag"),
        # 客户身份证信息(0:60后及以下/1:70后/2:80后/3:90后/4:00后)
        (
            when(
                condition=length(col("t.birth")) <= lit("19691231"),
                value=lit("0")  # 60后
            ).when(
                condition=length(col("t.birth")).between(lit("19700101"), lit("19791231")),
                value=lit("1")  # 70后
            ).when(
                condition=length(col("t.birth")).between(lit("19800101"), lit("19891231")),
                value=lit("2")  # 80后
            ).when(
                condition=length(col("t.birth")).between(lit("19900101"), lit("19991231")),
                value=lit("3")  # 90后
            ).when(
                condition=length(col("t.birth")) >= lit("20000101"),
                value=lit("4")  # 00后
            ).otherwise(lit(""))
        ).alias("age_range")
    ).alias("df_tmp1")

    df_y = df_tmp1.alias("t") \
        .groupBy(
        col("t.oa_branch_id"),
        col("t.oa_branch_name"),
        col("t.open_date_flag"),
        col("t.age_range")
    ).agg(
        sum(col("t.client_num")).alias("client_num"),
        sum(col("t.jgx_sum") / lit(10000)).alias("jgx_sum"),
        sum(
            when(
                condition=col("t.client_num") != lit(0),
                value=col("t.jgx_sum") / col("t.client_num")
            ).otherwise(lit(0))
        ).alias("avg_jgx")
    ).select(
        lit(v_busi_year).alias("busi_year"),
        col("t.oa_branch_id"),
        col("t.oa_branch_name"),
        col("t.open_date_flag"),
        col("t.t.age_range")
    )

    return_to_hive(
        spark=spark,
        df_result=df_y,
        target_table="ddw.t_cockpit_00093",
        insert_mode="overwrite",
        partition_column=["busi_year"],
        partition_value=[v_busi_year]
    )

    logging.info("ddw.t_cockpit_00093写入完成")
