# -*- coding: utf-8 -*-
from datetime import datetime, timedelta

from pyspark.sql.functions import col, lit, min, max, count, sum, coalesce, when, trim, regexp_replace

from config import Config
from utils.P_COCKPIT_00110_BEFORE import p_cockpit_00110_before
from utils.task_env import return_to_hive, update_dataframe, log

logger = Config().get_logger()

@log
def p_cockpit_00133_data(spark, busi_date):
    """
    经纪业务收入、权益情况-数据落地
    :param spark: SparkSession对象
    :param busi_date: 业务日期, 格式：yyyymmdd
    :return: None
    """
    i_month_id = busi_date[:6]

    df_date_trade = spark.table("edw.t10_pub_date") \
        .filter(
        (col("busi_date").substr(1, 6) == i_month_id) &
        (col("market_no") == "1") &
        (col("trade_flag") == "1")
    ).agg(
        min("busi_date").alias("v_begin_trade_date"),
        max("busi_date").alias("v_end_trade_date"),
        count("*").alias("v_trade_days")
    )

    first_row_trade = df_date_trade.first()
    v_begin_trade_date = first_row_trade["v_begin_trade_date"]
    v_end_trade_date = first_row_trade["v_end_trade_date"]
    v_trade_days = first_row_trade["v_trade_days"]

    df_date = spark.table("edw.t10_pub_date") \
        .filter(
        (col("busi_date").substr(1, 6) == i_month_id) &
        (col("market_no") == "1")
    ).agg(
        min("busi_date").alias("v_begin_date"),
        max("busi_date").alias("v_end_date")
    )

    first_row = df_date.first()
    v_begin_date = first_row["v_begin_date"]
    v_end_date = first_row["v_end_date"]

    logger.info(
        u"""
        v_begin_trade_date: %s
        v_end_trade_date:   %s
        v_trade_days:       %s
        v_begin_date:       %s
        v_end_date:         %s
        参数处理完毕
        """,
        v_begin_trade_date, v_end_trade_date, v_trade_days, v_begin_date, v_end_date
    )

    # ------------------- 参数处理完毕 -------------------

    df_113_m = spark.table("ddw.v_oa_branch") \
        .withColumn("busi_month", lit(i_month_id))

    return_to_hive(
        spark=spark,
        df_result=df_113_m,
        target_table="ddw.T_COCKPIT_00133",
        insert_mode="overwrite",
        partition_column="busi_month",
        partition_value=i_month_id
    )

    df_113_m = spark.table("ddw.T_COCKPIT_00133") \
        .filter(col("busi_month") == i_month_id)

    """
    财务内核表-调整前 cf_busimg.t_cockpit_00127
    经纪业务收入-留存手续费收入 REMAIN_TRANSFEE_INCOME
    经纪业务收入-交易所减免净收入 MARKET_REDUCE_INCOME
    经纪业务收入-利息净收入 CLEAR_INTEREST_INCOME
    """

    df_127 = spark.table("ddw.t_cockpit_00127").alias("df_127")
    df_b = spark.table("ddw.T_YY_BRANCH_OA_RELA").alias("df_b")

    df_y = df_127.join(df_b, col("df_127.book_id") == col("df_b.yy_book_id")) \
        .filter(col("month_id") == i_month_id) \
        .groupby(col("df_b.oa_branch_id"), col("df_b.oa_branch_name")) \
        .agg(
        sum("b6").alias("REMAIN_TRANSFEE_INCOME"),
        sum("b8").alias("MARKET_REDUCE_INCOME"),
        sum("b7").alias("CLEAR_INTEREST_INCOME")
    ).alias("df_y")

    df_113_m = update_dataframe(
        df_to_update=df_113_m,
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=["REMAIN_TRANSFEE_INCOME", "MARKET_REDUCE_INCOME", "CLEAR_INTEREST_INCOME"]
    )

    """
    经纪业务收入=留存手续费收入+交易所手续费减免净收入+利息净收入
    """

    df_113_m = df_113_m \
        .withColumn("FUTU_INCOME_new",
                    coalesce(col("REMAIN_TRANSFEE_INCOME"), lit(0)) +
                    coalesce(col("MARKET_REDUCE_INCOME"), lit(0)) +
                    coalesce(col("CLEAR_INTEREST_INCOME"), lit(0))
                    ) \
        .drop("FUTU_INCOME") \
        .withColumnRenamed("FUTU_INCOME_new", "FUTU_INCOME")

    """更新
    CF_BUSIMG.T_COCKPIT_00122  投资咨询基本信息维护参数表-主表
    CF_BUSIMG.T_COCKPIT_00122_1  投资咨询基本信息-内核分配比例-表1
    交易咨询-交易咨询收入   TRANS_CONSULT_INCOME  (投资咨询服务费总额：)
    交易咨询-销售收入  SALES_INCOME (投资咨询内核表——销售部门收入，业务单位作为销售部门时的收入)
    """

    df_122 = spark.table("ddw.t_cockpit_00122") \
        .filter(col("busi_month") == i_month_id).alias("t")
    df_122_1 = spark.table("ddw.t_cockpit_00122_1") \
        .filter(col("busi_month") == i_month_id).alias("a")
    df_fund_account = spark.table("edw.h12_fund_account").alias("b")
    df_branch_oa = spark.table("ddw.t_ctp_branch_oa_rela").alias("c")

    df_y = df_122.join(
        other=df_122_1,
        on=["client_id", "product_name"],
        how="left"
    ).join(
        other=df_fund_account,
        on=col("t.client_id") == col("b.fund_account_id"),
        how="left"
    ).join(
        other=df_branch_oa,
        on=col("b.branch_id") == col("c.CTP_BRANCH_ID"),
        how="inner"
    ).groupby(
        "c.oa_branch_id", "c.oa_branch_name"
    ).agg(
        sum(col("t.INVEST_TOTAL_SERVICE_FEE")).alias("TRANS_CONSULT_INCOME"),
        sum(
            when(
                condition=col("a.ALLOCA_OA_BRANCH_TYPE") == lit("1"),
                value=col("t.INVEST_TOTAL_SERVICE_FEE") * col("t.kernel_total_rate") * col("a.alloca_kernel_rate")
            ).otherwise(lit(0))
        ).alias("SALES_INCOME")
    )

    df_113_m = update_dataframe(
        df_to_update=df_113_m,
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=["TRANS_CONSULT_INCOME", "SALES_INCOME"]
    )

    """更新
    产品销售收入 PRODUCT_SALES_INCOME 未开发 默认未0
    """

    """更新
    CF_BUSIMG.T_COCKPIT_00126   场外协同清算台账参数表
    场外期权收入  OUT_OPTION_INCOME（ 场外期权-场外协同清算台账-合计收入 TOTAL_INCOME）
    名义本金  NOTIONAL_PRINCIPAL
    场外期权规模-权利金  ROYALTY
    """

    df_126 = spark.table("ddw.t_cockpit_00126").alias("t")

    df_y = df_126.join(
        other=df_fund_account,
        on=col("t.client_id") == col("b.fund_account_id"),
        how="left"
    ).join(
        other=df_branch_oa,
        on=col("b.branch_id") == col("c.CTP_BRANCH_ID"),
        how="inner"
    ).groupby(
        "c.oa_branch_id", "c.oa_branch_name"
    ).agg(
        sum(col("t.TOTAL_INCOME")).alias("OUT_OPTION_INCOME"),
        sum(col("t.NOTIONAL_PRINCIPAL")).alias("NOTIONAL_PRINCIPAL"),
        sum(col("t.TOTAL_ABSOLUTE_ROYALTY")).alias("ROYALTY")
    )

    df_113_m = update_dataframe(
        df_to_update=df_113_m,
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=["OUT_OPTION_INCOME", "NOTIONAL_PRINCIPAL", "ROYALTY"]
    )

    """更新
    未开发 默认未0
    IB协同/驻点业务协同收入
    自有资金项目参与收入
    日均权益
    考核日均权益
    """

    """更新
    经纪业务规模-期末权益
    期末权益 END_RIGHTS
    """

    df_sett_m = spark.table("edw.h15_client_sett") \
        .alias("sett_m") \
        .filter(col("sett_m.busi_date") == v_end_trade_date)

    df_y = df_sett_m.join(
        other=df_fund_account,
        on=col("sett_m.client_id") == col("b.fund_account_id"),
        how="left"
    ).join(
        other=df_branch_oa,
        on=col("b.branch_id") == col("c.CTP_BRANCH_ID"),
        how="inner"
    ).groupby(
        "c.oa_branch_id", "c.oa_branch_name"
    ).agg(
        sum(col("sett_m.rights")).alias("END_RIGHTS")
    ).select(
        "c.oa_branch_id", "c.oa_branch_name", "END_RIGHTS"
    )

    update_dataframe(
        df_to_update=df_113_m,
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=["END_RIGHTS"]
    )

    """
    产品销售规模 保有量 PRODUCT_INVENTORY
    产品销售规模 新增量 PRODUCT_NEW 
    """

    df_96 = spark.table("ddw.t_cockpit_00096") \
        .filter(col("busi_date").between(v_begin_trade_date, v_end_trade_date))

    df_y = df_96.alias("a").join(
        other=df_fund_account.alias("b"),
        on=(
                (col("a.id_no") == col("b.id_no")) &
                (col("a.client_name") == col("b.client_name"))
        ),
        how="inner"
    ).join(
        other=df_branch_oa.alias("c"),
        on=col("b.branch_id") == col("c.CTP_BRANCH_ID"),
        how="inner"
    ).groupby(
        "c.oa_branch_id", "c.oa_branch_name"
    ).agg(
        sum(col("a.confirm_share")).alias("PRODUCT_INVENTORY"),
        sum(
            when(
                col("a.wh_trade_type").isin(["0", "1"]),
                col("a.confirm_share")
            ).otherwise(0)
        ).alias("PRODUCT_NEW")
    ).select(
        "c.oa_branch_id", "c.oa_branch_name", "PRODUCT_INVENTORY", "PRODUCT_NEW"
    )

    update_dataframe(
        df_to_update=df_113_m,
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=["PRODUCT_INVENTORY", "PRODUCT_NEW"]
    )

    """
    IB协同/驻点业务规模-期末权益 IB_END_RIGHTS  “IB协同统计汇总表——期末权益”与“驻点人员营销统计数据表——期末权益”之和
    IB协同/驻点业务规模-日均权益 IB_AVG_RIGHTS  “IB协同统计汇总表——日均权益”与“驻点人员营销统计数据表——日均权益”之和
    """

    """
    IB协同统计汇总表——期末权益
    """

    df_tmp = spark.table("ddw.t_cockpit_00107").alias("t") \
        .join(
        other=df_branch_oa.alias("c"),
        on=col("t.branch_id") == col("c.CTP_BRANCH_ID"),
        how="inner"
    ).filter(
        (col("t.confirm_date").between(v_begin_trade_date, v_end_trade_date))
    ).select(
        col("t.confirm_date").alias("busi_date"), "t.fund_account_id", "c.OA_BRANCH_ID"
    ).dropDuplicates()

    df_133_1 = df_sett_m.alias("t") \
        .join(
        other=df_tmp.alias("a"),
        on=(
            (col("t.busi_date") == col("a.busi_date")) &
            (col("t.fund_account_id") == col("a.fund_account_id"))
        ),
        how="inner"
    ).filter(
        (col("t.busi_date").between(v_begin_trade_date, v_end_trade_date))
    ).groupby(
        "a.OA_BRANCH_ID"
    ).agg(
        sum(when(col("t.busi_date") == lit(1), col("t.rights")).otherwise(0)).alias("end_rights"),
        sum(when(lit(v_trade_days) > 0, col("t.rights") / v_trade_days).otherwise(0)).alias("avg_rights")
    ).select(
        "a.OA_BRANCH_ID", "end_rights", "avg_rights"
    )

    # return_to_hive(
    #     spark=spark,
    #     df_result=df_133_1,
    #     target_table="ddw.tmp_cockpit_00133_1",
    #     insert_mode="overwrite"
    # )

    """
    根据查询日期获取业务人员关系数据
    """

    df_tmp = spark.table("ods.T_DS_CRM_BROKER_INVESTOR_RELA").alias("a") \
        .join(
        other=spark.table("ods.T_DS_CRM_BROKER").alias("b"),
        on=col("a.broker_id") == col("b.broker_id"),
        how="inner"
    ).join(
        other=spark.table("ods.T_DS_MDP_DEPT00").alias("f"),
        on=col("b.department_id") == col("f.chdeptcode"),
        how="inner"
    ).join(
        other=spark.table("ods.T_DS_DC_INVESTOR").alias("c"),
        on=col("a.investor_id") == col("c.investor_id"),
        how="inner"
    ).join(
        other=df_fund_account.alias("x"),
        on=col("c.investor_id") == col("x.fund_account_id"),
        how="left"
    ).join(
        other=spark.table("edw.h11_branch").alias("x1"),
        on=col("x.branch_id") == col("x1.branch_id"),
        how="left"
    ).join(
        other=df_branch_oa.alias("x2"),
        on=col("x1.branch_id") == col("x2.ctp_branch_id"),
        how="left"
    ).filter(
        (col("b.broker_id").like('ZD%')) &
        (col("a.RELA_STS") == lit('A')) &
        (col("a.APPROVE_STS") == lit('0')) &
        (~col("a.data_pct").isNull())
    ).select(
        col("a.BROKER_ID"),
        col("b.broker_nam").alias("BROKER_NAME"),
        trim(col("a.INVESTOR_ID")).alias("FUND_ACCOUNT_ID"),
        col("c.investor_nam").alias("CLIENT_NAME"),
        regexp_replace(col("a.ST_DT"), '-', '').alias("BEGIN_DATE"),
        regexp_replace(col("a.END_DT"), '-', '').alias("END_DATE"),
        when(col("a.BROKER_RELA_TYP") == lit('301'), '居间关系')
        .when(col("a.BROKER_RELA_TYP") == lit('001'), '开发关系')
        .when(col("a.BROKER_RELA_TYP") == lit('002'), '服务关系')
        .when(col("a.BROKER_RELA_TYP") == lit('003'), '维护关系')
        .otherwise('-').alias("BROKER_RELA_TYPE"),
        col("a.data_pct").alias("DATA_PCT"),
        when(col("a.RELA_STS") == lit('A'), '有效')
        .when(col("a.RELA_STS") == lit('S'), '停止使用')
        .otherwise('-').alias("RELA_STATUS"),
        col("a.APPROVE_DT").alias("APPROVE_DT"),
        when(col("a.APPROVE_STS") == lit('0'), '审核通过')
        .when(col("a.APPROVE_STS") == lit('1'), '审核不通过')
        .when(col("a.APPROVE_STS") == lit('2'), '等待审核')
        .otherwise('-').alias("APPROVE_STS"),
        col("a.comment_desc").alias("COMMENT_DESC"),
        col("a.check_comments").alias("CHECK_COMMENTS"),
        when(regexp_replace(col("a.ST_DT"), "-", "") < v_begin_date, v_begin_date)
        .when(regexp_replace(col("a.ST_DT"), "-", "") >= v_begin_date, col("a.ST_DT"))
        .otherwise(lit('')).alias("REAL_BEGIN_DATE"),
        when(regexp_replace(col("a.ST_DT"), "-", "") <= v_end_date, col("a.END_DT"))
        .when(regexp_replace(col("a.ST_DT"), "-", "") > v_end_date, v_end_date)
        .otherwise(lit('')).alias("REAL_END_DATE")
    ).alias("df_tmp")

    df_133_2 = df_tmp.filter(col("REAL_BEGIN_DATE") <= col("REAL_END_DATE"))

    # return_to_hive(
    #     spark=spark,
    #     df_result=df_133_2,
    #     target_table="ddw.tmp_cockpit_00133_2",
    #     insert_mode="overwrite"
    # )

    """
    按照日期计算 期末权益  日均权益
    """

    df_tmp = df_133_2.select(
        col("FUND_ACCOUNT_ID"),
        col("REAL_BEGIN_DATE"),
        col("REAL_END_DATE")
    ).dropDuplicates()

    df_133_3 = spark.table("edw.h15_client_sett").alias("t") \
        .filter(col("t.busi_date").between(v_begin_date, v_end_date)) \
        .join(
        other=df_tmp.alias("a"),
        on=(
                (col("t.fund_account_id") == col("a.fund_account_id")) &
                (col("t.busi_date").between(col("a.REAL_BEGIN_DATE"), col("a.REAL_END_DATE")))
        ),
        how="inner"
    ).groupby(
        "t.fund_account_id"
    ).agg(
        sum(when(col("t.busi_date") == v_end_trade_date, col("t.rights")).otherwise(lit(0))).alias("END_RIGHTS"),
        sum(when(lit(v_trade_days) > 0, col("t.rights") / v_trade_days).otherwise(lit(0))).alias("avg_rights")
    ).select(
        col("t.fund_account_id"),
        col("END_RIGHTS"),
        col("avg_rights")
    )

    # return_to_hive(
    #     spark=spark,
    #     df_result=df_133_3,
    #     target_table="ddw.tmp_cockpit_00133_3",
    #     insert_mode="overwrite"
    # )

    """
    按照结束日期所在月份获取 “权益分配表——FOF产品”涉及到IB业务部客户的字段“分配日均权益合计” 如果本月没有数据，取上月数据
    CF_BUSIMG.TMP_COCKPIT_00110_7
    """

    p_cockpit_00110_before(spark, busi_date[:6])

    df_110_7 = spark.table("ddw.TMP_COCKPIT_00110_7")

    v_fof_count = df_110_7.agg(count("*").alias("count")).first()["count"]

    # 根据v_end_date生成v_last_month
    end_date = datetime.strptime(v_end_date, "%Y%m%d")
    last_month = end_date.replace(day=1) - timedelta(days=1)
    v_last_month = last_month.strftime("%Y%m")
    # 如果本月没有数据，取上月数据
    if v_fof_count == 0:
        p_cockpit_00110_before(spark, v_last_month)

    """
    汇总数据
    """

    tmp = df_133_2.alias("t") \
        .join(
        other=df_fund_account.alias("x"),
        on=col("t.FUND_ACCOUNT_ID") == col("x.fund_account_id"),
        how="inner"
    ).join(
        other=df_branch_oa.alias("x1"),
        on=col("x.branch_id") == col("x1.ctp_branch_id"),
        how="inner"
    ).join(
        other=df_133_3.alias("b"),
        on=col("t.FUND_ACCOUNT_ID") == col("b.fund_account_id"),
        how="left"
    ).select(
        col("x1.oa_branch_id"),
        col("t.fund_account_id"),
        (coalesce(col("b.end_rights"), lit(0)) * col("t.data_pct")).alias("end_rights"),  # 期末权益
        (coalesce(col("b.avg_rights"), lit(0)) * col("t.data_pct")).alias("avg_rights"),  # 日均权益
    )

    tmp1 = tmp.groupby("oa_branch_id") \
        .agg(
        sum("end_rights").alias("end_rights"),  # 期末权益
        sum("avg_rights").alias("avg_rights"),  # 日均权益
    ).select(
        col("oa_branch_id"),
        col("end_rights"),
        col("avg_rights"),
    )

    # 权益分配表——FOF产品 的日均权益合计
    tmp2 = df_110_7.groupby("oa_branch_id") \
        .agg(
        sum("SUM_AVG_RIGHTS").alias("SUM_AVG_RIGHTS"),
    ).select(
        col("oa_branch_id"),
        col("SUM_AVG_RIGHTS"),
    )

    df_133_4 = tmp1.alias("t") \
        .join(
        other=tmp2.alias("a"),
        on=col("t.oa_branch_id") == col("a.oa_branch_id"),
        how="left"
    ).select(
        "t.oa_branch_id",
        "t.end_rights",
        (col("t.avg_rights") + coalesce(col("a.SUM_AVG_RIGHTS"), lit(0))).alias("avg_rights"),
    )

    df_y = df_133_1.alias("t") \
        .join(
        other=df_133_4.alias("b"),
        on=col("t.oa_branch_id") == col("b.oa_branch_id"),
        how="left"
    ).select(
        "t.oa_branch_id",
        (col("t.end_rights") + coalesce(col("b.end_rights"), lit(0))).alias("IB_END_RIGHTS"),
        (col("t.avg_rights") + coalesce(col("b.avg_rights"), lit(0))).alias("IB_AVG_RIGHTS"),
    )

    df_113_m = update_dataframe(
        df_to_update=df_113_m,
        df_use_me=df_y,
        join_columns=["oa_branch_id"],
        update_columns=["IB_END_RIGHTS", "IB_AVG_RIGHTS"]
    )

    return_to_hive(
        spark=spark,
        df_result=df_113_m,
        target_table="ddw.T_COCKPIT_00133",
        insert_mode="overwrite",
        partition_column="busi_month",
        partition_value=i_month_id
    )
