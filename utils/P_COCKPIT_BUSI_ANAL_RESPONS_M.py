# *# -*- coding: utf-8 -*-
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sum, when, coalesce, countDistinct, count

from config import Config
from utils.date_utils import get_date_period_and_days
from utils.io_utils.common_uitls import to_color_str
from utils.task_env import return_to_hive, update_dataframe

logger = Config().get_logger()


def p_cockpit_busi_anal_respons_m(spark: SparkSession, busi_date):
    """
    经营分析-分管部门-按月落地
    :param spark: SparkSession对象
    :param busi_date: 业务日期,格式为"YYYYMMDD"
    :return: None
    """
    logger.info("p_cockpit_busi_anal_respons_m执行开始")

    def insert_and_reload(df):
        return_to_hive(
            spark=spark,
            df_result=df,
            target_table="ddw.T_COCKPIT_BUSI_ANAL_RESPONS_M",
            insert_mode="overwrite"
        )
        return spark.table("ddw.T_COCKPIT_BUSI_ANAL_RESPONS_M").filter(
            col("BUSI_MONTH") == i_month_id
        )

    v_begin_date = busi_date[:4] + "0101"
    v_begin_month = busi_date[:4] + "01"
    i_month_id = busi_date[:6]
    # 获取当前日期字符串
    datetime_str = datetime.now().strftime("%Y%m%d")
    (
        v_end_date,
        v_now_begin_date,
        v_now_trade_days
    ) = get_date_period_and_days(
        spark=spark,
        busi_month=i_month_id,
        end_date=datetime_str,
        is_trade_day=True
    )

    _, _, v_busi_trade_days = get_date_period_and_days(
        spark=spark,
        begin_date=v_begin_date,
        end_date=v_end_date,
        is_trade_day=True
    )

    v_new_begin_date = v_begin_date
    v_new_end_date = v_end_date
    v_now_end_date = v_end_date

    # 同比月份
    v_yoy_month_id = str(int(i_month_id) - 12)
    # 同比日期
    tmp_date = get_date_period_and_days(spark=spark, busi_month=v_yoy_month_id, is_trade_day=True)
    v_yoy_begin_date = tmp_date[0]
    v_yoy_end_date = tmp_date[1]
    v_yoy_trade_days = tmp_date[2]

    # 同比开始日期(从去年1号开始)
    v_yoy_all_begin_date = v_yoy_month_id[:4] + "0101"
    # 同比结束日期(v_end_date的前一年)
    v_yoy_all_end_date = str(int(v_end_date[:4]) - 1) + v_end_date[4:]

    # 从当年1号到当月最后一个交易日的天数
    v_busi_trade_days = get_date_period_and_days(
        spark=spark,
        begin_date=v_begin_month,
        end_date=v_end_date,
        is_trade_day=True
    )[2]

    # 初始化数据
    logger.info(to_color_str("初始化数据", "blue"))

    df_m = spark.table("ddw.t_Respons_Line").alias("t") \
        .filter(
        col("t.if_use") == "1"
    ).select(
        lit(i_month_id).alias("BUSI_MONTH"),
        col("t.RESPONS_LINE_ID"),
    )

    # 必须先写回hive表再重新读,否则没有完整元数据信息
    return_to_hive(
        spark=spark,
        df_result=df_m,
        target_table="ddw.T_COCKPIT_BUSI_ANAL_RESPONS_M",
        insert_mode="overwrite"
    )

    df_m = spark.table("ddw.T_COCKPIT_BUSI_ANAL_RESPONS_M") \
        .filter(
        col("BUSI_MONTH") == i_month_id
    )

    df_oa_branch = spark.table("ddw.T_OA_BRANCH").filter(
        col("RESPONS_LINE_ID").isNotNull()
    )

    logger.info(to_color_str("协同业务开始", "blue"))

    """
    --协同业务-产品销售规模-保有量
    --协同业务-产品销售规模-新增量

    月度销售人员保有奖励分配情况—客户保有份额 选择当前月份，显示历史到当月的数据
    月度销售人员保有奖励分配情况—新增量  选择当前月份，显示当年1月到当月数据
    过程：CF_BUSIMG.P_COCKPIT_00099
    """

    tmp = spark.table("ddw.T_COCKPIT_00096").alias("t") \
        .filter(
        (col("t.busi_date") <= v_end_date)
    ).join(
        other=spark.table("edw.h11_client").alias("b"),
        on=(col("t.client_name") == col("b.client_name")),
        how="inner"
    ).join(
        other=spark.table("ddw.T_CTP_BRANCH_OA_RELA").alias("c"),
        on=(col("b.branch_id") == col("c.ctp_branch_id")),
        how="inner"
    ).join(
        other=df_oa_branch.alias("d"),
        on=(col("c.oa_branch_id") == col("d.departmentid")),
        how="inner"
    ).groupBy(
        col("d.RESPONS_LINE_ID")
    ).agg(
        sum("t.confirm_share").alias("XT_PRODUCT_SALES_STOCK")
    ).select(
        col("d.RESPONS_LINE_ID"),
        col("XT_PRODUCT_SALES_STOCK")
    )

    tmp_new = spark.table("ddw.T_COCKPIT_00096").alias("t") \
        .filter(
        (col("t.busi_date").between(v_begin_date, v_end_date))
    ).join(
        other=spark.table("edw.h11_client").alias("b"),
        on=(col("t.client_name") == col("b.client_name")),
        how="inner"
    ).join(
        other=spark.table("ddw.T_CTP_BRANCH_OA_RELA").alias("c"),
        on=(col("b.branch_id") == col("c.ctp_branch_id")),
        how="inner"
    ).join(
        other=df_oa_branch.alias("d"),
        on=(col("c.oa_branch_id") == col("d.departmentid")),
        how="inner"
    ).groupBy(
        col("d.RESPONS_LINE_ID")
    ).agg(
        sum(
            when(
                col("t.wh_trade_type").isin("0", "1"),
                col("t.confirm_share")
            ).otherwise(lit(0))
        ).alias("XT_PRODUCT_SALES_NEW")
    ).select(
        col("d.RESPONS_LINE_ID"),
        col("XT_PRODUCT_SALES_NEW")
    )

    df_y = df_m.alias("t") \
        .join(
        other=tmp.alias("b"),
        on=(col("t.RESPONS_LINE_ID") == col("b.RESPONS_LINE_ID")),
        how="left"
    ).join(
        other=tmp_new.alias("c"),
        on=(col("t.RESPONS_LINE_ID") == col("c.RESPONS_LINE_ID")),
        how="left"
    ).select(
        col("t.RESPONS_LINE_ID"),
        (col("b.XT_PRODUCT_SALES_STOCK") / 10000).alias("XT_PRODUCT_SALES_STOCK"),
        (col("c.XT_PRODUCT_SALES_NEW") / 10000).alias("XT_PRODUCT_SALES_NEW")
    )

    # 更新数据
    df_m = update_dataframe(
        df_to_update=df_m,
        df_use_me=df_y,
        join_columns=["RESPONS_LINE_ID"],
        update_columns=[
            "XT_PRODUCT_SALES_STOCK",
            "XT_PRODUCT_SALES_NEW"
        ]
    )

    # 写回hive,再重读分区
    df_m = insert_and_reload(df_m)

    logger.info(to_color_str("协同业务-交易咨询-开始", "blue"))

    """
    --协同业务-交易咨询-交易咨询收入 当年到当前月份数据
    --协同业务-交易咨询-销售收入   当年到当前月份数据
    --过程：CF_BUSIMG.P_COCKPIT_00123
    """

    tmp = spark.table("ddw.T_COCKPIT_00122").alias("t") \
        .filter(
        (col("t.busi_month").between(v_begin_month, i_month_id))
    ).join(
        other=spark.table("ddw.T_COCKPIT_00122_1").alias("a"),
        on=(
                (col("t.busi_month") == col("a.busi_month")) &
                (col("t.client_id") == col("a.client_id")) &
                (col("t.product_name") == col("a.product_name"))
        ),
        how="left"
    ).join(
        other=df_oa_branch.alias("d"),
        on=(col("a.ALLOCA_OA_BRANCH_ID") == col("d.departmentid")),
        how="inner"
    ).groupBy(
        col("d.RESPONS_LINE_ID"),
        col("a.ALLOCA_OA_BRANCH_TYPE")
    ).agg(
        sum(
            col("t.INVEST_TOTAL_SERVICE_FEE") * col("t.kernel_total_rate") * col("a.alloca_kernel_rate")
        ).alias("alloca_income")
    ).select(
        col("d.RESPONS_LINE_ID"),
        col("a.ALLOCA_OA_BRANCH_TYPE"),
        col("alloca_income")
    )

    df_y = tmp.alias("t") \
        .groupBy(
        col("t.RESPONS_LINE_ID")
    ).agg(
        sum(
            when(
                col("t.ALLOCA_OA_BRANCH_TYPE").isin("0", "1"),
                col("t.alloca_income") / 10000
            ).otherwise(lit(0))
        ).alias("XT_TRADE_ZX_INCOME"),
        sum(
            when(
                col("t.ALLOCA_OA_BRANCH_TYPE") == "1",
                col("t.alloca_income") / 10000
            ).otherwise(lit(0))
        ).alias("XT_TRADE_ZX_XC_INCOME")
    ).select(
        col("t.RESPONS_LINE_ID"),
        col("XT_TRADE_ZX_INCOME"),
        col("XT_TRADE_ZX_XC_INCOME")
    )

    # 更新数据
    df_m = update_dataframe(
        df_to_update=df_m,
        df_use_me=df_y,
        join_columns=["RESPONS_LINE_ID"],
        update_columns=[
            "XT_TRADE_ZX_INCOME",
            "XT_TRADE_ZX_XC_INCOME"
        ]
    )

    # 写回hive,再重读分区
    df_m = insert_and_reload(df_m)

    logger.info(to_color_str("协同业务-交易咨询-结束", "blue"))

    logger.info(to_color_str("协同业务-IB协同/驻点业务-日均权益", "blue"))

    """
    --协同业务-IB协同/驻点业务-日均权益  选择当前月份，显示当前月份
    /*
    日均权益 “IB协同统计汇总表——日均权益”与“驻点人员营销统计数据表——日均权益”之和

    CF_BUSIMG.P_COCKPIT_00109  IB协同统计汇总表
    CF_BUSIMG.P_COCKPIT_00110  驻点人员营销统计数据表

    CF_BUSIMG.P_COCKPIT_00178_DATA IB协同统计汇总表 落地数据
    CF_BUSIMG.P_COCKPIT_00179_DATA 驻点人员营销统计数据表 落地数据
    */

    --协同业务-IB协同/驻点业务-日均权益
    """

    tmp = spark.table("Ddw.T_COCKPIT_00178").alias("t") \
        .filter(
        col("t.month_id") == i_month_id
    ).join(
        other=spark.table("ddw.T_CTP_BRANCH_OA_RELA").alias("b"),
        on=(col("t.branch_id") == col("b.ctp_branch_id")),
        how="inner"
    ).join(
        other=df_oa_branch.alias("d"),
        on=(col("b.oa_branch_id") == col("d.departmentid")),
        how="inner"
    ).groupBy(
        col("d.RESPONS_LINE_ID")
    ).agg(
        sum("t.AVG_RIGHTS").alias("AVG_RIGHTS")
    ).select(
        col("d.RESPONS_LINE_ID"),
        (col("AVG_RIGHTS") / 10000).alias("AVG_RIGHTS")
    )

    tmp1 = spark.table("Ddw.T_COCKPIT_00179").alias("t") \
        .filter(
        col("t.month_id") == i_month_id
    ).join(
        other=df_oa_branch.alias("d"),
        on=(col("t.oa_branch_id") == col("d.departmentid")),
        how="inner"
    ).groupBy(
        col("d.RESPONS_LINE_ID")
    ).agg(
        sum("t.AVG_RIGHTS").alias("AVG_RIGHTS")
    ).select(
        col("d.RESPONS_LINE_ID"),
        (col("AVG_RIGHTS") / 10000).alias("AVG_RIGHTS")
    )

    df_y = df_m.alias("t") \
        .join(
        other=tmp.alias("b"),
        on=(col("t.RESPONS_LINE_ID") == col("b.RESPONS_LINE_ID")),
        how="left"
    ).join(
        other=tmp1.alias("c"),
        on=(col("t.RESPONS_LINE_ID") == col("c.RESPONS_LINE_ID")),
        how="left"
    ).select(
        col("t.RESPONS_LINE_ID"),
        ((
                 coalesce(col("b.AVG_RIGHTS"), lit(0)) +
                 coalesce(col("c.AVG_RIGHTS"), lit(0))
         ) / 10000).alias("XT_COLLA_AVG_RIGHTS")
    )

    # 更新数据
    df_m = update_dataframe(
        df_to_update=df_m,
        df_use_me=df_y,
        join_columns=["RESPONS_LINE_ID"],
        update_columns=[
            "XT_COLLA_AVG_RIGHTS"
        ]
    )

    # 写回hive,再重读分区
    df_m = insert_and_reload(df_m)

    logger.info(to_color_str("协同业务-IB协同/驻点业务-日均权益-完成", "blue"))

    """
    --协同业务-IB协同/驻点业务-协同收入

    “IB协同收入调整表——收入合计”与“IB驻点收入调整表—— 收入合计”之和

    CF_BUSIMG.P_COCKPIT_00125  IB协同收入调整表
    CF_BUSIMG.P_COCKPIT_00142  IB驻点收入调整表

    需要扣税/1.06；
    协同是“ IB协同利息收入、减免收入、协同收入”之和，驻点里是“IB驻点利息收入、IB驻点减免收入、IB驻点留存收入”之和，两项加总

    CF_BUSIMG.P_COCKPIT_00175_DATA  IB协同收入调整表 落地数据
    CF_BUSIMG.P_COCKPIT_00176_DATA  IB驻点收入调整表 落地数据
    """

    logger.info(to_color_str("协同业务-IB协同/驻点业务-协同收入-开始", "blue"))

    tmp = spark.table("ddw.T_COCKPIT_00175").alias("t") \
        .filter(
        col("t.month_id") == i_month_id
    ).join(
        other=spark.table("ddw.T_CTP_BRANCH_OA_RELA").alias("b"),
        on=(col("t.branch_id") == col("b.ctp_branch_id")),
        how="inner"
    ).join(
        other=df_oa_branch.alias("d"),
        on=(col("b.oa_branch_id") == col("d.departmentid")),
        how="inner"
    ).groupBy(
        col("d.RESPONS_LINE_ID")
    ).agg(
        sum("t.xt_income").alias("XT_COLLA_INCOME")
    ).select(
        col("d.RESPONS_LINE_ID"),
        col("XT_COLLA_INCOME")
    )

    tmp1 = spark.table("ddw.T_COCKPIT_00176").alias("t") \
        .filter(
        col("t.month_id") == i_month_id
    ).join(
        other=spark.table("ddw.T_CTP_BRANCH_OA_RELA").alias("b"),
        on=(col("t.branch_id") == col("b.ctp_branch_id")),
        how="inner"
    ).join(
        other=df_oa_branch.alias("d"),
        on=(col("b.oa_branch_id") == col("d.departmentid")),
        how="inner"
    ).groupBy(
        col("d.RESPONS_LINE_ID")
    ).agg(
        sum("t.ZD_INCOME").alias("XT_COLLA_INCOME")
    ).select(
        col("d.RESPONS_LINE_ID"),
        col("XT_COLLA_INCOME")
    )

    df_y = df_m.alias("t") \
        .join(
        other=tmp.alias("b"),
        on=(col("t.RESPONS_LINE_ID") == col("b.RESPONS_LINE_ID")),
        how="left"
    ).join(
        other=tmp1.alias("c"),
        on=(col("t.RESPONS_LINE_ID") == col("c.RESPONS_LINE_ID")),
        how="left"
    ).select(
        col("t.RESPONS_LINE_ID"),
        (
                (
                        (coalesce(col("b.XT_COLLA_INCOME"), lit(0))) +
                        (coalesce(col("c.XT_COLLA_INCOME"), lit(0)))
                ) / 1.06 / 10000
        ).alias("XT_COLLA_INCOME")
    )

    # 更新数据
    df_m = update_dataframe(
        df_to_update=df_m,
        df_use_me=df_y,
        join_columns=["RESPONS_LINE_ID"],
        update_columns=[
            "XT_COLLA_INCOME"
        ]
    )

    # 写回hive,再重读分区
    df_m = insert_and_reload(df_m)

    logger.info(to_color_str("协同业务-IB协同/驻点业务-协同收入-完成", "blue"))

    """
    --协同业务-场外期权-名义本金
    --协同业务-场外期权-权利金
    --协同业务-场外期权-协同收入

    场外协同清算台账——名义本金
    场外协同清算台账——权利金绝对值总额
    场外协同清算台账——销售收入+协同定价收入
    当年到当前月份数据
    CF_BUSIMG.P_COCKPIT_00126
    """

    logger.info(to_color_str("协同业务-场外期权-开始", "blue"))

    df_y = spark.table("ddw.T_COCKPIT_00126").alias("t") \
        .filter(
        (col("t.done_date").substr(1, 6).between(v_begin_month, i_month_id))
    ).join(
        other=df_oa_branch.alias("d"),
        on=(col("t.oa_branch_id") == col("d.departmentid")),
        how="inner"
    ).groupBy(
        col("d.RESPONS_LINE_ID")
    ).agg(
        sum("t.NOTIONAL_PRINCIPAL").alias("XT_OFF_SITE_PRINCI"),
        sum("t.TOTAL_ABSOLUTE_ROYALTY").alias("XT_OFF_SITE_OPTINO"),
        (sum("t.SALES_REVENUE") + sum("t.COLLABORATIVE_PRICE")).alias("XT_OFF_SITE_INCOME")
    ).select(
        col("d.RESPONS_LINE_ID"),
        (col("XT_OFF_SITE_PRINCI") / 10000).alias("XT_OFF_SITE_PRINCI"),
        (col("XT_OFF_SITE_OPTINO") / 10000).alias("XT_OFF_SITE_OPTINO"),
        (col("XT_OFF_SITE_INCOME") / 10000).alias("XT_OFF_SITE_INCOME")
    )

    # 更新数据
    df_m = update_dataframe(
        df_to_update=df_m,
        df_use_me=df_y,
        join_columns=["RESPONS_LINE_ID"],
        update_columns=[
            "XT_OFF_SITE_PRINCI",
            "XT_OFF_SITE_OPTINO",
            "XT_OFF_SITE_INCOME"
        ]
    )

    # 写回hive,再重读分区
    df_m = insert_and_reload(df_m)

    logger.info(to_color_str("协同业务-场外期权-结束", "blue"))

    """
    --协同业务-自有资金参与项目-项目数量
    --协同业务-自有资金参与项目-期末权益
    --协同业务-自有资金参与项目-自有资金扣减额
    --协同业务-自有资金参与项目-协同收入

    自有资金投资项目综合收益情况跟踪表，可以筛选出：项目数量
    自有资金投资项目综合收益情况跟踪表——项目期末权益
    自有资金权益分配表——部门扣减日均权益
    自有资金投资项目综合收益情况跟踪表——本月协同收入
    选择当前月份，显示当年1月到当月数据
    CF_BUSIMG.P_COCKPIT_00172    自有资金投资项目综合收益情况跟踪表
    CF_BUSIMG.P_COCKPIT_00168_EDIT_2_Q  自有资金权益分配表

    CF_BUSIMG.P_COCKPIT_00177_DATA 落地数据 自有资金投资项目综合收益情况跟踪表
    """

    logger.info(to_color_str("协同业务-自有资金参与项目-开始", "blue"))

    tmp = spark.table("ddw.T_COCKPIT_00177").alias("t") \
        .filter(
        (col("t.busi_month").between(v_begin_month, i_month_id))
    ).join(
        other=df_oa_branch.alias("d"),
        on=(col("t.recommend_department") == col("d.departmentid")),
        how="inner"
    ).groupBy(
        col("d.RESPONS_LINE_ID")
    ).agg(
        countDistinct("t.PROJECT_REFERRED").alias("XT_OWN_FUNDS_NUMS"),
        sum("t.ENDING_INTEREST").alias("XT_OWN_FUNDS_END_RIGHTS"),
        sum("t.SYNERGISTIC_REVENUE").alias("XT_OWN_FUNDS_INCOME")
    ).select(
        col("d.RESPONS_LINE_ID"),
        col("XT_OWN_FUNDS_NUMS"),
        # 不要认为这里错了,只是提前除以10000,AI帮我写的
        (col("XT_OWN_FUNDS_END_RIGHTS") / 10000).alias("XT_OWN_FUNDS_END_RIGHTS"),
        (col("XT_OWN_FUNDS_INCOME") / 10000).alias("XT_OWN_FUNDS_INCOME")
    )

    tmp1 = spark.table("ddw.T_COCKPIT_00168_EDIT_2").alias("t") \
        .filter(
        (col("t.busi_month").between(v_begin_month, i_month_id))
    ).join(
        other=df_oa_branch.alias("d"),
        on=(col("t.ALLOCATION_DEPART") == col("d.departmentid")),
        how="inner"
    ).groupBy(
        col("d.RESPONS_LINE_ID")
    ).agg(
        sum("t.DEPART_REAL_ALLOCATION_RIGHT").alias("XT_OWN_FUNDS_DISCOUNTS")
    ).select(
        col("d.RESPONS_LINE_ID"),
        (col("XT_OWN_FUNDS_DISCOUNTS") / 10000).alias("XT_OWN_FUNDS_DISCOUNTS")
    )

    df_y = df_m.alias("t") \
        .join(
        other=tmp.alias("b"),
        on=(col("t.RESPONS_LINE_ID") == col("b.RESPONS_LINE_ID")),
        how="left"
    ).join(
        other=tmp1.alias("c"),
        on=(col("t.RESPONS_LINE_ID") == col("c.RESPONS_LINE_ID")),
        how="left"
    ).select(
        col("t.RESPONS_LINE_ID"),
        col("b.XT_OWN_FUNDS_NUMS"),
        col("b.XT_OWN_FUNDS_END_RIGHTS"),
        col("c.XT_OWN_FUNDS_DISCOUNTS"),
        col("b.XT_OWN_FUNDS_INCOME")
    )

    # 更新数据
    df_m = update_dataframe(
        df_to_update=df_m,
        df_use_me=df_y,
        join_columns=["RESPONS_LINE_ID"],
        update_columns=[
            "XT_OWN_FUNDS_NUMS",
            "XT_OWN_FUNDS_END_RIGHTS",
            "XT_OWN_FUNDS_DISCOUNTS",
            "XT_OWN_FUNDS_INCOME"
        ]
    )

    # 写回hive,再重读分区
    df_m = insert_and_reload(df_m)

    logger.info(to_color_str("协同业务-自有资金参与项目-结束", "blue"))

    """
    --财务指标-考核收入
    --财务指标-考核利润-考核利润
    """

    logger.info(to_color_str("财务指标-考核收入,考核利润-开始", "blue"))

    tmp = spark.table("ddw.T_COCKPIT_00172").alias("t") \
        .filter(
        col("t.month_id") == i_month_id
    ).join(
        other=spark.table("ddw.T_YY_BRANCH_OA_RELA").alias("b"),
        on=(col("t.book_id") == col("b.yy_book_id")),
        how="inner"
    ).join(
        other=df_oa_branch.alias("d"),
        on=(col("b.oa_branch_id") == col("d.departmentid")),
        how="inner"
    ).groupBy(
        col("d.RESPONS_LINE_ID")
    ).agg(
        sum("t.f5").alias("ASSESS_INCOME"),
        sum("t.f21").alias("ASSESS_PROFIT")
    ).select(
        col("d.RESPONS_LINE_ID"),
        (col("ASSESS_INCOME") / 10000).alias("ASSESS_INCOME"),
        (col("ASSESS_PROFIT") / 10000).alias("ASSESS_PROFIT")
    )

    tmp_last = spark.table("ddw.T_COCKPIT_00172").alias("t") \
        .filter(
        col("t.month_id") == v_yoy_month_id
    ).join(
        other=spark.table("ddw.T_YY_BRANCH_OA_RELA").alias("b"),
        on=(col("t.book_id") == col("b.yy_book_id")),
        how="inner"
    ).join(
        other=df_oa_branch.alias("d"),
        on=(col("b.oa_branch_id") == col("d.departmentid")),
        how="inner"
    ).groupBy(
        col("d.RESPONS_LINE_ID")
    ).agg(
        sum("t.f5").alias("ASSESS_INCOME"),
        sum("t.f21").alias("ASSESS_PROFIT")
    ).select(
        col("d.RESPONS_LINE_ID"),
        (col("ASSESS_INCOME") / 10000).alias("ASSESS_INCOME"),
        (col("ASSESS_PROFIT") / 10000).alias("ASSESS_PROFIT")
    )

    df_y = tmp.alias("t") \
        .join(
        other=tmp_last.alias("t1"),
        on=(col("t.RESPONS_LINE_ID") == col("t1.RESPONS_LINE_ID")),
        how="left"
    ).select(
        col("t.RESPONS_LINE_ID"),
        col("t.ASSESS_INCOME"),
        when(
            (col("t1.ASSESS_INCOME") != 0),
            (col("t.ASSESS_INCOME") / col("t1.ASSESS_INCOME") - 1) * 100
        ).otherwise(lit(0)).alias("ASSESS_INCOME_YOY"),
        col("t.ASSESS_PROFIT"),
        when(
            (col("t1.ASSESS_PROFIT") != 0),
            (col("t.ASSESS_PROFIT") / col("t1.ASSESS_PROFIT") - 1) * 100
        ).otherwise(lit(0)).alias("ASSESS_PROFIT_YOY")
    )

    # 更新数据
    df_m = update_dataframe(
        df_to_update=df_m,
        df_use_me=df_y,
        join_columns=["RESPONS_LINE_ID"],
        update_columns=[
            "ASSESS_INCOME",
            "ASSESS_INCOME_YOY",
            "ASSESS_PROFIT",
            "ASSESS_PROFIT_YOY"
        ]
    )

    # 写回hive,再重读分区
    df_m = insert_and_reload(df_m)

    logger.info(to_color_str("财务指标-考核收入,考核利润-结束", "blue"))

    """
    --财务指标-考核收入-交易所手续费减免净收入
    --财务指标-考核收入-留存手续费收入
    --财务指标-考核收入-利息净收入
    """

    logger.info(to_color_str("财务指标-考核收入-交易所手续费减免净收入,留存手续费收入,利息净收入-开始", "blue"))

    tmp = spark.table("ddw.T_COCKPIT_00174").alias("t") \
        .filter(
        col("t.busi_month") == i_month_id
    ).join(
        other=spark.table("ddw.T_CTP_BRANCH_OA_RELA").alias("c"),
        on=(col("t.branch_id") == col("c.ctp_branch_id")),
        how="inner"
    ).join(
        other=df_oa_branch.alias("d"),
        on=(col("c.oa_branch_id") == col("d.departmentid")),
        how="inner"
    ).groupBy(
        col("d.RESPONS_LINE_ID")
    ).agg(
        sum("t.market_reduct").alias("EXCH_NET_INCOME"),
        sum("t.feature_income_total").alias("feature_income_total"),
        sum("t.remain_transfee").alias("REMAIN_TRANSFEE_INCOME"),
        sum("t.interest_income").alias("ASSESS_INTEREST_INCOME")
    ).select(
        col("d.RESPONS_LINE_ID"),
        (col("EXCH_NET_INCOME") / 10000).alias("EXCH_NET_INCOME"),
        (col("EXCH_NET_INCOME") / col("feature_income_total") * 100).alias("EXCH_NET_INCOME_PROP"),
        (col("REMAIN_TRANSFEE_INCOME") / 10000).alias("REMAIN_TRANSFEE_INCOME"),
        (col("REMAIN_TRANSFEE_INCOME") / col("feature_income_total") * 100).alias("REMAIN_TRANSFEE_INCOME_PROP"),
        (col("ASSESS_INTEREST_INCOME") / 10000).alias("ASSESS_INTEREST_INCOME"),
        (col("ASSESS_INTEREST_INCOME") / col("feature_income_total") * 100).alias("ASSESS_INTEREST_INCOME_PROP")
    ).fillna(0)

    # 更新数据
    df_m = update_dataframe(
        df_to_update=df_m,
        df_use_me=tmp,
        join_columns=["RESPONS_LINE_ID"],
        update_columns=[
            "EXCH_NET_INCOME",
            "EXCH_NET_INCOME_PROP",
            "REMAIN_TRANSFEE_INCOME",
            "REMAIN_TRANSFEE_INCOME_PROP",
            "ASSESS_INTEREST_INCOME",
            "ASSESS_INTEREST_INCOME_PROP"
        ]
    )

    # 写回hive,再重读分区
    df_m = insert_and_reload(df_m)

    logger.info(to_color_str("财务指标-考核收入-交易所手续费减免净收入,留存手续费收入,利息净收入-结束", "blue"))

    logger.info(to_color_str("财务指标-全部结束", "blue"))

    # 收入结构-分析指标-经纪业务收入

    logger.info(to_color_str("收入结构-分析指标-经纪业务收入-开始", "blue"))

    tmp = spark.table("ddw.T_COCKPIT_00174").alias("t") \
        .filter(
        (col("t.busi_month").between(v_begin_month, i_month_id))
    ).join(
        other=spark.table("edw.h12_fund_account").alias("b"),
        on=(col("t.fund_account_id") == col("b.fund_account_id")),
        how="left"
    ).join(
        other=spark.table("ddw.T_CTP_BRANCH_OA_RELA").alias("c"),
        on=(col("b.branch_id") == col("c.ctp_branch_id")),
        how="inner"
    ).join(
        other=df_oa_branch.alias("d"),
        on=(col("c.oa_branch_id") == col("d.departmentid")),
        how="inner"
    ).groupBy(
        col("t.fund_account_id"),
        col("d.RESPONS_LINE_ID"),
        when(
            (col("b.open_date").between(v_begin_date, v_end_date)),
            lit("1")
        ).otherwise(lit("0")).alias("is_new_flag")
    ).agg(
        sum("t.remain_transfee").alias("remain_transfee"),
        sum("t.INTEREST_INCOME").alias("INTEREST_INCOME"),
        sum("t.MARKET_REDUCT").alias("MARKET_REDUCT"),
        sum("t.FEATURE_INCOME_TOTAL").alias("FEATURE_INCOME_TOTAL")
    ).select(
        col("d.RESPONS_LINE_ID"),
        col("remain_transfee"),
        col("INTEREST_INCOME"),
        col("MARKET_REDUCT"),
        col("FEATURE_INCOME_TOTAL"),
        col("is_new_flag")
    )

    tmp_result = tmp.alias("t") \
        .groupBy(
        col("t.RESPONS_LINE_ID")
    ).agg(
        sum("t.remain_transfee").alias("CR_REMAIN_TRANSFEE_INCOME"),
        sum("t.MARKET_REDUCT").alias("MARKET_TRANSFEE_INCOME"),
        sum("t.INTEREST_INCOME").alias("INTEREST_INCOME"),
        sum("t.FEATURE_INCOME_TOTAL").alias("FUTURE_INCOME"),
        sum(
            when(
                col("t.is_new_flag") == "0",
                col("t.remain_transfee")
            ).otherwise(lit(0))
        ).alias("remain_transfee_stock"),
        sum(
            when(
                col("t.is_new_flag") == "1",
                col("t.remain_transfee")
            ).otherwise(lit(0))
        ).alias("remain_transfee_new"),
        sum(
            when(
                col("t.is_new_flag") == "0",
                col("t.MARKET_REDUCT")
            ).otherwise(lit(0))
        ).alias("MARKET_REDUCT_stock"),
        sum(
            when(
                col("t.is_new_flag") == "1",
                col("t.MARKET_REDUCT")
            ).otherwise(lit(0))
        ).alias("MARKET_REDUCT_new"),
        sum(
            when(
                col("t.is_new_flag") == "0",
                col("t.INTEREST_INCOME")
            ).otherwise(lit(0))
        ).alias("INTEREST_INCOME_stock"),
        sum(
            when(
                col("t.is_new_flag") == "1",
                col("t.INTEREST_INCOME")
            ).otherwise(lit(0))
        ).alias("INTEREST_INCOME_new")
    )

    df_y = tmp_result.alias("t") \
        .select(
        col("t.RESPONS_LINE_ID"),
        (col("t.FUTURE_INCOME") / 10000).alias("FUTURE_INCOME"),
        (col("t.CR_REMAIN_TRANSFEE_INCOME") / 10000).alias("CR_REMAIN_TRANSFEE_INCOME"),
        when(
            (col("t.FUTURE_INCOME") != 0),
            col("t.CR_REMAIN_TRANSFEE_INCOME") / col("t.FUTURE_INCOME") * 100
        ).otherwise(lit(0)).alias("CR_REMAIN_TRANSFEE_INCOME_PROP"),
        when(
            (col("t.CR_REMAIN_TRANSFEE_INCOME") != 0),
            col("t.remain_transfee_stock") / col("t.CR_REMAIN_TRANSFEE_INCOME") * 100
        ).otherwise(lit(0)).alias("REMAIN_TRANSFEE_STOCK_PROP"),
        when(
            (col("t.CR_REMAIN_TRANSFEE_INCOME") != 0),
            col("t.remain_transfee_new") / col("t.CR_REMAIN_TRANSFEE_INCOME") * 100
        ).otherwise(lit(0)).alias("REMAIN_TRANSFEE_NEW_PROP"),
        (col("t.MARKET_TRANSFEE_INCOME") / 10000).alias("MARKET_TRANSFEE_INCOME"),
        when(
            (col("t.FUTURE_INCOME") != 0),
            col("t.MARKET_TRANSFEE_INCOME") / col("t.FUTURE_INCOME") * 100
        ).otherwise(lit(0)).alias("MARKET_TRANSFEE_PROP"),
        when(
            (col("t.MARKET_TRANSFEE_INCOME") != 0),
            col("t.MARKET_REDUCT_stock") / col("t.MARKET_TRANSFEE_INCOME") * 100
        ).otherwise(lit(0)).alias("MARKET_TRANSFEE_STOCK_PROP"),
        when(
            (col("t.MARKET_TRANSFEE_INCOME") != 0),
            col("t.MARKET_REDUCT_new") / col("t.MARKET_TRANSFEE_INCOME") * 100
        ).otherwise(lit(0)).alias("MARKET_TRANSFEE_NEW_PROP"),
        (col("t.INTEREST_INCOME") / 10000).alias("INTEREST_INCOME"),
        when(
            (col("t.FUTURE_INCOME") != 0),
            col("t.INTEREST_INCOME") / col("t.FUTURE_INCOME") * 100
        ).otherwise(lit(0)).alias("INTEREST_INCOME_PROP"),
        when(
            (col("t.INTEREST_INCOME") != 0),
            col("t.INTEREST_INCOME_stock") / col("t.INTEREST_INCOME") * 100
        ).otherwise(lit(0)).alias("INTEREST_INCOME_STOCK_PROP"),
        when(
            (col("t.INTEREST_INCOME") != 0),
            col("t.INTEREST_INCOME_new") / col("t.INTEREST_INCOME") * 100
        ).otherwise(lit(0)).alias("INTEREST_INCOME_NEW_PROP")
    )

    # 更新数据
    df_m = update_dataframe(
        df_to_update=df_m,
        df_use_me=df_y,
        join_columns=["RESPONS_LINE_ID"],
        update_columns=[
            "FUTURE_INCOME",
            "CR_REMAIN_TRANSFEE_INCOME",
            "CR_REMAIN_TRANSFEE_INCOME_PROP",
            "REMAIN_TRANSFEE_STOCK_PROP",
            "REMAIN_TRANSFEE_NEW_PROP",
            "MARKET_TRANSFEE_INCOME",
            "MARKET_TRANSFEE_PROP",
            "MARKET_TRANSFEE_STOCK_PROP",
            "MARKET_TRANSFEE_NEW_PROP",
            "INTEREST_INCOME",
            "INTEREST_INCOME_PROP",
            "INTEREST_INCOME_STOCK_PROP",
            "INTEREST_INCOME_NEW_PROP"
        ]
    )

    # 写回hive,再重读分区
    df_m = insert_and_reload(df_m)

    logger.info(to_color_str("收入结构-分析指标-经纪业务收入-结束", "blue"))

    """
    --业务指标-日均权益 20240626
    --业务指标-日均权益同比 20240626
    --业务指标-考核日均权益 20240626  逻辑与日均权益一致
    --业务指标-考核日均权益同比  20240626 逻辑与日均权益一致
    """

    logger.info(to_color_str("业务指标-日均权益,日均权益同比,考核日均权益,考核日均权益同比-开始", "blue"))

    tmp = spark.table("edw.h15_client_sett").alias("t") \
        .filter(
        (col("t.busi_date").between(v_now_begin_date, v_now_end_date))
    ).join(
        other=spark.table("edw.h12_fund_account").alias("b"),
        on=(col("t.fund_account_id") == col("b.fund_account_id")),
        how="left"
    ).join(
        other=spark.table("ddw.T_CTP_BRANCH_OA_RELA").alias("c"),
        on=(col("b.branch_id") == col("c.ctp_branch_id")),
        how="inner"
    ).join(
        other=df_oa_branch.alias("d"),
        on=(col("c.oa_branch_id") == col("d.departmentid")),
        how="inner"
    ).groupBy(
        col("d.RESPONS_LINE_ID")
    ).agg(
        sum(
            when(
                (lit(v_now_trade_days) != 0),
                col("t.rights") / v_now_trade_days
            ).otherwise(lit(0))
        ).alias("avg_rights")
    ).select(
        col("d.RESPONS_LINE_ID"),
        (col("avg_rights") / 10000).alias("avg_rights")
    )

    tmp1 = spark.table("edw.h15_client_sett").alias("t") \
        .filter(
        (col("t.busi_date").between(v_yoy_begin_date, v_yoy_end_date))
    ).join(
        other=spark.table("edw.h12_fund_account").alias("b"),
        on=(col("t.fund_account_id") == col("b.fund_account_id")),
        how="left"
    ).join(
        other=spark.table("ddw.T_CTP_BRANCH_OA_RELA").alias("c"),
        on=(col("b.branch_id") == col("c.ctp_branch_id")),
        how="inner"
    ).join(
        other=df_oa_branch.alias("d"),
        on=(col("c.oa_branch_id") == col("d.departmentid")),
        how="inner"
    ).groupBy(
        col("d.RESPONS_LINE_ID")
    ).agg(
        sum(
            when(
                (lit(v_yoy_trade_days) != 0),
                col("t.rights") / v_yoy_trade_days
            ).otherwise(lit(0))
        ).alias("avg_rights")
    ).select(
        col("d.RESPONS_LINE_ID"),
        (col("avg_rights") / 10000).alias("avg_rights")
    )

    def get_tmp2_and_tmp3_depart_real_allocation_right(tmp_month):
        return spark.table("ddw.T_COCKPIT_00168_EDIT_2").alias("t") \
            .filter(
            col("t.busi_month") == tmp_month
        ).join(
            other=df_oa_branch.alias("d"),
            on=(col("t.ALLOCATION_DEPART") == col("d.departmentid")),
            how="inner"
        ).groupBy(
            col("d.RESPONS_LINE_ID")
        ).agg(
            sum("t.DEPART_REAL_ALLOCATION_RIGHT").alias("depart_real_allocation_right")
        ).select(
            col("d.RESPONS_LINE_ID"),
            (col("depart_real_allocation_right") / 10000).alias("depart_real_allocation_right")
        )

    tmp2 = get_tmp2_and_tmp3_depart_real_allocation_right(i_month_id)
    tmp3 = get_tmp2_and_tmp3_depart_real_allocation_right(v_yoy_month_id)

    df_y = tmp.alias("t") \
        .join(
        other=tmp1.alias("t1"),
        on=(col("t.RESPONS_LINE_ID") == col("t1.RESPONS_LINE_ID")),
        how="left"
    ).join(
        other=tmp2.alias("t2"),
        on=(col("t.RESPONS_LINE_ID") == col("t2.RESPONS_LINE_ID")),
        how="left"
    ).join(
        other=tmp3.alias("t3"),
        on=(col("t.RESPONS_LINE_ID") == col("t3.RESPONS_LINE_ID")),
        how="left"
    ).select(
        col("t.RESPONS_LINE_ID"),
        col("t.avg_rights"),
        ((col("t.avg_rights") / col("t1.avg_rights") - 1) * 100).alias("AVG_RIGHTS_YOY"),
        (col("t.avg_rights") - col("t2.depart_real_allocation_right")).alias("ASSESS_AVG_RIGHTS"),
        (
                (
                        (col("t.avg_rights") - col("t2.depart_real_allocation_right")) /
                        (col("t1.avg_rights") - col("t3.depart_real_allocation_right"))
                ) * 100 - 100
        ).alias(
            "ASSESS_AVG_RIGHTS_YOY")
    )

    # 更新数据
    df_m = update_dataframe(
        df_to_update=df_m,
        df_use_me=df_y,
        join_columns=["RESPONS_LINE_ID"],
        update_columns=[
            "AVG_RIGHTS",
            "AVG_RIGHTS_YOY",
            "ASSESS_AVG_RIGHTS",
            "ASSESS_AVG_RIGHTS_YOY"
        ]
    )

    # 写回hive,再重读分区
    df_m = insert_and_reload(df_m)

    logger.info(to_color_str("业务指标-日均权益,日均权益同比,考核日均权益,考核日均权益同比-结束", "blue"))

    """
    --业务指标-成交量 20240626
    --业务指标-成交量同比 20240626
    --业务指标-成交额 20240626
    --业务指标-成交额同比 20240626
    """

    logger.info(to_color_str("业务指标-成交量,成交量同比,成交额,成交额同比-开始", "blue"))

    def get_tmp_and_tmp1_done_amount_and_done_money(begin_month, end_month):
        return spark.table("edw.h15_hold_balance").alias("t") \
            .filter(
            (col("t.busi_date").between(begin_month, end_month))
        ).join(
            other=spark.table("edw.h12_fund_account").alias("b"),
            on=(col("t.fund_account_id") == col("b.fund_account_id")),
            how="left"
        ).join(
            other=spark.table("ddw.T_CTP_BRANCH_OA_RELA").alias("c"),
            on=(col("b.branch_id") == col("c.ctp_branch_id")),
            how="inner"
        ).join(
            other=df_oa_branch.alias("d"),
            on=(col("c.oa_branch_id") == col("d.departmentid")),
            how="inner"
        ).groupBy(
            col("d.RESPONS_LINE_ID")
        ).agg(
            sum("t.done_amt").alias("DOEN_AMOUNT"),
            sum("t.done_sum").alias("done_money")
        ).select(
            col("d.RESPONS_LINE_ID"),
            (col("DOEN_AMOUNT") / 10000).alias("DOEN_AMOUNT"),
            (col("done_money") / 100000000).alias("DONE_MONEY")
        )

    tmp = get_tmp_and_tmp1_done_amount_and_done_money(v_now_begin_date, v_now_end_date)
    tmp1 = get_tmp_and_tmp1_done_amount_and_done_money(v_yoy_begin_date, v_yoy_end_date)

    df_y = tmp.alias("t") \
        .join(
        other=tmp1.alias("t1"),
        on=(col("t.RESPONS_LINE_ID") == col("t1.RESPONS_LINE_ID")),
        how="left"
    ).select(
        col("t.RESPONS_LINE_ID"),
        col("t.DOEN_AMOUNT"),
        when(
            (col("t1.DOEN_AMOUNT") != 0),
            (col("t.DOEN_AMOUNT") / col("t1.DOEN_AMOUNT") - 1) * 100
        ).otherwise(lit(0)).alias("DOEN_AMOUNT_YOY"),
        col("t.DONE_MONEY"),
        when(
            (col("t1.DONE_MONEY") != 0),
            (col("t.DONE_MONEY") / col("t1.DONE_MONEY") - 1) * 100
        ).otherwise(lit(0)).alias("DONE_MONEY_YOY")
    )

    # 更新数据
    df_m = update_dataframe(
        df_to_update=df_m,
        df_use_me=df_y,
        join_columns=["RESPONS_LINE_ID"],
        update_columns=[
            "DOEN_AMOUNT",
            "DOEN_AMOUNT_YOY",
            "DONE_MONEY",
            "DONE_MONEY_YOY"
        ]
    )

    # 写回hive,再重读分区
    df_m = insert_and_reload(df_m)

    logger.info(to_color_str("业务指标-成交量,成交量同比,成交额,成交额同比-结束", "blue"))

    """
    -- 业务指标-总客户数,  20240626
    -- 业务指标-总客户数同比,  20240626
    -- 业务指标-新增开户数, 20240626 
    -- 业务指标-新增开户数同比 20240626
    """

    logger.info(to_color_str("业务指标-总客户数,总客户数同比,新增开户数,新增开户数同比-开始", "blue"))

    tmp = spark.table("edw.h11_client").alias("t") \
        .filter(
        col("t.isactive") != "3"
    ).join(
        other=spark.table("ddw.T_CTP_BRANCH_OA_RELA").alias("c"),
        on=(col("t.branch_id") == col("c.ctp_branch_id")),
        how="inner"
    ).join(
        other=df_oa_branch.alias("d"),
        on=(col("c.oa_branch_id") == col("d.departmentid")),
        how="inner"
    ).select(
        col("t.client_id"),
        col("d.RESPONS_LINE_ID"),
        col("t.open_date")
    )

    tmp1 = tmp.alias("t") \
        .groupBy(
        col("t.RESPONS_LINE_ID")
    ).agg(
        sum(
            when(
                col("t.open_date") <= v_end_date,
                1
            ).otherwise(0)
        ).alias("TOTAL_CLIENT_NUM"),
        sum(
            when(
                (col("t.open_date").between(v_now_begin_date, v_now_end_date)),
                1
            ).otherwise(0)
        ).alias("NEW_CLIENT_NUM")
    ).select(
        col("t.RESPONS_LINE_ID"),
        col("TOTAL_CLIENT_NUM"),
        col("NEW_CLIENT_NUM")
    )

    tmp2 = tmp.alias("t") \
        .groupBy(
        col("t.RESPONS_LINE_ID")
    ).agg(
        sum(
            when(
                col("t.open_date") <= v_yoy_all_end_date,
                1
            ).otherwise(0)
        ).alias("TOTAL_CLIENT_NUM"),
        sum(
            when(
                (col("t.open_date").between(v_yoy_begin_date, v_yoy_end_date)),
                1
            ).otherwise(0)
        ).alias("NEW_CLIENT_NUM")
    ).select(
        col("t.RESPONS_LINE_ID"),
        col("TOTAL_CLIENT_NUM"),
        col("NEW_CLIENT_NUM")
    )

    df_y = tmp1.alias("t") \
        .join(
        other=tmp2.alias("t1"),
        on=(col("t.RESPONS_LINE_ID") == col("t1.RESPONS_LINE_ID")),
        how="left"
    ).select(
        col("t.RESPONS_LINE_ID"),
        col("t.TOTAL_CLIENT_NUM"),
        when(
            (col("t1.TOTAL_CLIENT_NUM") != 0),
            (col("t.TOTAL_CLIENT_NUM") / col("t1.TOTAL_CLIENT_NUM") - 1) * 100
        ).otherwise(lit(0)).alias("TOTAL_CLIENT_NUM_YOY"),
        col("t.NEW_CLIENT_NUM"),
        when(
            (col("t1.NEW_CLIENT_NUM") != 0),
            (col("t.NEW_CLIENT_NUM") / col("t1.NEW_CLIENT_NUM") - 1) * 100
        ).otherwise(lit(0)).alias("NEW_CLIENT_NUM_YOY")
    )

    # 更新数据
    df_m = update_dataframe(
        df_to_update=df_m,
        df_use_me=df_y,
        join_columns=["RESPONS_LINE_ID"],
        update_columns=[
            "TOTAL_CLIENT_NUM",
            "TOTAL_CLIENT_NUM_YOY",
            "NEW_CLIENT_NUM",
            "NEW_CLIENT_NUM_YOY"
        ]
    )

    # 写回hive,再重读分区
    df_m = insert_and_reload(df_m)

    logger.info(to_color_str("业务指标-总客户数,总客户数同比,新增开户数,新增开户数同比-结束", "blue"))

    #   -- 业务指标-总有效客户数,  20240626
    #   -- 业务指标-总有效客户数同比,  20240626

    logger.info(to_color_str("业务指标-总有效客户数,总有效客户数同比-开始", "blue"))

    def get_tmp_and_tmp1_total_active_num(begin_date, end_date):
        return spark.table("edw.h15_hold_balance").alias("t") \
            .filter(
            (col("t.busi_date").between(begin_date, end_date))
        ).join(
            other=spark.table("ddw.T_CTP_BRANCH_OA_RELA").alias("c"),
            on=(col("t.branch_id") == col("c.ctp_branch_id")),
            how="inner"
        ).join(
            other=df_oa_branch.alias("d"),
            on=(col("c.oa_branch_id") == col("d.departmentid")),
            how="inner"
        ).select(
            col("t.client_id"),
            col("d.RESPONS_LINE_ID")
        )

    tmp = get_tmp_and_tmp1_total_active_num(v_now_begin_date, v_now_end_date)
    tmp1 = get_tmp_and_tmp1_total_active_num(v_yoy_all_begin_date, v_yoy_all_end_date)

    tmp2 = tmp.alias("t").groupBy("t.RESPONS_LINE_ID") \
        .agg(count("t.client_id").alias("TOTAL_ACTIVE_NUM")) \
        .select(col("t.RESPONS_LINE_ID"), col("TOTAL_ACTIVE_NUM"))

    tmp3 = tmp1.alias("t").groupBy("t.RESPONS_LINE_ID") \
        .agg(count("t.client_id").alias("TOTAL_ACTIVE_NUM")) \
        .select(col("t.RESPONS_LINE_ID"), col("TOTAL_ACTIVE_NUM"))

    df_y = tmp2.alias("t") \
        .join(
        other=tmp3.alias("t1"),
        on=(col("t.RESPONS_LINE_ID") == col("t1.RESPONS_LINE_ID")),
        how="left"
    ).select(
        col("t.RESPONS_LINE_ID"),
        col("t.TOTAL_ACTIVE_NUM"),
        when(
            (col("t1.TOTAL_ACTIVE_NUM") != 0),
            (col("t.TOTAL_ACTIVE_NUM") / col("t1.TOTAL_ACTIVE_NUM") - 1) * 100
        ).otherwise(lit(0)).alias("TOTAL_ACTIVE_NUM_YOY")
    )

    # 更新数据
    df_m = update_dataframe(
        df_to_update=df_m,
        df_use_me=df_y,
        join_columns=["RESPONS_LINE_ID"],
        update_columns=[
            "TOTAL_ACTIVE_NUM",
            "TOTAL_ACTIVE_NUM_YOY"
        ]
    )

    # 写回hive,再重读分区
    df_m = insert_and_reload(df_m)

    logger.info(to_color_str("业务指标-总有效客户数,总有效客户数同比-结束", "blue"))

    #   --业务结构-日均权益-存量客户 202406
    #   --业务结构-日均权益-新增客户 202406

    logger.info(to_color_str("业务结构-日均权益-存量客户,日均权益-新增客户-开始", "blue"))

    tmp_new = spark.table("edw.h15_client_sett").alias("t") \
        .filter(
        (col("t.busi_date").between(v_begin_date, v_end_date))
    ).join(
        other=spark.table("edw.h12_fund_account").alias("b"),
        on=(col("t.fund_account_id") == col("b.fund_account_id")),
        how="left"
    ).join(
        other=spark.table("ddw.T_CTP_BRANCH_OA_RELA").alias("c"),
        on=(col("b.branch_id") == col("c.ctp_branch_id")),
        how="inner"
    ).join(
        other=spark.table("ddw.T_OA_BRANCH").alias("d"),
        on=(col("c.oa_branch_id") == col("d.departmentid")),
        how="inner"
    ).filter(
        col("d.RESPONS_LINE_ID").isNotNull()
    ).groupBy(
        "t.fund_account_id",
        when(
            (col("b.open_date").between(v_begin_date, v_end_date)),
            lit("1")
        ).otherwise(lit("0")).alias("is_new_flag"),
        col("d.RESPONS_LINE_ID")
    ).agg(
        sum("t.rights").alias("sum_rights")
    ).select(
        col("t.fund_account_id"),
        col("is_new_flag"),
        col("d.RESPONS_LINE_ID"),
        col("sum_rights")
    )

    tmp_result = tmp_new.alias("t") \
        .groupBy("t.RESPONS_LINE_ID") \
        .agg(
        sum(when(
            (col("t.is_new_flag") == "1") & (lit(v_busi_trade_days) != 0),
            col("t.sum_rights") / v_busi_trade_days
        ).otherwise(lit(0))).alias("AVG_RIGHTS_NEW"),
        sum(when(
            (col("t.is_new_flag") == "0") & (lit(v_busi_trade_days) != 0),
            col("t.sum_rights") / v_busi_trade_days
        ).otherwise(lit(0))).alias("AVG_RIGHTS_STOCK"),
        sum(when(
            (lit(v_busi_trade_days) != 0),
            col("t.sum_rights") / v_busi_trade_days
        ).otherwise(lit(0))).alias("sum_avg_rights")
    ).select(
        col("t.RESPONS_LINE_ID"),
        col("AVG_RIGHTS_STOCK"),
        col("AVG_RIGHTS_NEW"),
        col("sum_avg_rights")
    )

    df_y = tmp_result.alias("t") \
        .select(
        (col("t.RESPONS_LINE_ID") / 10000).alias("RESPONS_LINE_ID"),
        (col("t.AVG_RIGHTS_STOCK") / 10000).alias("AVG_RIGHTS_STOCK"),
        when(
            (col("t.sum_avg_rights") != 0),
            col("t.AVG_RIGHTS_STOCK") / col("t.sum_avg_rights") * 100
        ).otherwise(lit(0)).alias("AVG_RIGHTS_STOCK_PROP"),
        col("t.AVG_RIGHTS_NEW"),
        when(
            (col("t.sum_avg_rights") != 0),
            col("t.AVG_RIGHTS_NEW") / col("t.sum_avg_rights") * 100
        ).otherwise(lit(0)).alias("AVG_RIGHTS_NEW_PROP")
    )

    # 更新数据
    df_m = update_dataframe(
        df_to_update=df_m,
        df_use_me=df_y,
        join_columns=["RESPONS_LINE_ID"],
        update_columns=[
            "AVG_RIGHTS_STOCK",
            "AVG_RIGHTS_STOCK_PROP",
            "AVG_RIGHTS_NEW",
            "AVG_RIGHTS_NEW_PROP"
        ]
    )

    # 写回hive,再重读分区
    return_to_hive(
        spark=spark,
        df_result=df_m,
        target_table="ddw.T_COCKPIT_BUSI_ANAL_RESPONS_M",
        insert_mode="overwrite"
    )

    df_m = spark.table("ddw.T_COCKPIT_BUSI_ANAL_RESPONS_M").filter(
        col("BUSI_MONTH") == i_month_id
    )

    """
    --业务结构-成交量-存量客户 20240626
    --业务结构-成交量-新增客户 20240626
    --业务结构-成交额-存量客户 20240626
    --业务结构-成交额-新增客户 20240626
    """
    logger.info(to_color_str("业务指标-成交量,成交额", "blue"))

    tmp_new = spark.table("edw.h15_hold_balance").alias("t") \
        .filter(
        (col("t.busi_date").between(v_begin_date, v_end_date))
    ).join(
        other=spark.table("edw.h12_fund_account").alias("b"),
        on=(col("t.fund_account_id") == col("b.fund_account_id")),
        how="left"
    ).join(
        other=spark.table("ddw.T_CTP_BRANCH_OA_RELA").alias("c"),
        on=(col("b.branch_id") == col("c.ctp_branch_id")),
        how="inner"
    ).join(
        other=spark.table("ddw.T_OA_BRANCH").alias("d"),
        on=(col("c.oa_branch_id") == col("d.departmentid")),
        how="inner"
    ).filter(
        col("d.RESPONS_LINE_ID").isNotNull()
    ).groupBy(
        "t.fund_account_id",
        when(
            (col("b.open_date").between(v_begin_date, v_end_date)),
            lit("1")
        ).otherwise(lit("0")).alias("is_new_flag"),
        col("d.RESPONS_LINE_ID")
    ).agg(
        sum("t.done_amt").alias("sum_done_amt"),
        sum("t.done_sum").alias("sum_done_sum")
    ).select(
        col("t.fund_account_id"),
        col("is_new_flag"),
        col("d.RESPONS_LINE_ID"),
        col("sum_done_amt"),
        col("sum_done_sum")
    )

    tmp_result = tmp_new.alias("t") \
        .groupBy(
        col("t.RESPONS_LINE_ID")
    ).agg(
        sum(when(
            (col("t.is_new_flag") == "1") & (lit(v_busi_trade_days) != 0),
            col("t.sum_done_amt")
        ).otherwise(lit(0))).alias("DONE_AMOUNT_NEW"),
        sum(when(
            (col("t.is_new_flag") == "0") & (lit(v_busi_trade_days) != 0),
            col("t.sum_done_amt")
        ).otherwise(lit(0))).alias("DONE_AMOUNT_STOCK"),
        sum(when(
            (col("t.is_new_flag") == "1") & (lit(v_busi_trade_days) != 0),
            col("t.sum_done_sum")
        ).otherwise(lit(0))).alias("DONE_MONEY_NEW"),
        sum(when(
            (col("t.is_new_flag") == "0") & (lit(v_busi_trade_days) != 0),
            col("t.sum_done_sum")
        ).otherwise(lit(0))).alias("DONE_MONEY_STOCK"),
        sum(col("t.sum_done_amt")).alias("SUM_DONE_AMOUNT"),
        sum(col("t.sum_done_sum")).alias("SUM_DONE_MONEY")
    ).select(
        col("t.RESPONS_LINE_ID"),
        col("DONE_AMOUNT_STOCK"),
        col("DONE_AMOUNT_NEW"),
        col("DONE_MONEY_STOCK"),
        col("DONE_MONEY_NEW"),
        col("SUM_DONE_AMOUNT"),
        col("SUM_DONE_MONEY")
    )

    df_y = tmp_result.alias("t") \
        .select(
        col("t.RESPONS_LINE_ID"),
        (col("t.DONE_AMOUNT_STOCK") / 10000).alias("DONE_AMOUNT_STOCK"),
        when(
            (col("t.SUM_DONE_AMOUNT") != 0),
            col("t.DONE_AMOUNT_STOCK") / col("t.SUM_DONE_AMOUNT") * 100
        ).otherwise(lit(0)).alias("DONE_AMOUNT_STOCK_PROP"),
        (col("t.DONE_AMOUNT_NEW") / 10000).alias("DONE_AMOUNT_NEW"),
        when(
            (col("t.SUM_DONE_AMOUNT") != 0),
            col("t.DONE_AMOUNT_NEW") / col("t.SUM_DONE_AMOUNT") * 100
        ).otherwise(lit(0)).alias("DONE_AMOUNT_NEW_PROP"),
        (col("t.DONE_MONEY_STOCK") / 10000).alias("DONE_MONEY_STOCK"),
        when(
            (col("t.SUM_DONE_MONEY") != 0),
            col("t.DONE_MONEY_STOCK") / col("t.SUM_DONE_MONEY") * 100
        ).otherwise(lit(0)).alias("DONE_MONEY_STOCK_PROP"),
        (col("t.DONE_MONEY_NEW") / 10000).alias("DONE_MONEY_NEW"),
        when(
            (col("t.SUM_DONE_MONEY") != 0),
            col("t.DONE_MONEY_NEW") / col("t.SUM_DONE_MONEY") * 100
        ).otherwise(lit(0)).alias("DONE_MONEY_NEW_PROP")
    )

    # 更新数据
    df_m = update_dataframe(
        df_to_update=df_m,
        df_use_me=df_y,
        join_columns=["RESPONS_LINE_ID"],
        update_columns=[
            "DONE_AMOUNT_STOCK",
            "DONE_AMOUNT_STOCK_PROP",
            "DONE_AMOUNT_NEW",
            "DONE_AMOUNT_NEW_PROP",
            "DONE_MONEY_STOCK",
            "DONE_MONEY_STOCK_PROP",
            "DONE_MONEY_NEW",
            "DONE_MONEY_NEW_PROP"
        ]
    )

    # 写回hive,再重读分区
    return_to_hive(
        spark=spark,
        df_result=df_m,
        target_table="ddw.T_COCKPIT_BUSI_ANAL_RESPONS_M",
        insert_mode="overwrite"
    )

    df_m = spark.table("ddw.T_COCKPIT_BUSI_ANAL_RESPONS_M").filter(
        col("BUSI_MONTH") == i_month_id
    )

    """
    市场成交量，成交额
    取中期协月度交易数据
    """
    logger.info(to_color_str("市场成交量,成交额", "blue"))

    (
        v_total_done_amount,
        v_total_done_money
    ) = spark.table("ddw.T_COCKPIT_INDUSTRY_TRAD").alias("t") \
        .filter(
        col("t.etl_month") == i_month_id
    ).agg(
        coalesce(sum(col("t.trad_num")), lit(0)) * 2,
        coalesce(sum(col("t.trad_amt")), lit(0)) * 2 * 100000000
    ).collect()[0]

    """
    市场客户权益-日均
    """

    tmp = spark.table("ddw.T_COCKPIT_INDUSTRY_MANAGE").alias("t") \
        .filter(
        (col("t.etl_month") == i_month_id) &
        (col("t.index_name") == "客户权益")
    ).agg(
        (coalesce(sum(col("t.index_value")), lit(0)) * 100000000).alias("rights")
    ).select(
        col("rights")
    )

    v_total_rights = tmp.alias("t") \
        .select(
        when(
            (lit(lit(v_busi_trade_days)) != 0),
            col("t.rights") / lit(v_busi_trade_days)
        ).otherwise(lit(0)).alias("rights")
    ).first()["rights"]

    """
    市场手续费收入
    """

    v_total_index_value = spark.table("ddw.T_COCKPIT_INDUSTRY_MANAGE").alias("t") \
        .filter(
        (col("t.etl_month") == i_month_id) &
        (col("t.index_name") == "手续费收入")
    ).agg(
        (coalesce(sum(col("t.index_value")), lit(0)) * 100000000).alias("index_value")
    ).select(
        col("index_value")
    ).first()["index_value"]

    """
    --市场地位-日均权益市占率  20240626 
    --市场地位-经纪业务手续费收入市占率 20240626 
    """

    tmp = spark.table("edw.h15_client_sett").alias("t") \
        .filter(
        (col("t.busi_date").between(v_now_begin_date, v_now_end_date))
    ).join(
        other=spark.table("edw.h12_fund_account").alias("b"),
        on=(col("t.fund_account_id") == col("b.fund_account_id")),
        how="left"
    ).join(
        other=spark.table("ddw.T_CTP_BRANCH_OA_RELA").alias("c"),
        on=(col("b.branch_id") == col("c.ctp_branch_id")),
        how="inner"
    ).join(
        other=spark.table("ddw.T_OA_BRANCH").alias("d"),
        on=(col("c.oa_branch_id") == col("d.departmentid")),
        how="inner"
    ).filter(
        col("d.RESPONS_LINE_ID").isNotNull()
    ).groupBy(
        col("t.fund_account_id"),
        col("d.RESPONS_LINE_ID")
    ).agg(
        sum("t.rights").alias("rights"),
        sum(
            coalesce(col("t.transfee"), lit(0)) +
            coalesce(col("t.delivery_transfee"), lit(0)) +
            coalesce(col("t.strikefee"), lit(0))
        ).alias("transfee")
    ).select(
        col("t.fund_account_id"),
        col("d.RESPONS_LINE_ID"),
        col("rights"),
        col("transfee")
    )

    tmp1 = tmp.alias("t") \
        .groupBy(
        col("t.RESPONS_LINE_ID")
    ).agg(
        sum(
            when(
                (lit(v_busi_trade_days) != 0),
                col("t.rights") / lit(v_busi_trade_days)
            ).otherwise(lit(0))
        ).alias("AVG_RIGHTS"),
        sum(col("t.transfee")).alias("TRANSFEE")
    ).select(
        col("t.RESPONS_LINE_ID"),
        col("AVG_RIGHTS"),
        col("TRANSFEE")
    )

    df_y = tmp1.alias("t") \
        .select(
        col("t.RESPONS_LINE_ID"),
        when(
            (lit(v_total_rights) != 0),
            col("t.AVG_RIGHTS") / v_total_rights * 10000
        ).otherwise(lit(0)).alias("AVG_RIGHTS_MARKET_RATE"),
        when(
            (lit(v_total_index_value) != 0),
            col("t.TRANSFEE") / v_total_index_value * 10000
        ).otherwise(lit(0)).alias("FUTU_TRANS_INCOME_MARKET_RATE")
    )

    # 更新数据
    df_m = update_dataframe(
        df_to_update=df_m,
        df_use_me=df_y,
        join_columns=["RESPONS_LINE_ID"],
        update_columns=[
            "AVG_RIGHTS_MARKET_RATE",
            "FUTU_TRANS_INCOME_MARKET_RATE"
        ]
    )

    # 写回hive,再重读分区
    return_to_hive(
        spark=spark,
        df_result=df_m,
        target_table="ddw.T_COCKPIT_BUSI_ANAL_RESPONS_M",
        insert_mode="overwrite"
    )

    df_m = spark.table("ddw.T_COCKPIT_BUSI_ANAL_RESPONS_M").filter(
        col("BUSI_MONTH") == i_month_id
    )

    """
    --市场地位-成交额市场份额占比（市占率） 20240626
    --市场地位-成交量市场份额占比（市占率） 20240626
    """
    logger.info(to_color_str("市场地位 --日均权益，手续费", "blue"))

    tmp = spark.table("edw.h15_hold_balance").alias("t") \
        .filter(
        (col("t.busi_date").between(v_now_begin_date, v_now_end_date))
    ).join(
        other=spark.table("edw.h12_fund_account").alias("b"),
        on=(col("t.fund_account_id") == col("b.fund_account_id")),
        how="left"
    ).join(
        other=spark.table("ddw.T_CTP_BRANCH_OA_RELA").alias("c"),
        on=(col("b.branch_id") == col("c.ctp_branch_id")),
        how="inner"
    ).join(
        other=spark.table("ddw.T_OA_BRANCH").alias("d"),
        on=(col("c.oa_branch_id") == col("d.departmentid")),
        how="inner"
    ).filter(
        col("d.RESPONS_LINE_ID").isNotNull()
    ).groupBy(
        col("t.fund_account_id"),
        col("d.RESPONS_LINE_ID")
    ).agg(
        sum("t.done_amt").alias("done_amount"),
        sum("t.done_sum").alias("done_money")
    ).select(
        col("t.fund_account_id"),
        col("d.RESPONS_LINE_ID"),
        col("done_amount"),
        col("done_money")
    )

    tmp1 = tmp.alias("t") \
        .groupBy(
        col("t.RESPONS_LINE_ID")
    ).agg(
        sum(col("t.done_amount")).alias("done_amount"),
        sum(col("t.done_money")).alias("done_money")
    ).select(
        col("t.RESPONS_LINE_ID"),
        col("done_amount"),
        col("done_money")
    )

    df_y = tmp1.alias("t") \
        .select(
        col("t.RESPONS_LINE_ID"),
        when(
            (lit(v_total_done_amount) != 0),
            col("t.done_amount") / v_total_done_amount * 10000
        ).otherwise(lit(0)).alias("DONE_AMOUNT_MARKET_RATE"),
        when(
            (lit(v_total_done_money) != 0),
            col("t.done_money") / v_total_done_money * 10000
        ).otherwise(lit(0)).alias("DONE_MONEY_MAREKT_RATE")
    )

    # 更新数据
    df_m = update_dataframe(
        df_to_update=df_m,
        df_use_me=df_y,
        join_columns=["RESPONS_LINE_ID"],
        update_columns=[
            "DONE_AMOUNT_MARKET_RATE",
            "DONE_MONEY_MAREKT_RATE"
        ]
    )

    # 写回hive,再重读分区
    df_m = insert_and_reload(df_m)

    logger.info(to_color_str("收入结构其他收入数据更新begin", "blue"))

    """
    --收入结构-主指标-营业收入
    --营业收入=经纪业务收入+其他收入
    --其他收入=交易咨询收入+产品销售收入+场外期权收入+自有资金参与项目收入

    产品销售收入 不统计产品费用分配表
    一、产品销售收入指标为：以下三项之和
    1、资管部“产品费用分配表“字段 综合收益
    2、金融产品中心”收入分配表-FOF产品“字段 经纪业务总收入     CF_BUSIMG.P_COCKPIT_00119_DATA
    3、金融产品中心”收入分配表-普通产品“字段 经纪业务总收入    CF_BUSIMG.P_COCKPIT_00121_DATA

    场外期权收入：场外期权-场外协同清算台账-合计收入/营业收入
    """

    tmp = spark.table("ddw.T_COCKPIT_00126").alias("t") \
        .filter(
        (col("t.done_date").substr(1, 6).between(v_begin_month, i_month_id))
    ).join(
        other=df_oa_branch.alias("d"),
        on=(col("t.oa_branch_id") == col("d.departmentid")),
        how="inner"
    ).groupBy(
        col("d.RESPONS_LINE_ID")
    ).agg(
        (sum(col("t.TOTAL_INCOME")) / 10000).alias("OFF_SITE_INCOME")
    ).select(
        col("d.RESPONS_LINE_ID"),
        col("OFF_SITE_INCOME")
    )

    tmp_product_fof = spark.table("ddw.T_COCKPIT_00118").alias("t") \
        .filter(
        col("t.busi_month").between(v_begin_month, i_month_id)
    ).join(
        other=spark.table("ddw.T_COCKPIT_00117").alias("a"),
        on=(
                (col("t.busi_month") == col("a.busi_month")) &
                (col("t.filing_code") == col("a.filing_code"))
        ),
        how="inner"
    ).join(
        other=df_oa_branch.alias("d"),
        on=(col("a.ALLOCA_OA_BRANCH_ID") == col("d.departmentid")),
        how="inner"
    ).groupBy(
        col("d.RESPONS_LINE_ID")
    ).agg(
        sum(col("t.TOTAL_FUTU_INCOME") * col("a.alloca_reate")).alias("PRODUCT_SELL_INCOME")
    ).select(
        col("d.RESPONS_LINE_ID"),
        col("PRODUCT_SELL_INCOME")
    )

    tmp_product_pt = spark.table("ddw.T_COCKPIT_00120").alias("t") \
        .filter(
        col("t.busi_month").between(v_begin_month, i_month_id)
    ).join(
        other=spark.table("ddw.T_COCKPIT_00117").alias("a"),
        on=(
                (col("t.busi_month") == col("a.busi_month")) &
                (col("t.filing_code") == col("a.filing_code"))
        ),
        how="inner"
    ).join(
        other=df_oa_branch.alias("d"),
        on=(col("a.ALLOCA_OA_BRANCH_ID") == col("d.departmentid")),
        how="inner"
    ).groupBy(
        col("d.RESPONS_LINE_ID")
    ).agg(
        sum(col("t.TOTAL_FUTU_INCOME") * col("a.alloca_reate")).alias("PRODUCT_SELL_INCOME")
    ).select(
        col("d.RESPONS_LINE_ID"),
        col("PRODUCT_SELL_INCOME")
    )

    tmp1 = df_m.alias("t") \
        .join(
        other=tmp.alias("b"),
        on=(col("t.RESPONS_LINE_ID") == col("b.RESPONS_LINE_ID")),
        how="left"
    ).join(
        other=tmp_product_fof.alias("c"),
        on=(col("t.RESPONS_LINE_ID") == col("c.RESPONS_LINE_ID")),
        how="left"
    ).join(
        other=tmp_product_pt.alias("d"),
        on=(col("t.RESPONS_LINE_ID") == col("d.RESPONS_LINE_ID")),
        how="left"
    ).select(
        col("t.RESPONS_LINE_ID"),
        (
                col("t.FUTURE_INCOME") +
                col("t.XT_TRADE_ZX_INCOME") +
                col("t.XT_OWN_FUNDS_INCOME") +
                col("c.PRODUCT_SELL_INCOME") +
                col("d.PRODUCT_SELL_INCOME") +
                col("b.OFF_SITE_INCOME")
        ).alias("OPERAT_INCOME"),
        col("t.FUTURE_INCOME"),
        col("t.XT_TRADE_ZX_INCOME"),
        (
                col("c.PRODUCT_SELL_INCOME") +
                col("d.PRODUCT_SELL_INCOME")
        ).alias("PRODUCT_SELL_INCOME"),
        col("b.OFF_SITE_INCOME"),
        col("t.XT_OWN_FUNDS_INCOME")
    )

    df_y = tmp1.alias("t") \
        .select(
        col("t.RESPONS_LINE_ID"),
        col("t.OPERAT_INCOME"),
        when(
            (col("t.OPERAT_INCOME") != 0),
            col("t.FUTURE_INCOME") / col("t.OPERAT_INCOME") * 100
        ).otherwise(lit(0)).alias("FUTURE_INCOME_PROP"),
        when(
            (col("t.OPERAT_INCOME") != 0),
            col("t.XT_TRADE_ZX_INCOME") / col("t.OPERAT_INCOME") * 100
        ).otherwise(lit(0)).alias("TRADE_ZX_INCOME_PROP"),
        when(
            (col("t.OPERAT_INCOME") != 0),
            col("t.PRODUCT_SELL_INCOME") / col("t.OPERAT_INCOME") * 100
        ).otherwise(lit(0)).alias("PRODUCT_XC_INCOME_PORP"),
        when(
            (col("t.OPERAT_INCOME") != 0),
            col("t.OFF_SITE_INCOME") / col("t.OPERAT_INCOME") * 100
        ).otherwise(lit(0)).alias("OVER_OPTION_INCOME_PROP"),
        when(
            (col("t.OPERAT_INCOME") != 0),
            col("t.XT_OWN_FUNDS_INCOME") / col("t.OPERAT_INCOME") * 100
        ).otherwise(lit(0)).alias("OWN_FUNDS_INCOME_PROP")
    )

    # 更新数据
    df_m = update_dataframe(
        df_to_update=df_m,
        df_use_me=df_y,
        join_columns=["RESPONS_LINE_ID"],
        update_columns=[
            "OPERAT_INCOME",
            "FUTURE_INCOME_PROP",
            "TRADE_ZX_INCOME_PROP",
            "PRODUCT_XC_INCOME_PORP",
            "OVER_OPTION_INCOME_PROP",
            "OWN_FUNDS_INCOME_PROP"
        ]
    )

    # 写回hive,再重读分区
    return_to_hive(
        spark=spark,
        df_result=df_m,
        target_table="ddw.T_COCKPIT_BUSI_ANAL_RESPONS_M",
        insert_mode="overwrite"
    )

    logger.info(to_color_str("收入结构其他收入数据更新end", "blue"))
    logger.info(to_color_str("全部结束", "blue"))

