# -*- coding: utf-8 -*-
from pyspark.sql.functions import col, lit, when, sum
from utils.date_utils import *
from utils.task_env import *
from utils.StructTypes.ddw_t_cockpit_00138 import schema as ddw_t_cockpit_00138_schema
from utils.StructTypes.ddw_t_oa_branch import schema as ddw_t_oa_branch_schema

logger = Config().get_logger()


@log
def p_cockpit_busi_anal_tar_line_q(spark, busi_date):
    """
    经营分析-业务单位-考核指标落地
    :param spark: SparkSession对象
    :param busi_date: 业务日期,格式为"YYYYMMDD"
    :return: None
    """
    logger.info("p_cockpit_busi_anal_tar_line_q执行开始")

    v_busi_year = busi_date[:4]
    v_BUSI_QUARTER = get_quarter(busi_date)

    """
        考核指标：
    001：考核收入
    002：经纪业务手续费收入市占率
    003：考核日均权益
    004：日均权益市占率
    005：考核利润
    006：成交额
    007：成交额市占率
    008：成交量
    009：成交量市占率
    010：新增直接开发有效客户数量
    011：新增有效客户数量
    012：产品销售额
    """

    logger.info(to_color_str('更新考核指标落地表', 'blue'))

    df_result = spark.table("ddw.T_business_line").alias("t") \
        .filter(
        col("t.if_use") == "1"
    ).crossJoin(
        other=spark.table("ddw.T_BUSI_ANAL_TARGET_TYPE").alias("a")
    ).select(
        lit(v_busi_year).alias("BUSI_YEAR"),
        lit(v_BUSI_QUARTER).alias("BUSI_QUARTER"),
        col("t.business_line_id"),
        col("a.busi_type"),
        col("a.busi_type_name")
    )

    return_to_hive(
        spark=spark,
        df_result=df_result,
        target_table="ddw.T_COCKPIT_BUSI_ANAL_TAR_LINE_Q",
        insert_mode="overwrite"
    )

    df_result = spark.table("ddw.T_COCKPIT_BUSI_ANAL_TAR_LINE_Q").filter(
        (col("BUSI_YEAR") == v_busi_year) &
        (col("BUSI_QUARTER") == v_BUSI_QUARTER)
    )

    logger.info(to_color_str('更新各个指标的数据', 'blue'))

    df_138_yq = spark.table('ddw.t_cockpit_00138').where(
        (col('year_id') == lit(v_busi_year)) &
        (col('quarter_id') == lit(v_BUSI_QUARTER))
    )
    df_138_yq = spark.createDataFrame(
        data=df_138_yq.rdd,
        schema=ddw_t_cockpit_00138_schema
    )

    df_oa_branch = spark.table('ddw.t_oa_branch').where(col('business_line_id').isNotNull())
    df_oa_branch = spark.createDataFrame(
        data=df_oa_branch.rdd,
        schema=ddw_t_oa_branch_schema
    )

    tmp = df_138_yq.join(
        other=df_oa_branch,
        on=(df_138_yq['oa_branch_id'] == df_oa_branch['departmentid']),
        how='inner'
    ).groupBy(
        df_oa_branch['business_line_id'],
        df_138_yq['quarter_id'],
        when(
            (df_138_yq['index_type'] == '0') & (df_138_yq['index_asses_benchmark'] == '1'),
            '001'
        ).when(
            (df_138_yq['index_type'] == '1') & (df_138_yq['index_asses_benchmark'] == '1'),
            '002'
        ).when(
            (df_138_yq['index_type'] == '0') & (df_138_yq['index_asses_benchmark'] == '2'),
            '003'
        ).when(
            (df_138_yq['index_type'] == '1') & (df_138_yq['index_asses_benchmark'] == '2'),
            '004'
        ).when(
            (df_138_yq['index_type'] == '0') & (df_138_yq['index_asses_benchmark'] == '3'),
            '005'
        ).when(
            (df_138_yq['index_type'] == '0') & (df_138_yq['index_asses_benchmark'] == '4'),
            '006'
        ).when(
            (df_138_yq['index_type'] == '1') & (df_138_yq['index_asses_benchmark'] == '4'),
            '007'
        ).when(
            (df_138_yq['index_type'] == '0') & (df_138_yq['index_asses_benchmark'] == '5'),
            '008'
        ).when(
            (df_138_yq['index_type'] == '1') & (df_138_yq['index_asses_benchmark'] == '5'),
            '009'
        ).when(
            (df_138_yq['index_type'] == '0') & (df_138_yq['index_asses_benchmark'] == '6') &
            (df_138_yq['index_name'].like('%新增直接开发有效客户数量%')),
            '010'
        ).when(
            (df_138_yq['index_type'] == '0') & (df_138_yq['index_asses_benchmark'] == '6') &
            (df_138_yq['index_name'].like('%新增有效客户数量%')),
            '011'
        ).when(
            (df_138_yq['index_type'] == '0') & (df_138_yq['index_asses_benchmark'] == '7'),
            '012'
        ).otherwise('').alias('busi_type'),
    ).agg(
        sum(df_138_yq['year_target_value']).alias('year_target_value'),
        sum(df_138_yq['complet_value']).alias('complete_value')
    ).select(
        df_oa_branch['business_line_id'],
        df_138_yq['quarter_id'],
        'busi_type',
        'year_target_value',
        'complete_value',
    )

    df_y = tmp.select(
        tmp['business_line_id'],
        tmp['quarter_id'],
        tmp['busi_type'],
        tmp['complete_value'],
        when(
            tmp['year_target_value'] != 0,
            tmp['complete_value'] / tmp['year_target_value'] * 100
        ).otherwise(0).alias('complete_value_rate')
    )

    df_result_y = update_dataframe(
        df_to_update=df_result,
        df_use_me=df_y,
        join_columns=['business_line_id', 'busi_type'],
        update_columns=['complete_value', 'complete_value_rate']
    )

    return_to_hive(
        spark=spark,
        df_result=df_result_y,
        target_table='ddw.t_cockpit_busi_anal_tar_line_q',
        insert_mode='overwrite'
    )

