# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, when, sum, udf
from pyspark.sql.types import StringType

from config import Config
from utils.date_utils import get_quarter
from utils.hy_utils.type_map import get_busi_type
from utils.task_env import return_to_hive, update_dataframe, log
from utils.StructTypes.ddw_t_oa_branch import schema as ddw_t_oa_branch_schema
from utils.StructTypes.ddw_t_busi_anal_target_type import schema as ddw_t_busi_anal_target_type_schema
from utils.StructTypes.ddw_t_cockpit_00138 import schema as ddw_t_cockpit_00138_schema
from utils.StructTypes.ddw_t_cockpit_busi_anal_target_q import schema as ddw_t_cockpit_busi_anal_target_q_schema

logger = Config().get_logger()


@log
def p_cock_busi_anal_target_q_data(spark: SparkSession, busi_date):
    """
    经营分析-业务单位-经营目标完成情况-按季度
    :param spark: SparkSession对象
    :param busi_date: 业务日期,格式YYYYMMDD
    :return: None
    """

    v_busi_year = busi_date[:4]
    v_busi_quarter = get_quarter(busi_date)
    # spark = SparkSession.builder.appName("test").getOrCreate()
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

    df_oa_branch = spark.table('ddw.t_oa_branch')
    df_oa_branch = spark.createDataFrame(
        data=df_oa_branch.rdd,
        schema=ddw_t_oa_branch_schema
    )

    df_busi_anal_target_type = spark.table('ddw.t_busi_anal_target_type')
    df_busi_anal_target_type = spark.createDataFrame(
        data=df_busi_anal_target_type.rdd,
        schema=ddw_t_busi_anal_target_type_schema
    )

    df_138_yq = spark.table('ddw.t_cockpit_00138').filter(
        (col('year_id') == lit(v_busi_year)) &
        (col('quarter_id') == lit(v_busi_quarter))
    )
    df_138_yq = spark.createDataFrame(
        data=df_138_yq.rdd,
        schema=ddw_t_cockpit_00138_schema
    )

    @log
    def init_data():
        """
        初始化数据
        :return: df
        """
        df = df_oa_branch.filter(
            df_oa_branch['canceled'].isNull()
        ).crossJoin(
            other=df_busi_anal_target_type
        ).select(
            lit(v_busi_year).alias('BUSI_YEAR'),
            lit(v_busi_quarter).alias('BUSI_QUARTER'),
            df_oa_branch['departmentid'].alias('OA_BRANCH_ID'),
            df_busi_anal_target_type['busi_type'],
            df_busi_anal_target_type['busi_type_name']
        )

        return_to_hive(
            spark=spark,
            df_result=df,
            target_table="ddw.T_COCKPIT_BUSI_ANAL_TARGET_Q",
            insert_mode="overwrite",
        )

        df = spark.table('ddw.T_COCKPIT_BUSI_ANAL_TARGET_Q')
        df = spark.createDataFrame(
            data=df.rdd,
            schema=ddw_t_cockpit_busi_anal_target_q_schema
        )

        return df

    @log
    def update_data(df):
        """
        更新数据
        :return: None
        """
        # 注册UDF
        get_busi_type_udf = udf(get_busi_type, StringType())
        tmp = df_138_yq.groupBy(
            df_138_yq['oa_branch_id'],
            df_138_yq['quarter_id'],
            get_busi_type_udf(
                df_138_yq['index_type'],
                df_138_yq['index_asses_benchmark'],
                df_138_yq['index_name']
            ).alias('busi_type'),
        ).agg(
            sum(df_138_yq['year_target_value']).alias('year_target_value'),
            sum(df_138_yq['complet_value']).alias('complete_value')
        ).select(
            df_138_yq['oa_branch_id'],
            df_138_yq['quarter_id'],
            'busi_type',
            'year_target_value',
            'complete_value'
        )

        df_y = tmp.select(
            tmp['oa_branch_id'],
            tmp['quarter_id'],
            tmp['busi_type'],
            tmp['complete_value'],
            when(
                tmp['year_target_value'] != 0,
                tmp['complete_value'] / tmp['year_target_value'] * 100
            ).otherwise(0).alias('complete_value_rate')
        )

        df = update_dataframe(
            df_to_update=df,
            df_use_me=df_y,
            join_columns=['oa_branch_id', 'busi_type'],
            update_columns=['complete_value', 'complete_value_rate']
        )

        return df

    df_yq = init_data()
    df_yq = update_data(df_yq)

    return_to_hive(
        spark=spark,
        df_result=df_yq,
        target_table="ddw.T_COCKPIT_BUSI_ANAL_TARGET_Q",
        insert_mode="overwrite",
    )

    logger.info("p_cock_busi_anal_target_q_data执行完成")
    logger.info("本次任务为:经营分析-业务单位-经营目标完成情况-按季度")
