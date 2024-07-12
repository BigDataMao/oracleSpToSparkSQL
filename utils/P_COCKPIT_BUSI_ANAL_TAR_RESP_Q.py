# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sum, udf, when
from pyspark.sql.types import StringType

from config import Config
from utils.date_utils import get_quarter
from utils.hy_utils.type_map import get_busi_type
from utils.io_utils.common_uitls import to_color_str
from utils.task_env import log, return_to_hive, update_dataframe
from utils.StructTypes.ddw_t_oa_branch import schema as ddw_t_oa_branch_schema
from utils.StructTypes.ddw_t_cockpit_00138 import schema as ddw_t_cockpit_00138_schema
from utils.StructTypes.ddw_t_respons_line import schema as ddw_t_respons_line_schema
from utils.StructTypes.ddw_t_busi_anal_target_type import schema as ddw_t_busi_anal_target_type_schema

logger = Config().get_logger()


@log
def p_cockpit_busi_anal_tar_resp_q(spark, i_month_id):
    """
    经营分析-分管部门-经营目标完成情况-按季度落地
    :param spark: SparkSession对象
    :param i_month_id: 业务日期,格式YYYYMM
    :return: None
    """
    v_busi_year = i_month_id[:4]
    v_BUSI_QUARTER = get_quarter(i_month_id)
    # 注册UDF
    get_busi_type_udf = udf(get_busi_type, StringType())

    def init_data():
        """
        初始化数据
        :return: df_out: DataFrame, 初始化的季度数据,包含所有需要的列
        """
        df_respons_line = spark.table("ddw.t_Respons_Line").where(col("if_use") == '1')
        t = spark.createDataFrame(
            data=df_respons_line.rdd,
            schema=ddw_t_respons_line_schema
        )

        df_target_type = spark.table("ddw.T_BUSI_ANAL_TARGET_TYPE")
        a = spark.createDataFrame(
            data=df_target_type.rdd,
            schema=ddw_t_busi_anal_target_type_schema
        )

        df_out = t.crossJoin(
            other=a
        ).select(
            lit(v_busi_year).alias("BUSI_YEAR"),
            lit(v_BUSI_QUARTER).alias("BUSI_QUARTER"),
            t["RESPONS_LINE_ID"],
            a["busi_type"],
            a["busi_type_name"]
        )

        return_to_hive(
            spark=spark,
            df_result=df_out,
            target_table="ddw.T_COCKPIT_BUSI_ANAL_TAR_RESP_Q",
            insert_mode="overwrite"
        )

        df_out = spark.table("ddw.T_COCKPIT_BUSI_ANAL_TAR_RESP_Q")
        return df_out

    def update_data(df_in):
        """
        更新数据
        :param df_in: 被更新的DataFrame
        :return: df_out: DataFrame, 更新后的季度数据
        """
        df_138 = spark.table("ddw.t_cockpit_00138").where(
            (col("year_id") == lit(v_busi_year)) &
            (col("quarter_id") == lit(v_BUSI_QUARTER))
        )
        t = spark.createDataFrame(
            data=df_138.rdd,
            schema=ddw_t_cockpit_00138_schema
        )
        t = t.withColumn(
            "busi_type",
            get_busi_type_udf(t["index_type"], t["index_asses_benchmark"], t["index_name"])
        )

        df_oa_branch = spark.table("ddw.T_OA_BRANCH").where(col("respons_line_id").isNotNull())
        a = spark.createDataFrame(
            data=df_oa_branch.rdd,
            schema=ddw_t_oa_branch_schema
        )

        tmp = t.join(
            other=a,
            on=t["oa_branch_id"] == a["departmentid"],
            how="inner"
        ).groupBy(
            a["respons_line_id"],
            t["QUARTER_ID"],
            t["busi_type"]
        ).agg(
            sum(t["year_target_value"]).alias("year_target_value"),
            sum(t["complet_value"]).alias("complete_value")
        ).select(
            'respons_line_id',
            'QUARTER_ID',
            'busi_type',
            'year_target_value',
            'complete_value',
        )

        y = tmp.select(
            'respons_line_id',
            'QUARTER_ID',
            'busi_type',
            'complete_value',
            when(
                tmp['year_target_value'] != 0,
                tmp['complete_value'] / tmp['year_target_value'] * 100
            ).otherwise(0).alias('complete_value_rate')
        )

        df_out = update_dataframe(
            df_to_update=df_in,
            df_use_me=y,
            join_columns=["RESPONS_LINE_ID", "BUSI_TYPE"],
            update_columns=[
                "complete_value",
                "complete_value_rate"
            ]
        )

        return df_out

    logger.info(to_color_str('初始化数据', 'blue'))
    df_result = init_data()

    logger.info(to_color_str('更新数据', 'blue'))
    df_result = update_data(df_result)

    logger.info(to_color_str('写回hive表', 'blue'))
    return_to_hive(
        spark=spark,
        df_result=df_result,
        target_table="ddw.T_COCKPIT_BUSI_ANAL_TAR_RESP_Q",
        insert_mode="overwrite"
    )
