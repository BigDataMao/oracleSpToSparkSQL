# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf, sum, when
from pyspark.sql.types import StringType

from config import Config
from utils.hy_utils.type_map import get_busi_type
from utils.io_utils.common_uitls import to_color_str
from utils.task_env import return_to_hive, update_dataframe, log
from utils.StructTypes.ddw_t_oa_branch import schema as ddw_t_oa_branch_schema
from utils.StructTypes.ddw_t_busi_anal_target_type import schema as ddw_t_busi_anal_target_type_schema
from utils.StructTypes.ddw_t_cockpit_00138 import schema as ddw_t_cockpit_00138_schema

logger = Config().get_logger()


@log
def p_cock_busi_anal_target_y_data(spark, i_month_id):
    v_busi_year = i_month_id[:4]

    # 注册UDF
    get_busi_type_udf = udf(get_busi_type, StringType())

    def init_data():
        """
        初始化数据
        :return: df_out: DataFrame, 初始化的季度数据,包含所有需要的列
        """
        df_oa_branch = spark.table("ddw.t_oa_branch").where(col('canceled').isNull())
        t = spark.createDataFrame(
            data=df_oa_branch.rdd,
            schema=ddw_t_oa_branch_schema
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
            t["RESPONS_LINE_ID"],
            a["busi_type"],
            a["busi_type_name"]
        )

        return_to_hive(
            spark=spark,
            df_result=df_out,
            target_table="ddw.t_cockpit_busi_anal_target_y",
            insert_mode="overwrite"
        )

        df_out = spark.table("ddw.t_cockpit_busi_anal_target_y")
        return df_out

    def update_data(df_in):
        """
        更新数据
        :param df_in: 被更新的DataFrame
        :return: df_out: DataFrame, 更新后的年数据
        """
        df_138 = spark.table("ddw.t_cockpit_00138").where(
            (col("year_id") == lit(v_busi_year))
        )
        t = spark.createDataFrame(
            data=df_138.rdd,
            schema=ddw_t_cockpit_00138_schema
        )
        t = t.withColumn(
            "busi_type",
            get_busi_type_udf(t["index_type"], t["index_asses_benchmark"], t["index_name"])
        )

        tmp = t.groupBy(
            t["oa_branch_id"],
            t["busi_type"]
        ).agg(
            sum(t["year_target_value"]).alias("year_target_value"),
            sum(t["complet_value"]).alias("complete_value")
        ).select(
            "oa_branch_id",
            'busi_type',
            'year_target_value',
            'complete_value',
        )

        y = tmp.select(
            'oa_branch_id',
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
            join_columns=["oa_branch_id", "busi_type"],
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
        target_table="ddw.T_COCKPIT_BUSI_ANAL_TARGET_Y",
        insert_mode="overwrite"
    )
