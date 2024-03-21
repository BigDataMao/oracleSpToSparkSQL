# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit


# spark入口
def create_env():
    spark = SparkSession.builder \
        .appName("HiveTest") \
        .master("local") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://cdh_master:10000") \
        .config("spark.hadoop.hive.exec.scratchdir", "/user/hive/tmp") \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def return_to_hive(spark, df_result, target_table, insert_mode):
    """
    用于将数据返回hive
    :return: none
    """

    # 获取目标表的元数据信息
    target_columns = [c.name for c in spark.table(target_table).schema]

    # 添加缺失的列并设置默认值
    for c in target_columns:
        if c not in df_result.columns:
            df_result = df_result.withColumn(c, lit(None))

    # 插入数据
    df_result.select(target_columns).write.mode(insert_mode).insertInto(target_table)
