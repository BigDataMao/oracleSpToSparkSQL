# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit


# spark入口
def create_env():
    spark = SparkSession.builder \
        .appName("HiveTest") \
        .master("local") \
        .config("spark.sql.warehouse.dir", "hdfs://cdh-master:8020/user/hive/warehouse") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://cdh-master:9083") \
        .config("spark.hadoop.hive.exec.scratchdir", "/user/hive/tmp") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def is_overwrite(insert_mode):
    if insert_mode == "overwrite":
        return True
    else:
        return False


def return_to_hive(spark, df_result, target_table, insert_mode):
    """
    用于将数据返回hive
    :return: none
    """

    if_overwrite = is_overwrite(insert_mode)

    # 获取目标表的元数据信息
    target_columns = [c.name for c in spark.table(target_table).schema]

    # 添加缺失的列并设置默认值
    for c in target_columns:
        if c not in df_result.columns:
            df_result = df_result.withColumn(c, lit(None))

    # 插入数据
    df_result.select(target_columns).write.insertInto(target_table, overwrite=if_overwrite)


def return_to_hive_partitioned(spark, df_result, target_table, partition_column, partition_value, insert_mode):
    """
    用于将数据返回hive分区表
    :return: none
    """

    if_overwrite = is_overwrite(insert_mode)

    # 获取目标表的元数据信息
    target_columns = [c.name for c in spark.table(target_table).schema]

    # 添加缺失的列并设置默认值
    for c in target_columns:
        if c not in df_result.columns:
            df_result = df_result.withColumn(c, lit(None))

    # 添加分区列和值
    df_result = df_result.withColumn(partition_column, lit(partition_value))

    # 插入数据
    df_result.select(target_columns).write.insertInto(target_table, overwrite=if_overwrite)
