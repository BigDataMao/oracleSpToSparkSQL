# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, coalesce


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


def return_to_hive(spark, df_result, target_table, insert_mode, partition_column=None, partition_value=None):
    """
    用于将数据返回hive或hive分区表
    :return: none
    """
    # 判断是否覆盖写
    if_overwrite = insert_mode == "overwrite"

    # 获取目标表的元数据信息
    target_columns = [c.name for c in spark.table(target_table).schema]

    # 添加缺失的列并设置默认值
    for c in target_columns:
        if c not in df_result.columns:
            df_result = df_result.withColumn(c, lit(None))

    # 如果是分区表，添加分区列和值
    if partition_column is not None and partition_value is not None:
        df_result = df_result.withColumn(partition_column, lit(partition_value))

    """
    以下代码用于处理df_result中的列数量比Hive表中的列多的情况
    """
    # 获取Hive表的列名
    hive_table_columns = spark \
        .sql("DESCRIBE {}".format(target_table)) \
        .select("col_name") \
        .rdd \
        .map(lambda r: r[0]) \
        .collect()
    # 选择与Hive表列名匹配的列
    selected_columns = list(filter(lambda column: column in hive_table_columns, df_result.columns))
    df_result = df_result.select(selected_columns)

    # 插入数据
    df_result.select(target_columns).write.insertInto(target_table, overwrite=if_overwrite)


def update_dataframe(df1, df2, join_condition, update_columns: list):

    df_result = df1.join(df2, join_condition, "left")

    # Iterate over update_columns and apply the updates
    for column in update_columns:
        new_col_name = column + "_new"
        df_result = df_result.withColumn(new_col_name, coalesce(df2[column], df1[column]))
        df_result = df_result.drop(column)
        df_result = df_result.withColumnRenamed(new_col_name, column)

    return df_result
