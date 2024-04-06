# -*- coding: utf-8 -*-

"""
将oracle的建表语句转化为hive的建表语句
"""

import re


def oracle_table_to_hive(oracle_table_ddl):
    """
    将oracle的建表语句转化为hive的建表语句
    :param oracle_table_ddl: str, oracle的建表语句
    :return: str, hive的建表语句
    """
    # 将建表语句中的所有换行符替换为空格
    oracle_table_ddl = re.sub(r"\n", " ", oracle_table_ddl)
    # 将建表语句中的所有制表符替换为空格
    oracle_table_ddl = re.sub(r"\t", " ", oracle_table_ddl)
    # 将建表语句中的所有多个空格替换为一个空格
    oracle_table_ddl = re.sub(r"\s+", " ", oracle_table_ddl)

    # 从建表语句中找到表名
    table_name = re.search(r"CREATE TABLE ([^ ]*) ", oracle_table_ddl).group(1)

    # 从建表语句中找到字段定义部分
    columns = re.search(r"\((.*)\)", oracle_table_ddl).group(1)

    # 将字段定义部分按逗号分割
    columns = columns.split(",")

    # 将字段定义部分的每个字段转化为hive的字段定义
    hive_columns = []
    for column in columns:
        # 将字段定义部分的每个字段按空格分割
        column = column.strip().split(" ")
        # 将字段定义部分的每个字段转化为hive的字段定义
        hive_column = " ".join(column)
        hive_columns.append(hive_column)

    # 将hive的字段定义部分用逗号连接
    hive_columns = ", ".join(hive_columns)

    # 将表名和hive的字段定义部分拼接为hive的建表语句
    hive_table_ddl = f"CREATE TABLE {table_name} ({hive_columns})"

    return hive_table_ddl