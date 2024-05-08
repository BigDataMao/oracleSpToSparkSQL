# -*- coding: utf-8 -*-

"""
将oracle的建表语句转化为hive的建表语句
"""

import re
from typing import List, Tuple

"""
数据类型映射关系如下：
| 数据类型    | Oracle             | Hive          |
| ---------- | ------------------ | ------------- |
| 字符串      | CHAR               | STRING        |
| 字符串      | VARCHAR            | STRING        |
| 字符串      | VARCHAR2           | STRING        |
| 字符串      | VARCHAR(n)         | STRING        |
| 字符串      | VARCHAR2(n)        | STRING        |
| 文本        | CLOB               | STRING        |
| 字符串      | DATE               | STRING        |
| 数值        | NUMBER             | DOUBLE        |
| 数值        | NUMBER(p,s)        | DOUBLE        |
| 数值        | INTEGER            | DOUBLE        |
"""


def find_matching_parentheses(s):
    """
    寻找第一个左括号和其匹配的右括号
    :param s: 输入字符串
    :return: 左右括号的索引值
    """

    if '(' not in s or ')' not in s:
        print("没有括号")
        return -1, -1

    stack = []
    for i, char in enumerate(s):
        if char == '(':
            stack.append(i)
        elif char == ')':
            if not stack:
                return -1, -1  # 没有匹配的左括号
            elif len(stack) == 1:
                return stack.pop(), i
            else:
                stack.pop()
    if stack:
        return stack.pop(), -1  # 没有匹配的右括号
    return -1, -1  # 没有括号


def oracle_ddl_to_hive(oracle_ddl):
    """
    处理oracle的ddl语句,提取字段名和字段类型,并将字段类型映射为hive的数据类型
    :param oracle_ddl: oracle的ddl语句
    :return pure_ddl: 映射为hive的数据类型,返回一个二维数组
    """

    if oracle_ddl is None:
        return None

    # 获取包含字段信息的部分前后的索引
    left_index, right_index = find_matching_parentheses(oracle_ddl)
    # 获取字段信息字符串
    str_ddl = oracle_ddl[left_index + 1:right_index]
    # 删除CONSTRAINT约束及后面的内容
    str_ddl = re.sub(r",\s*CONSTRAINT.*", '', str_ddl)
    # 删除PRIMARY KEY约束及后面的内容
    str_ddl = re.sub(r",\s*PRIMARY.*", '', str_ddl)
    # 切割一行一行的字段信息
    row_ddl = [s.strip() for s in str_ddl.split(", ")]  # 注意这里的空格不能去掉,否则类似"NUMBER(10,2)"会被切割成"NUMBER(10"和"2)"
    # 去除多余的空格
    row_ddl = [re.sub(r"\s+", " ", s) for s in row_ddl]
    # 每行字段信息切割
    row_arr_ddl = [re.split(r"\s", s) for s in row_ddl]
    # 获取字段的时候,仅保留字段名和字段类型,舍去NOT NULL DEFAULT等
    pure_ddl = [s[:2] for s in row_arr_ddl]
    # 将数据类型后面带的括号去掉,例如varchar2(200)改成varchar2
    for s in pure_ddl:
        s[1] = re.sub(r"\(.*?\)", '', s[1])
    # 直接从oracle数据库读取的字段名会带双引号,需要去掉
    for s in pure_ddl:
        s[0] = s[0].replace('"', '')

    # 构建映射关系
    type_mapping = {
        "CHAR": "STRING",
        "VARCHAR": "STRING",
        "VARCHAR2": "STRING",
        "DATE": "STRING",
        "NUMBER": "DOUBLE",
        "INTEGER": "DOUBLE"
    }
    # 映射数据类型
    for s in pure_ddl:
        s[1] = type_mapping.get(s[1], s[1])

    return pure_ddl


def generate_hive_ddl(table_info: dict, type_mapping: Tuple[str,str]):
    """
    生成hive的ddl语句
    :param table_info: 字典,至少包含hive_table_fullname,partition_col,is_partition三个键值对
    :param type_mapping: 二维数组,包含字段名和字段类型
    :return: hive_ddl: hive的ddl语句,可以供直接执行
    """
    # 获取type_mapping中第一个元素(字段名)的长度最大值
    max_len = max([len(x[0]) for x in type_mapping])
    adjust_len = max_len + 4
    # 获取分区字段列表
    partition_col_list = table_info.get('partition_col')
    # 生成分区后缀类似于: PARTITIONED BY (dt STRING, country STRING)
    if table_info.get('is_partition'):
        partition_ddl = "PARTITIONED BY (\n"
        for name in partition_col_list:
            partition_ddl += "\t" + name.upper().ljust(adjust_len) + " STRING, \n"
        partition_ddl = partition_ddl[:-3] + ")"
    else:
        partition_ddl = ""
    # 生成字段信息
    tmp_ddl = ""
    for name, name_type in type_mapping:
        if name.lower() not in partition_col_list:
            if tmp_ddl == "":
                tmp_ddl = "\t" + name.ljust(adjust_len) + "\t" + name_type
            else:
                tmp_ddl = tmp_ddl + ",\n" + "\t" + name.ljust(adjust_len) + "\t" + name_type

    hive_ddl = \
        f"""CREATE TABLE IF NOT EXISTS {table_info.get('hive_table_fullname')}(\n{tmp_ddl})\n{partition_ddl}
        """
    return hive_ddl



