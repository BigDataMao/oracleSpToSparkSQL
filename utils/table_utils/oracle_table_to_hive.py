# -*- coding: utf-8 -*-

"""
将oracle的建表语句转化为hive的建表语句
"""

import re

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

    if oracle_ddl is None:
        return None

    # 获取包含字段信息的部分前后的索引
    left_index, right_index = find_matching_parentheses(oracle_ddl)
    # 获取字段信息字符串
    str_ddl = oracle_ddl[left_index + 1:right_index]
    # 切割一行一行的字段信息
    row_ddl = [s.strip() for s in str_ddl.split(",")]
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


if __name__ == '__main__':
    oracle_ddl = """
    create table CF_BUSIMG.T_COCKPIT_BUSI_ANAL_TAR_RESP_Q
    (
        "busi_year"           VARCHAR2(4),
        busi_quarter        VARCHAR2(2),
        respons_line_id     VARCHAR2(20),
        busi_type           VARCHAR2(10),
        busi_type_name      VARCHAR2(100),
        complete_value      NUMBER default 0,
        complete_value_rate NUMBER default 0
    )
    tablespace TS_CF_BUSIMG
        pctfree 10
        initrans 1
        maxtrans 255
        storage
        (
            initial 64K
            next 1M
            minextents 1
            maxextents unlimited
        );
    """

    arr_1 = oracle_ddl_to_hive(oracle_ddl=oracle_ddl)
    print(arr_1)
