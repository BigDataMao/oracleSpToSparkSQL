# -*- coding: utf-8 -*-
import logging
import os
from os import path

from pyhive import hive

from utils.io_utils.clean_table_info import clean_table_info
from utils.io_utils.csv_utils import csv_to_dict_list
from utils.table_utils.get_oracle_ddl import get_oracle_ddl
from utils.table_utils.oracle_ddl_to_hive_ddl import oracle_ddl_to_hive, generate_hive_ddl

is_drop = True

# 当前目录
current_dir = os.path.dirname(os.path.abspath(__file__))
# 项目目录
project_dir = current_dir
# table_info.csv 文件路径
table_info_file = path.join(project_dir, 'input/table_info.csv')

# 从csv文件中读取表信息
data = csv_to_dict_list(table_info_file)
# 清洗表信息
table_info = clean_table_info(data)
# 获取oracle_ddl
table_info_clean = get_oracle_ddl(table_info)

# i = 1
# for item in table_info_clean:
#     # 获取字段名和字段类型
#     tuple2 = oracle_ddl_to_hive(item.get('oracle_ddl'))
#     # 生成hive的ddl语句
#     hive_ddl = generate_hive_ddl(item, tuple2)
#
#     print('\n\n' + str(i) + '\n\n')
#     print(hive_ddl)
#
#     i += 1


conn = hive.Connection(
    # host='cdh-master',
    host='192.168.25.10',
    port=10000,
    username='root'
)
for item in table_info_clean:
    tuple2 = oracle_ddl_to_hive(item.get('oracle_ddl'))
    hive_ddl = generate_hive_ddl(item, tuple2)
    print(hive_ddl)

    curser = conn.cursor()

    if is_drop:
        query = "DROP TABLE IF EXISTS %s" % item.get('hive_table_fullname')
        curser.execute(query)
        logging.info("Table %s dropped successfully" % item.get('hive_table_fullname'))

    query = hive_ddl
    curser.execute(query)
    logging.info("Table %s created successfully" % item.get('hive_table_fullname'))

    curser.close()

conn.close()
