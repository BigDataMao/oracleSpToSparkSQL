# -*- coding: utf-8 -*-
# 本机运行测试任务: https://blog.csdn.net/qq_39950572/article/details/136260712
# pip install pyspark==2.4.4
import codecs
import csv
import json
import os

from os import path

import oracledb

from config import Config
from utils.table_utils.oracle_table_to_hive import oracle_ddl_to_hive

print(os.getcwd())
# 当前目录
current_dir = os.path.dirname(os.path.abspath(__file__))
# 项目目录
project_dir = current_dir
# table_info.csv 文件路径
table_info_file = path.join(project_dir, 'input/table_info.csv')


# 读取 CSV 文件并转换为字典列表
def csv_to_dict_list(file_path):
    """
    读取 CSV 文件并转换为字典列表
    :param file_path: 文件路径
    :return: 字典列表
    """
    data = []
    with codecs.open(file_path, 'r', encoding='utf-8') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        for item in csv_reader:
            data.append(dict(item))
    return data


table_info = csv_to_dict_list(table_info_file)
table_info_clean = []

for item in table_info:
    if not item.get('oracle_table_fullname') or not item.get('hive_table_fullname'):
        continue
    oracle_info = item.get('oracle_table_fullname').strip().split('.')
    oracle_db = oracle_info[0].upper()
    oracle_table = oracle_info[1].upper()
    hive_table_fullname = item.get('hive_table_fullname').strip().lower()
    partition_col = item.get('partition_col').lower().strip().split(',') if item.get('partition_col') else []
    is_partition = True if partition_col else False
    table_info_clean.append({
        'oracle_db': oracle_db,
        'oracle_table': oracle_table,
        'hive_table_fullname': hive_table_fullname,
        'is_partition': is_partition,
        'partition_col': partition_col
    })

# for item in table_info_clean[:4]:
#     print(json.dumps(item, indent=4))

# # 保存清洗后的数据
# table_info_clean_file = path.join(project_dir, 'output/table_info_clean.json')
# with open(table_info_clean_file, 'w', encoding='utf-8') as f:
#     f.write(json.dumps(table_info_clean, indent=4, ensure_ascii=False))

config = Config('config.json')
database = config.get('database')
user = database.get('user')
password = database.get('password')
dsn = database.get('dsn')

oracledb.init_oracle_client(lib_dir=r'/usr/lib/oracle/11.2/client64/lib')

pool = oracledb.create_pool(
        user=user,
        password=password,
        dsn=dsn,
        min=1,
        max=5,
        increment=1,
    )

oracle_pool = pool.acquire()
with oracle_pool.cursor() as cursor:
    # 打印每个表的建表语句
    for index, item in enumerate(table_info_clean):
        try:
            # 执行 SQL 查询以获取表的建表语句 (DDL)
            cursor.execute(
                "SELECT DBMS_METADATA.get_ddl('TABLE', :table_name, :db_name) FROM DUAL",
                {'table_name': item.get('oracle_table'), 'db_name': item.get('oracle_db')}
            )
            # 获取查询结果
            result = cursor.fetchone()

            # 将 cx_Oracle.LOB 对象转换为字符串
            oracle_ddl = result[0].read()
            table_info_clean[index]["oracle_ddl"] = oracle_ddl

        except oracledb.DatabaseError as e:
            error, = e.args
            # 如果是ORA-31603错误，表示对象不存在，继续处理下一个表
            if error.code == 31603:
                print("表 %s 不存在" % (item.get('oracle_table') + '.' + item.get('oracle_db')))
                item["oracle_ddl"] = None
                continue
            else:
                # 其他错误，打印错误信息
                print("处理表 %s 时发生错误:" % error.message)
                item["oracle_ddl"] = None
                continue

        except Exception as ex:
            # 捕获所有其他异常类型
            print("处理表时发生错误:", ex)
            item["oracle_ddl"] = None
            continue

# 关闭游标和连接
pool.release(oracle_pool)
pool.close()

i = 1
for item in table_info_clean:
    tuple2 = oracle_ddl_to_hive(item.get('oracle_ddl'))
    # 获取tuple2中第一个元素的长度最大值
    max_len = max([len(x[0]) for x in tuple2])
    adjust_len = max_len + 4
    # 获取分区字段列表
    partition_col_list = item.get('partition_col')
    # 生产分区后缀类似于: PARTITIONED BY (dt STRING, country STRING)
    if item.get('is_partition'):
        partition_ddl = "PARTITIONED BY (\n"
        for name in partition_col_list:
            partition_ddl += name.ljust(adjust_len) + " STRING, \n"
        partition_ddl = partition_ddl[:-3] + ")"
    else:
        partition_ddl = ""


    tmp_ddl = ""
    for name, name_type in tuple2:
        if name.lower() not in partition_col_list:
            if tmp_ddl == "":
                tmp_ddl = name.ljust(adjust_len) + "\t" + name_type
            else:
                tmp_ddl = tmp_ddl + ",\n" + name.ljust(adjust_len) + "\t" + name_type

    hive_ddl = \
        f"""create table {item.get('hive_table_fullname')}(\n{tmp_ddl})\n{partition_ddl};
        """
    print('\n\n' + str(i) + '\n\n')
    print(hive_ddl)



    i += 1
