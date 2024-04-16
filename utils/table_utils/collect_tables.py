# -*- coding: utf-8 -*-
import json
import logging
import os
import re

import oracledb

from utils.table_utils.oracle_table_to_hive import oracle_ddl_to_hive

# Oracle 连接信息
username = 'wolf'
password = 'wolf'
dsn = '192.168.25.19:1521/WOLF'
oracledb.init_oracle_client(lib_dir=r'/usr/lib/oracle/11.2/client64/lib')  # 针对oracle11.2的老版本需要采用这种厚模式
# 存储所有的表相关信息
table_info = {
    # "hive_table_name": {
    #     "oracle_db": "oracle_db_name",
    #     "oracle_table": "oracle_table_name",
    #     "oracle_full_name": "oracle_db_name.oracle_table_name"
    #     "oracle_ddl": "oracle_ddl"
    # }
}
# 脚本目录
script_dir = os.path.dirname(os.path.abspath(__file__))
logging.info("脚本目录: %s", script_dir)
# 上级目录
cockpit_dir = os.path.dirname(script_dir)
logging.info("上级目录: %s", cockpit_dir)


def collect_tables():
    # 存储参数的数组
    hive_tables = []
    # 用户提供的数据库名列表
    oracle_users = ["CTP63", "CF_STAT", "FUTURES"]
    # 遍历当前目录下以 P_COCKPIT 开头的文件
    for filename in os.listdir(cockpit_dir):
        if filename.startswith('P_COCKPIT_00092'):
            with open(cockpit_dir + "/" + filename, 'r', encoding='utf-8') as file:
                content = file.read()
                # 使用正则表达式找到 spark.table() 方法内部的参数
                spark_table_params = re.findall(r'spark\.table\(["\']([^"\']*)["\']\)', content)
                # 将找到的参数添加到数组中
                hive_tables.extend(spark_table_params)

                # 使用正则表达式找到 return_to_hive 方法内 target_table 参数的值
                return_to_hive_params = re.findall(r'return_to_hive\(.*?target_table=["\']([^"\']*)["\']', content)
                # 将找到的参数添加到数组中
                hive_tables.extend(return_to_hive_params)

    # 去重
    hive_tables = list(set(hive_tables))

    # 排除以 "edw" 开头的表
    hive_tables = [table for table in hive_tables if not table.lower().startswith("edw")]

    # 打印结果
    print("下面是hive表\n")
    for param in hive_tables:
        print(param)
        table_info[param] = {}
    print("\n" * 3)

    # 获取oracle_tables
    oracle_tables = []
    for param in hive_tables:
        table = None
        # 规则1: 将以 ddw 开头的表名替换为 cf_busimg 开头
        if param.lower().startswith("ddw"):
            table_name = param[4:]
            full_name = "cf_busimg" + param[3:]
            table = ("CF_BUSIMG", table_name, full_name)
            table_info[param]["oracle_db"] = "CF_BUSIMG"
            table_info[param]["oracle_table"] = table_name
            table_info[param]["oracle_full_name"] = full_name

        # 规则2: 去掉 "ods." 前缀，并将第一个下划线 "_" 改成点号 "."
        if param.lower().startswith("ods."):
            tmp_name = param[4:]
            db_name = None
            table_name = None
            full_name = None
            for user in oracle_users:
                if tmp_name.lower().startswith(user.lower()):
                    db_name = user
                    table_name = tmp_name[len(user) + 1:]
                    full_name = user + "." + table_name
                    break
            table = (db_name, table_name, full_name)
            table_info[param]["oracle_db"] = db_name
            table_info[param]["oracle_table"] = table_name
            table_info[param]["oracle_full_name"] = full_name
        oracle_tables.append(table)

    # 打印结果
    print("下面是oracle表\n")
    for param in oracle_tables:
        print(param)
    print("\n" * 3)

    json_str = json.dumps(table_info, indent=4)

    print(json_str)

    # 用oracledb的thick模式
    pool = oracledb.create_pool(
        user=username,
        password=password,
        dsn=dsn,
        min=1,
        max=5,
        increment=1,
    )

    oracle_pool = pool.acquire()
    with oracle_pool.cursor() as cursor:
        # 打印每个表的建表语句
        for table in table_info.values():
            try:
                # 执行 SQL 查询以获取表的建表语句 (DDL)
                cursor.execute(
                    "SELECT DBMS_METADATA.get_ddl('TABLE', :table_name, :db_name) FROM DUAL",
                    {'table_name': table["oracle_table"].upper(), 'db_name': table["oracle_db"].upper()}
                )
                # 获取查询结果
                result = cursor.fetchone()

                # 将 cx_Oracle.LOB 对象转换为字符串
                oracle_ddl = result[0].read()
                print(oracle_ddl)
                table["oracle_ddl"] = oracle_ddl

            except oracledb.DatabaseError as e:
                error, = e.args
                # 如果是ORA-31603错误，表示对象不存在，继续处理下一个表
                if error.code == 31603:
                    print("表 %s 不存在" % table["oracle_full_name"])
                    table["oracle_ddl"] = None
                    continue
                else:
                    # 其他错误，打印错误信息
                    print("处理表 %s 时发生错误:" % error.message)
                    table["oracle_ddl"] = None
                    continue

            except Exception as ex:
                # 捕获所有其他异常类型
                print("处理表时发生错误:", ex)
                table["oracle_ddl"] = None
                continue

    # 关闭游标和连接
    pool.release(oracle_pool)
    pool.close()

    json_str = json.dumps(table_info, indent=4)
    print(json_str)
    return table_info


if __name__ == '__main__':
    tmp_ddl = ""
    table_info = collect_tables()
    for key, value in table_info.items():
        arr_1 = oracle_ddl_to_hive(value["oracle_ddl"])
        if arr_1:
            for name, name_type in arr_1:
                if tmp_ddl == "":
                    tmp_ddl = name + "\t" + name_type
                else:
                    tmp_ddl = tmp_ddl + ",\n" + name + "\t" + name_type
            hive_ddl = \
                f"""create table {key}(\n{tmp_ddl});
                """
            print(hive_ddl)
