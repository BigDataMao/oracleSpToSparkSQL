# -*- coding: utf-8 -*-
import os
import re
import cx_Oracle

# 存储参数的数组
hive_tables = []
# 用户提供的数据库名列表
users = ["CTP63", "CF_STAT", "FUTURES"]

# 遍历当前目录下以 P_COCKPIT 开头的文件
for filename in os.listdir('.'):
    if filename.startswith('P_COCKPIT'):
        with open(filename, 'r', encoding='utf-8') as file:
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
print("\n" * 3)

# 获取oracle_tables
oracle_tables = []
for param in hive_tables:
    # 规则1: 将以 ddw 开头的表名替换为 cf_busimg 开头
    if param.lower().startswith("ddw"):
        param = "cf_busimg" + param[3:]
    # # 规则2: 去掉 "ods." 前缀，并将第一个下划线 "_" 改成点号 "."
    # if param.lower().startswith("ods."):
    #     param = param[4:].replace("_", ".", 1)
    # oracle_tables.append(param)

    # 规则2: 去掉 "ods." 前缀，并将第一个下划线 "_" 改成点号 "."
    if param.lower().startswith("ods."):
        table_name = param[4:]
        for user in users:
            if table_name.lower().startswith(user.lower()):
                table_name = user + "." + table_name[len(user)+1:]
                break
        param = table_name
    oracle_tables.append(param)

# 打印结果
print("下面是oracle表\n")
for param in oracle_tables:
    print(param)
print("\n" * 3)


# Oracle 连接信息
username = 'wolf'
password = 'wolf'
dsn = 'txy:1136/wolf'  # 格式如: host:port/service_name

# 连接到 Oracle 数据库
connection = cx_Oracle.connect(username, password, dsn)

# 创建游标
cursor = connection.cursor()

# 打印每个表的建表语句
for table_name in oracle_tables:
    # 执行 SQL 查询以获取表的建表语句
    cursor.execute("SELECT dbms_metadata.get_ddl('TABLE', :table_name) FROM dual", {'table_name': table_name})
    # 获取查询结果
    result = cursor.fetchone()
    # 打印建表语句
    print("建表语句 for {table_name}:")
    print(result[0])

# 关闭游标和连接
cursor.close()
connection.close()
