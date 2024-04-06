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
    table = None
    # 规则1: 将以 ddw 开头的表名替换为 cf_busimg 开头
    if param.lower().startswith("ddw"):
        table_name = param[4:]
        full_name = "cf_busimg" + param[3:]
        table = ("CF_BUSIMG", table_name, full_name)

    # 规则2: 去掉 "ods." 前缀，并将第一个下划线 "_" 改成点号 "."
    if param.lower().startswith("ods."):
        tmp_name = param[4:]
        db_name = None
        table_name = None
        full_name = None
        for user in users:
            if tmp_name.lower().startswith(user.lower()):
                db_name = user
                table_name = tmp_name[len(user)+1:]
                full_name = user + "." + table_name
                break
        table = (db_name, table_name, full_name)
    oracle_tables.append(table)

# 打印结果
print("下面是oracle表\n")
for param in oracle_tables:
    print(param)
print("\n" * 3)


# Oracle 连接信息
username = 'wolf'
password = 'wolf'
dsn = 'txy:1136/wolfdb'

# 连接到 Oracle 数据库
connection = cx_Oracle.connect(username, password, dsn)

# 创建游标
cursor = connection.cursor()

# 打印每个表的建表语句
for table in oracle_tables:
    try:
        # 执行 SQL 查询以获取表的建表语句
        cursor.execute(
            "SELECT dbms_metadata.get_ddl('TABLE', :table_name, :db_name) FROM dual",
            {'table_name': table[1].upper(),
             'db_name': table[0].upper()}
        )
        # 获取查询结果
        result = cursor.fetchone()



        # 将 cx_Oracle.LOB 对象转换为字符串
        ddl_oracle = result[0].read()

        # 删除 SEGMENT 部分
        ddl_oracle = ddl_oracle.split("SEGMENT")[0]

        # 去除字符串类型的长度限制
        ddl_hive = re.sub(r'\s*VARCHAR2\(\d+\)', 'STRING', ddl_oracle)

        # 将 DECIMAL 类型转换为 FLOAT
        ddl_hive = ddl_hive.replace("NUMBER", "FLOAT")

        # 去除 DEFAULT 和 NOT NULL 约束
        ddl_hive = re.sub(r'DEFAULT[^,]+', '', ddl_hive)
        ddl_hive = ddl_hive.replace("NOT NULL", "")

        # 去除临时表的 ON COMMIT PRESERVE ROWS 部分
        ddl_hive = ddl_hive.replace("ON COMMIT PRESERVE ROWS", "")

        # 去除字段命名中的空格
        ddl_hive = ddl_hive.replace('"', '')  # 去除字段名中的双引号
        ddl_hive = ddl_hive.replace(', ', ',')  # 去除字段名后的空格

        # 删除数据类型精度参数
        ddl_hive = re.sub(r'FLOAT\(\d+,\d+\)', 'FLOAT', ddl_hive)

        # 删除CHAR类型
        ddl_hive = re.sub(r'CHAR\(\d+\)', 'STRING', ddl_hive)

        # 将 DECIMAL 类型转换为 FLOAT
        ddl_hive = ddl_hive.replace("DECIMAL", "FLOAT")

        # 将字段名后的类型统一转换为大写
        ddl_hive = ddl_hive.replace("string", "STRING")
        ddl_hive = ddl_hive.replace("char", "STRING")

        # 去除字段名后的空格
        ddl_hive = ddl_hive.replace('FLOAT', 'FLOAT ').replace('STRING', 'STRING ')

        # 删除 ON COMMIT PRESERVE ROWS 部分
        ddl_hive = ddl_hive.replace("ON COMMIT PRESERVE ROWS", "")

        # 删除字段名中的双引号
        ddl_hive = ddl_hive.replace('"', '')

        # 打印Hive的建表语句
        print("Hive建表语句 for {full_name}:".format(full_name=table[2]))
        print(ddl_hive, "\n" * 3)









    except cx_Oracle.DatabaseError as e:
        error, = e.args
        # 如果是ORA-31603错误，表示对象不存在，继续处理下一个表
        if error.code == 31603:
            print(f"表 {table[2]} 不存在")
            continue
        else:
            # 其他错误，打印错误信息
            print(f"处理表 {table[2]} 时发生错误: {error.message}")

# 关闭游标和连接
cursor.close()
connection.close()
