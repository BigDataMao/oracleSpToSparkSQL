# -*- coding: utf-8 -*-
from typing import List, Dict, Any

import oracledb

from config import Config

config = Config('config.json')
# logger = config.get_logger()
database = config.get('database')
user = database.get('user')
password = database.get('password')
dsn = database.get('dsn')


oracledb.init_oracle_client(lib_dir=r'/usr/lib/oracle/11.2/client64/lib')

def get_oracle_ddl(table_info: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    pool = oracledb.create_pool(
        user=user,
        password=password,
        dsn=dsn,
        min=1,
        max=5,
        increment=1,
    )
    oracle_pool = pool.acquire()
    table_info_clean = table_info
    with oracle_pool.cursor() as cursor:
        # 打印每个表的建表语句
        for index, item in enumerate(table_info):
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
    return table_info_clean