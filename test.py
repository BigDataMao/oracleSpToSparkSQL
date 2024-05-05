# -*- coding: utf-8 -*-
from pyhive import hive
conn = hive.Connection(
    host='cdh-master',
    port=10000,
    username='root'
)
curser = conn.cursor()
query = """
desc ods.t_ds_crm_broker
"""
curser.execute(query)
print(curser.fetchall())

curser.close()
conn.close()
