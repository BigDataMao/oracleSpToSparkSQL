# -*- coding: utf-8 -*-
# 本机运行测试任务: https://blog.csdn.net/qq_39950572/article/details/136260712
# pip install pyspark==2.4.4
import codecs
import csv
import os

from os import path

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
    with codecs.open(file_path, 'r', encoding='utf8') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        for str_row in csv_reader:
            data.append(dict(str_row))
    return data


table_info = csv_to_dict_list(table_info_file)
table_info_clean = []

for item in table_info:
    oracle_info = item.get('oracle_table_full_name').strip().split('.')
    oracle_db = oracle_info[0].upper()
    oracle_table = oracle_info[1].upper()
    hive_table_full_name = item.get('hive_table_fullname').strip().lower()

