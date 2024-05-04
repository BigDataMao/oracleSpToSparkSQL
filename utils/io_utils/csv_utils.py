# -*- coding: utf-8 -*-
import codecs
import csv
from typing import List, Dict, Any, OrderedDict


def csv_to_dict_list(file_path) -> List[Dict[str, Any]]:
    """
    读取 CSV 文件并转换为字典列表
    :param file_path: 文件路径
    :return: 字典列表
    """
    data: List[Dict[str, Any]] = []
    with open(file_path, 'r', encoding='utf-8') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        item: OrderedDict[str, Any]
        for item in csv_reader:
            data.append(dict(item))
    return data