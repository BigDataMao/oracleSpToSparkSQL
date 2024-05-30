# -*- coding: utf-8 -*-

def get_busi_type(index_type, index_asses_benchmark, index_name):
    conditions = {
        ('0', '1'): '001',  # 考核收入
        ('1', '1'): '002',  # 经纪业务手续费收入市占率
        ('0', '2'): '003',  # 考核日均权益
        ('1', '2'): '004',  # 日均权益市占率
        ('0', '3'): '005',  # 考核利润
        ('0', '4'): '006',  # 考核成交额
        ('1', '4'): '007',  # 成交额市占率
        ('0', '5'): '008',  # 成交量
        ('1', '5'): '009',  # 成交量市占率
        ('0', '7'): '012',  # 产品销售额
    }

    # 检查特定的字符串匹配条件
    if index_type == '0' and index_asses_benchmark == '6':
        if '新增直接开发有效客户数量' in index_name:
            return '010'  # 新增直接开发有效客户数量
        elif '新增有效客户数量' in index_name:
            return '011'  # 新增有效客户数量

    # 从条件字典中获取结果
    return conditions.get((index_type, index_asses_benchmark), '')
