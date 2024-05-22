# -*- coding: utf-8 -*-

def get_busi_type(index_type, index_asses_benchmark, index_name):
    if index_type == '0' and index_asses_benchmark == '1':
        return '001'    # 考核收入
    elif index_type == '1' and index_asses_benchmark == '1':
        return '002'    # 经纪业务手续费收入市占率
    elif index_type == '0' and index_asses_benchmark == '2':
        return '003'    # 考核日均权益
    elif index_type == '1' and index_asses_benchmark == '2':
        return '004'    # 日均权益市占率
    elif index_type == '0' and index_asses_benchmark == '3':
        return '005'    # 考核利润
    elif index_type == '0' and index_asses_benchmark == '4':
        return '006'    # 考核成交额
    elif index_type == '1' and index_asses_benchmark == '4':
        return '007'    # 成交额市占率
    elif index_type == '0' and index_asses_benchmark == '5':
        return '008'    # 成交量
    elif index_type == '1' and index_asses_benchmark == '5':
        return '009'    # 成交量市占率
    elif index_type == '0' and index_asses_benchmark == '6' and index_name.like('%新增直接开发有效客户数量%'):
        return '010'    # 新增直接开发有效客户数量
    elif index_type == '0' and index_asses_benchmark == '6' and index_name.like('%新增有效客户数量%'):
        return '011'    # 新增有效客户数量
    elif index_type == '0' and index_asses_benchmark == '7':
        return '012'    # 产品销售额
    else:
        return ''
