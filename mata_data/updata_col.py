# -*- coding: utf-8 -*-
import oracledb

from config import Config

config = Config()
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
col_map = {
    "edw.h11_client_organprop": "客户机构属性",
    "edw.h11_client_prop": "客户属性",
    "edw.h11_client_runprop": "客户运行属性",
    "edw.h11_branch": "分支机构",
    "edw.h11_client": "客户",
    "edw.h12_trade_account": "交易账户",
    "edw.h11_market": "市场",
    "edw.h12_bank_account": "银行账户",
    "edw.h12_fund_account": "资金账户",
    "edw.h11_client_linkman": "客户联系人",
    "edw.tmp_hync65_balance_product": "临时_余额产品",
    "edw.tmp_hync65_balance_product1": "临时_余额产品1",
    "edw.tmp_tradingcode": "临时_交易代码",
    "edw.h13_impawn_info": "质押信息",
    "edw.h13_product": "产品",
    "edw.h15_market_data": "市场数据",
    "edw.h14_entrust": "委托",
    "edw.h15_market_data_tmp": "临时_市场数据",
    "edw.h15_market_sett": "市场结算",
    "edw.h14_close_detail": "平仓明细",
    "edw.h14_entrust_tmp": "临时_委托",
    "edw.h15_client_sett_tmp": "临时_客户结算",
    "edw.h14_close_detail_tmp": "临时_平仓明细",
    "edw.h16_account_info": "账户信息",
    "edw.h14_execute_result": "执行结果",
    "edw.t10_dict_entry": "字典条目",
    "edw.t10_dict_entry_map": "字典条目映射",
    "edw.t10_dictionary": "字典",
    "edw.h14_delivery": "交割",
    "edw.tmp_account_balance2": "临时_账户余额2",
    "edw.tmp_balance1_product": "临时_余额产品1",
    "edw.tmp_balance2_product": "临时_余额产品2",
    "edw.h14_execute_result_tmp": "临时_执行结果",
    "edw.h15_hold_balance": "持仓余额",
    "edw.h16_book_info": "账簿信息",
    "edw.h16_hync65_account_balance": "账簿_账户余额",
    "edw.h14_delivery_tmp": "临时_交割",
    "edw.t10_pub_date": "发布日期",
    "edw.t_table_perform": "表性能",
    "edw.tmp_account_balance1": "临时_账户余额1",
    "edw.h14_fund_jour": "资金流水",
    "edw.h15_hold_balance_tmp": "临时_持仓余额",
    "edw.h14_done": "成交表",
    "edw.h14_fund_jour_tmp": "临时_资金流水",
    "edw.h16_hync65_account_voucher": "账簿_账户凭证",
    "edw.h14_impawn_io": "质押出入",
    "edw.h15_hold_detail": "持仓明细",
    "edw.h16_hync65_voucher_product": "账簿_凭证产品",
    "edw.h17_security": "证券",
    "edw.h14_impawn_io_tmp": "临时_质押出入",
    "edw.h15_market_sett_tmp": "临时_市场结算",
    "edw.h14_done_tmp": "临时_成交表",
    "edw.h14_market_fund_jour": "市场资金流水",
    "edw.h15_hold_detail_tmp": "临时_持仓明细",
    "edw.h16_hync65_balance_product": "账簿_余额产品",
    "edw.h14_market_fund_jour_tmp": "临时_市场资金流水",
    "edw.h15_client_sett": "客户结算"
}
with oracle_pool.cursor() as cursor:
    for key in col_map:
        sql = f"UPDATE DMP.T_MD_OBJ SET OBJ_NAME = '{col_map.get(key)}' where OBJ_CODE = '{key}'"
        cursor.execute(sql)
oracle_pool.commit()
pool.release(oracle_pool)
pool.close()

