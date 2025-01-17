# -*- coding: utf-8 -*-
from datetime import datetime

from config import Config
from utils.P_COCKPIT_ANALYSE_TOP_DATA import p_cockpit_analyse_top_data
from utils.P_COCKPIT_ANAL_LINE_TOP_DATA import p_cockpit_anal_line_top_data
from utils.P_COCKPIT_BUSI_ANALYSE_D_DATA import p_cockpit_busi_analyse_d_data
from utils.P_COCKPIT_BUSI_ANALYSE_M_DATA import p_cockpit_busi_analyse_m_data
from utils.P_COCKPIT_BUSI_ANAL_RESPONS_D import p_cockpit_busi_anal_respons_d
from utils.P_COCKPIT_BUSI_ANAL_RESPONS_M import p_cockpit_busi_anal_respons_m
from utils.P_COCKPIT_BUSI_ANAL_TAR_LINE_Q import p_cockpit_busi_anal_tar_line_q
from utils.P_COCKPIT_BUSI_ANAL_TAR_RESP_Q import p_cockpit_busi_anal_tar_resp_q
from utils.P_COCKPIT_BUSI_ANA_LINE_D_DATA import p_cockpit_busi_ana_line_d_data
from utils.P_COCKPIT_BUSI_ANA_LINE_M_DATA import p_cockpit_busi_ana_line_m_data
from utils.P_COCKPIT_BUSI_ANA_TOP_PD_DATA import p_cockpit_busi_ana_top_pd_data
from utils.P_COCKPIT_BU_ANAL_RESP_TOP_PD import p_cockpit_bu_anal_resp_top_pd
from utils.P_COCKPIT_BU_ANAL_TARG_RESP_Y import p_cockpit_bu_anal_targ_resp_y
from utils.P_COCKPIT_CLIENT_ANALYSE_DATA import p_cockpit_client_analyse_data
from utils.P_COCKPIT_CLIENT_LINE_DATA import p_cockpit_client_line_data
from utils.P_COCKPIT_CLIENT_RESPONS_DATA import p_cockpit_client_respons_data
from utils.P_COCKPIT_RESPONS_TOP_DATA import p_cockpit_respons_top_data
from utils.P_COCK_BUSI_ANAL_TARGET_Q_DATA import p_cock_busi_anal_target_q_data
from utils.P_COCK_BUSI_ANAL_TARGET_Y_DATA import p_cock_busi_anal_target_y_data
from utils.P_COC_BU_ANAL_TARG_LINE_Y_DATA import p_coc_bu_anal_targ_line_y_data
from utils.P_INDEX_RESULT_DATA_BRANCH import p_index_result_data_branch
from utils.parse_arguments import parse_arguments
from utils.task_env import create_env

config = Config()
logger = config.get_logger()


if __name__ == '__main__':
    spark = create_env()
    busi_date = parse_arguments()

    logger.info("接收到参数busi_date: %s", busi_date)
    pub_date_table = "edw.t10_pub_date"
    start_time = datetime.now()

    # 千万工程指标数据落地
    # p_cockpit_00092_data(spark, busi_date)
    # 千万工程开户时间区间落地数据
    # p_cockpit_00093_data(spark, busi_date)

    """
    二期驾驶舱数据落地
    """

    # 业务排名落地表(经营分析 - 业务排名驾驶舱)
    # 周数据，到年
    # p_cockpit_index_branch_data(spark, busi_date)
    # 客户分析 - 业务单位
    # 月份数据
    p_cockpit_client_analyse_data(spark, busi_date)
    # 客户分析 - 业务单位 - TOP9客户
    # 周数据，到年
    p_cockpit_analyse_top_data(spark, busi_date)
    # 客户分析 - 业务条线
    # 月份数据
    p_cockpit_client_line_data(spark, busi_date)
    # 客户分析 - 业务条线 - TOP9客户
    # 周数据，到年
    p_cockpit_anal_line_top_data(spark, busi_date)
    # 客户分析 - 分管部门
    # 月份数据
    p_cockpit_client_respons_data(spark, busi_date)
    # 客户分析 - 分管部门 - TOP9客户
    # 周数据，到年
    p_cockpit_respons_top_data(spark, busi_date)
    # 经营分析 - 业务单位 - 单日期落地
    # 日期数据
    p_cockpit_busi_analyse_d_data(spark, busi_date)
    # 经营分析 - 业务单位 - 按月落地
    # 月数据
    p_cockpit_busi_analyse_m_data(spark, busi_date)
    # 经营分析 - 业务单位 - 成交品种排名落地
    # 月份数据
    p_cockpit_busi_ana_top_pd_data(spark, busi_date)
    # 经营分析 - 业务单位 - 经营目标完成情况 - 按年
    # 无逻辑 - 年数据
    p_cock_busi_anal_target_y_data(spark, busi_date[:6])
    # 经营分析 - 业务单位 - 经营目标完成情况 - 按季度
    # 无逻辑 - 年数据
    p_cock_busi_anal_target_q_data(spark, busi_date)
    # 经营分析 - 业务条线 - 单日期落地
    # 日期数据
    p_cockpit_busi_ana_line_d_data(spark, busi_date)
    # 经营分析 - 业务条线 - 按月落地
    # 月数据
    p_cockpit_busi_ana_line_m_data(spark, busi_date)
    # 经营分析 - 业务条线 - 成交品种排名落地
    # 月数据
    # p_cockpit_bu_anal_line_top_pd(spark, busi_date)
    # 经营分析 - 业务条线 - 经营目标完成情况 - 按年
    # 无逻辑 - -年数据
    p_coc_bu_anal_targ_line_y_data(spark, busi_date[:6])
    # 经营分析 - 业务条线 - 经营目标完成情况 - 按季度
    # 无逻辑 - -年数据
    p_cockpit_busi_anal_tar_line_q(spark, busi_date)
    # 经营分析 - 分管部门 - 单日期落地
    # 日期数据
    p_cockpit_busi_anal_respons_d(spark, busi_date)
    # 经营分析 - 分管部门 - 按月落地
    # 月数据
    p_cockpit_busi_anal_respons_m(spark, busi_date)
    # 经营分析 - 分管部门 - 成交品种排名落地
    # 月数据
    p_cockpit_bu_anal_resp_top_pd(spark, busi_date)
    # 经营分析 - 分管部门 - 经营目标完成情况 - 按年
    # 年数据无逻辑
    p_cockpit_bu_anal_targ_resp_y(spark, busi_date[:6])
    # 经营分析 - 分管部门 - 经营目标完成情况 - 按季度
    # 年数据，无逻辑
    p_cockpit_busi_anal_tar_resp_q(spark, busi_date[:6])
    # 宏源-科目月余额表
    # p_hync65_account_balance(spark, busi_date)
    # 宏源-用友数据生成-财务内核表
    p_index_result_data_branch(spark, busi_date)

    end_time = datetime.now()
    duration = end_time - start_time
    duration = divmod(duration.seconds, 60)
    logger.info("函数 %s 执行时间: %s 分 %s 秒", "main_hy2", duration[0], duration[1])

