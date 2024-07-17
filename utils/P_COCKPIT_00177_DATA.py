# -*- coding: utf-8 -*-
import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, sum, greatest, lit, regexp_replace, round, when, expr, to_date, concat_ws, \
    collect_list, datediff

from utils.date_utils import get_date_period_and_days
from utils.task_env import log, return_to_hive


@log
def p_cockpit_00177_data(spark: SparkSession, i_month_id):
    """
    自有资金投资项目综合收益情况跟踪表二期-查询
    :param spark: SparkSession对象
    :param i_month_id: 月份ID,格式为"YYYYMM"
    :return:
    """

    v_max_date = get_date_period_and_days(
        spark=spark,
        busi_month=i_month_id,
        is_trade_day=False
    )[1]

    """
    INSERT INTO ddw.T_COCKPIT_00177
    (BUSI_MONTH,
     PROJECT_REFERRED,
     INVESTMENT_CATEGORY,
     CLASSIFI_OF_STRATEGY,
     RECOMMEND_DEPARTMENT,
     INVESTMENT_DATE,
     END_DATE,
     INVESTMENT_DAYS,
     BEGIN_NET_INVESTMENT,
     YEAR_BEING_NET_INVESTMENT,
     MONTH_END_NET_INVESTMENT,
     INITIAL_INVESTMENT_MONEY,
     MONTH_END_MARKET,
     COMPREHENSIVE_INCOME,
     INVESTMENT_INCOME,
     SYNERGISTIC_REVENUE,
     YEAR_COMPREHENSIVE_INCOME,
     YEAR_INVESTMENT_INCOME,
     YEAR_SYNERGISTIC_REVENUE,
     YEAR_COMPOSITE_RATE,
     ENDING_INTEREST,
     COMMITMENT_RIGHTS,
     DIFFERENCE_RIGHTS_INTERESTS,
     FUNDING_DATE,
     PRESENTATION_CONDITION,
     REMARK)
    with tmp as
     (select t.project_id,
             sum(case
                   when t.is_owned_funds = '0' then
                    c.project_end_right
                   else
                    0
                 end) as ending_interest,
             sum(case
                   when t.is_owned_funds = '0' then
                    c.promise_right
                   else
                    0
                 end) as commitment_rights,
             sum(case
                   when t.is_owned_funds = '0' then
                    c.right_diff
                   else
                    0
                 end) as difference_rights_interests,
             case
               when t.is_owned_funds = '0' then
                c.fund_time
             end as funding_date,
             case
               when t.is_owned_funds = '0' then
                c.describ
             end as presentation_condition,
             case
               when t.is_owned_funds = '0' then
                c.remark
             end as remark
        from ddw.t_cockpit_00146 t
        left join ddw.t_cockpit_00152 c
          on t.project_id = c.project_id
         and c.busi_month = i_month_id
       group by t.project_id,
                case
                  when t.is_owned_funds = '0' then
                   c.fund_time
                end,
                case
                  when t.is_owned_funds = '0' then
                   c.describ
                end,
                case
                  when t.is_owned_funds = '0' then
                   c.remark
                end,
                t.is_owned_funds),

    tmp_date_diff as
     (select t.project_id,
             (to_date((case
                        when t.end_date > v_max_date then
                         v_max_date
                        else
                         t.end_date
                      end),
                      'yyyy-mm-dd') - to_date(t.begin_date, 'yyyy-mm-dd')) + 1 as diff_pro --项目投资天数
        from ddw.T_COCKPIT_00146 t),
    tmp_re_depart as
     (select b.project_id,
             listagg(b.recommend_depart, '/') within group(order by b.project_id) as recommend_department
        from ddw.t_cockpit_00147 b
       group by b.project_id),
    tmp1 as --年初净值（元）
     (select a.project_id,
             sum(case
                   when t2.account_code = '11010199' then
                    t1.total_balance
                   else
                    0
                 end) + sum(case
                              when t2.account_code = '11010201' then
                               t1.changes_begin_value
                              else
                               0
                            end) as year_being_net_investment
        from ddw.t_cockpit_00146 a
        left join ddw.T_HYNC65_PRODUCT_RESULT t1
          on a.project_name = t1.securities_name
        left join ddw.T_HYNC65_PRODUCT_BALANCE t2
          on t1.account_period = t2.account_period
         and t1.product_code = t2.product_code
       where t1.account_period = substr(i_month_id, 1, 4) || '01'
       group by a.project_id),
    tmp2 as --月末净值（元）
     (select a.project_id,
             sum(case
                   when t2.account_code = '11010199' then
                    t1.total_balance
                   else
                    0
                 end) + sum(case
                              when t2.account_code = '11010201' then
                               t1.changes_end_value
                              else
                               0
                            end) as month_end_net_investment
        from ddw.t_cockpit_00146 a
        left join ddw.T_HYNC65_PRODUCT_RESULT t1
          on a.project_name = t1.securities_name
        left join ddw.T_HYNC65_PRODUCT_BALANCE t2
          on t1.account_period = t2.account_period
         and t1.product_code = t2.product_code
       where t1.account_period = i_month_id
       group by a.project_id),
    tmp3 as --初始投资金额（元）
     (select a.project_id,
             sum(t2.local_end_balance) initial_investment_money
        from ddw.t_cockpit_00146 a
        left join ddw.T_HYNC65_PRODUCT_BALANCE t2
          on a.project_name = t2.securities_name
       where t2.account_period = i_month_id
         and t2.account_code = '11010199'
       group by a.project_id),
    tmp4 as --月末市值（元）
     (select a.project_id, sum(t2.local_end_balance) month_end_market
        from ddw.t_cockpit_00146 a
        left join ddw.T_HYNC65_PRODUCT_BALANCE t2
          on a.project_name = t2.securities_name
       where t2.account_period = i_month_id
         and (t2.account_code = '11010199' or t2.account_code = '11010201')
       group by a.project_id),
    tmp5 as --本年投资收益（元)
     (select a.project_id,
             sum(case
                   when t2.account_period = i_month_id then
                    t2.local_end_balance
                   else
                    0
                 end) - sum(case
                              when t2.account_period = substr(i_month_id, 1, 4) || '01' then
                               t2.local_end_balance
                              else
                               0
                            end) + sum(t1.investment_income) as investment_income
        from ddw.t_cockpit_00146 a
        left join ddw.T_HYNC65_PRODUCT_RESULT t1
          on a.project_name = t1.securities_name
        left join ddw.T_HYNC65_PRODUCT_BALANCE t2
          on t1.account_period = t2.account_period
         and t1.product_code = t2.product_code
       where /*t1.account_period between substr(I_BUSI_MONTH,1,4)||'01' and I_BUSI_MONTH*/
      /*and*/
       t2.account_code = '11010201'
       group by a.project_id),
     tmp6 as --平均成本
     (select a.project_id,
             sum(t1.total_cost) cost
        from ddw.t_cockpit_00146 a
        left join ddw.T_HYNC65_PRODUCT_RESULT t1
          on a.project_name = t1.securities_name
        left join ddw.T_HYNC65_PRODUCT_BALANCE t2
          on t1.account_period = t2.account_period
         and t1.product_code = t2.product_code
       where
       t2.account_code = '11010199'
       group by a.project_id),
      tmp7 as
      (select a.project_id,
             sum(case
                   when t2.account_period = i_month_id then
                    t2.local_end_balance
                   else
                    0
                 end) - sum(case
                              when t2.account_period = i_month_id then
                               t2.local_begin_balance
                              else
                               0
                            end) + sum(t1.investment_income) as investment_income
        from ddw.t_cockpit_00146 a
        left join ddw.T_HYNC65_PRODUCT_RESULT t1
          on a.project_name = t1.securities_name
        left join ddw.T_HYNC65_PRODUCT_BALANCE t2
          on t1.account_period = t2.account_period
         and t1.product_code = t2.product_code
       where /*t1.account_period between substr(I_BUSI_MONTH,1,4)||'01' and I_BUSI_MONTH*/
      /*and*/
       t2.account_code = '11010201'
       group by a.project_id)
    select i_month_id as busi_month,

           t.project_name as project_referred,
           t.strategy_type as investment_category,
           a.product_type as classifi_of_strategy,
           b.recommend_department as recommend_department,
           t.begin_date as investment_date,
           t.end_date as end_date,
           d.diff_pro as investment_days,
           1.0000 as begin_net_investment,
           e.year_being_net_investment as year_being_net_investment,
           f.month_end_net_investment as month_end_net_investment,
           g.initial_investment_money as initial_investment_money,
           h.month_end_market as month_end_market,
           k.investment_income as comprehensive_income,
           i.investment_income as investment_income,
           '' as synergistic_revenue,
           i.investment_income as year_comprehensive_income,
           i.investment_income as year_investment_income,
           '' as year_synergistic_revenue,
           i.investment_income/j.cost /d.diff_pro as year_composite_rate

          ,
           c.ending_interest             as ending_interest,
           c.commitment_rights           as commitment_rights,
           c.difference_rights_interests as difference_rights_interests,
           c.funding_date                as funding_date,
           c.presentation_condition      as presentation_condition,
           c.remark                      as remark
      from ddw.t_cockpit_00146 t
      left join ddw.T_HYNC65_PRODUCT_TYPE a
        on t.project_id = a.product_code
      left join tmp_re_depart b
        on t.project_id = b.project_id
      left join tmp c
        on t.project_id = c.project_id
      left join tmp_date_diff d
        on t.project_id = d.project_id
      left join tmp1 e
        on t.project_id = e.project_id
      left join tmp2 f
        on t.project_id = f.project_id
      left join tmp3 g
        on t.project_id = g.project_id
      left join tmp4 h
        on t.project_id = h.project_id
      left join tmp5 i
        on t.project_id = i.project_id
      left join tmp6 j
        on t.project_id = j.project_id
      left join tmp7 k
        on t.project_id = k.project_id

    ;
    """

    # 读取数据
    t_df: DataFrame = spark.table("ddw.t_cockpit_00146")
    c_df: DataFrame = spark.table("ddw.t_cockpit_00152")
    b_df: DataFrame = spark.table("ddw.t_cockpit_00147")
    t1_df: DataFrame = spark.table("ddw.T_HYNC65_PRODUCT_RESULT")
    t2_df: DataFrame = spark.table("ddw.T_HYNC65_PRODUCT_BALANCE")
    a_df: DataFrame = spark.table("ddw.T_HYNC65_PRODUCT_TYPE")

    # 模块1: tmp
    tmp_df = t_df.join(c_df, (t_df.project_id == c_df.project_id) & (c_df.busi_month == i_month_id), "left") \
        .groupBy(
        t_df.project_id.alias("project_id"),
        when(t_df.is_owned_funds == '0', c_df.fund_time).otherwise(None).alias("funding_date"),
        when(t_df.is_owned_funds == '0', c_df.describ).otherwise(None).alias("presentation_condition"),
        when(t_df.is_owned_funds == '0', c_df.remark).otherwise(None).alias("remark")
    ).agg(
        sum(when(t_df.is_owned_funds == '0', c_df.project_end_right).otherwise(0)).alias("ending_interest"),
        sum(when(t_df.is_owned_funds == '0', c_df.promise_right).otherwise(0)).alias("commitment_rights"),
        sum(when(t_df.is_owned_funds == '0', c_df.right_diff).otherwise(0)).alias("difference_rights_interests")
    )

    # 模块2: tmp_date_diff
    tmp_date_diff_df = t_df.withColumn(
        "diff_pro",
        datediff(
            to_date(when(t_df.end_date > v_max_date, v_max_date).otherwise(t_df.end_date), 'yyyy-MM-dd'),
            to_date(t_df.begin_date, 'yyyy-MM-dd')
        ) + 1
    )

    # 模块3: tmp_re_depart
    tmp_re_depart_df = b_df.groupBy("project_id") \
        .agg(
        concat_ws("/", collect_list("recommend_depart")).alias("recommend_department")
    )

    # 模块4: tmp1
    tmp1_df = t_df.join(t1_df, t_df.project_name == t1_df.securities_name, "left") \
        .join(t2_df, (t1_df.account_period == t2_df.account_period) & (t1_df.product_code == t2_df.product_code),
              "left") \
        .where(t1_df.account_period == i_month_id[:4] + '01') \
        .groupBy(t_df.project_id) \
        .agg(
        sum(when(t2_df.account_code == '11010199', t1_df.total_balance).otherwise(0) +
            when(t2_df.account_code == '11010201', t1_df.changes_begin_value).otherwise(0)).alias(
            "year_being_net_investment")
    )

    # 模块5: tmp2
    tmp2_df = t_df.join(t1_df, t_df.project_name == t1_df.securities_name, "left") \
        .join(t2_df, (t1_df.account_period == t2_df.account_period) & (t1_df.product_code == t2_df.product_code),
              "left") \
        .where(t1_df.account_period == i_month_id) \
        .groupBy(t_df.project_id) \
        .agg(
        sum(when(t2_df.account_code == '11010199', t1_df.total_balance).otherwise(0) +
            when(t2_df.account_code == '11010201', t1_df.changes_end_value).otherwise(0)).alias(
            "month_end_net_investment")
    )

    # 模块6: tmp3
    tmp3_df = t_df.join(t2_df, t_df.project_name == t2_df.securities_name, "left") \
        .where((t2_df.account_period == i_month_id) & (t2_df.account_code == '11010199')) \
        .groupBy(t_df.project_id) \
        .agg(sum(t2_df.local_end_balance).alias("initial_investment_money"))

    # 模块7: tmp4
    tmp4_df = t_df.join(t2_df, t_df.project_name == t2_df.securities_name, "left") \
        .where((t2_df.account_period == i_month_id) & (
                (t2_df.account_code == '11010199') | (t2_df.account_code == '11010201'))) \
        .groupBy(t_df.project_id) \
        .agg(sum(t2_df.local_end_balance).alias("month_end_market"))

    # 模块8: tmp5
    tmp5_df = t_df.join(t1_df, t_df.project_name == t1_df.securities_name, "left") \
        .join(t2_df, (t1_df.account_period == t2_df.account_period) & (t1_df.product_code == t2_df.product_code),
              "left") \
        .where(t2_df.account_code == '11010201') \
        .groupBy(t_df.project_id) \
        .agg(
        (sum(when(t2_df.account_period == i_month_id, t2_df.local_end_balance).otherwise(0)) -
         sum(when(t2_df.account_period == i_month_id[:4] + '01', t2_df.local_end_balance).otherwise(0)) +
         sum(t1_df.investment_income)).alias("investment_income")
    )

    # 模块9: tmp6
    tmp6_df = t_df.join(t1_df, t_df.project_name == t1_df.securities_name, "left") \
        .join(t2_df, (t1_df.account_period == t2_df.account_period) & (t1_df.product_code == t2_df.product_code),
              "left") \
        .where(t2_df.account_code == '11010199') \
        .groupBy(t_df.project_id) \
        .agg(sum(t1_df.total_cost).alias("cost"))

    # 模块10: tmp7
    tmp7_df = t_df.join(t1_df, t_df.project_name == t1_df.securities_name, "left") \
        .join(t2_df, (t1_df.account_period == t2_df.account_period) & (t1_df.product_code == t2_df.product_code),
              "left") \
        .where(t2_df.account_code == '11010201') \
        .groupBy(t_df.project_id) \
        .agg(
        (sum(when(t2_df.account_period == i_month_id, t2_df.local_end_balance).otherwise(0)) -
         sum(when(t2_df.account_period == i_month_id, t2_df.local_begin_balance).otherwise(0)) +
         sum(t1_df.investment_income)).alias("investment_income")
    )

    # 最终结果合并
    result_df = t_df.alias("t") \
        .join(a_df.alias("a"), col("t.project_id") == col("a.product_code"), "left") \
        .join(tmp_re_depart_df.alias("b"), col("t.project_id") == col("b.project_id"), "left") \
        .join(tmp_df.alias("c"), col("t.project_id") == col("c.project_id"), "left") \
        .join(tmp_date_diff_df.alias("d"), col("t.project_id") == col("d.project_id"), "left") \
        .join(tmp1_df.alias("e"), col("t.project_id") == col("e.project_id"), "left") \
        .join(tmp2_df.alias("f"), col("t.project_id") == col("f.project_id"), "left") \
        .join(tmp3_df.alias("g"), col("t.project_id") == col("g.project_id"), "left") \
        .join(tmp4_df.alias("h"), col("t.project_id") == col("h.project_id"), "left") \
        .join(tmp5_df.alias("i"), col("t.project_id") == col("i.project_id"), "left") \
        .join(tmp6_df.alias("j"), col("t.project_id") == col("j.project_id"), "left") \
        .join(tmp7_df.alias("k"), col("t.project_id") == col("k.project_id"), "left") \
        .select(
        lit(i_month_id).alias("busi_month"),
        col("t.project_name").alias("project_referred"),
        col("t.strategy_type").alias("investment_category"),
        col("a.product_type").alias("classifi_of_strategy"),
        col("b.recommend_department").alias("recommend_department"),
        col("t.begin_date").alias("investment_date"),
        col("t.end_date").alias("end_date"),
        col("d.diff_pro").alias("investment_days"),
        lit(1.0000).alias("begin_net_investment"),
        col("e.year_being_net_investment").alias("year_being_net_investment"),
        col("f.month_end_net_investment").alias("month_end_net_investment"),
        col("g.initial_investment_money").alias("initial_investment_money"),
        col("h.month_end_market").alias("month_end_market"),
        col("i.investment_income").alias("comprehensive_income"),
        col("i.investment_income").alias("investment_income"),
        lit("").alias("synergistic_revenue"),
        col("i.investment_income").alias("year_comprehensive_income"),
        col("i.investment_income").alias("year_investment_income"),
        lit("").alias("year_synergistic_revenue"),
        (col("i.investment_income") / col("j.cost") / col("d.diff_pro")).alias("year_composite_rate"),
        col("c.ending_interest").alias("ending_interest"),
        col("c.commitment_rights").alias("commitment_rights"),
        col("c.difference_rights_interests").alias("difference_rights_interests"),
        col("c.funding_date").alias("funding_date"),
        col("c.presentation_condition").alias("presentation_condition"),
        col("c.remark").alias("remark")
    )

    return_to_hive(
        spark=spark,
        df_result=result_df,
        target_table="ddw.T_COCKPIT_00177",
        insert_mode="overwrite"
    )

