CREATE OR REPLACE PROCEDURE CF_BUSIMG.P_COCKPIT_00177_DATA(I_MONTH_ID    IN VARCHAR2,
                                                           O_RETURN_MSG  OUT VARCHAR2, --返回消息
                                                           O_RETURN_CODE OUT INTEGER --返回代码
                                                           ) IS

  ---------------------------------------------------------------------------------------
  -- copyright              wolf 1.0
  -- func_create_begin
  -- func_id                P_COCKPIT_00172
  -- version                1.0
  -- func_name              自有资金投资项目综合收益情况跟踪表二期-查询
  -- func_remark
  -- create_date            2024/04/22
  -- create_programer       lhh
  -- modify_remark
  -- func_create_end
  ---------------------------------------------------------------------------------------
  --固定变量
  ---------------------------------------------------------------------------------------
  v_op_object  VARCHAR2(50) DEFAULT 'P_COCKPIT_00172'; -- '操作对象';
  v_error_msg  VARCHAR2(200); --返回信息
  v_error_code INTEGER;
  v_userException EXCEPTION;

  ---------------------------------------------------------------------------------------
  --业务变量
  v_max_date    varchar2(10); --当月最大日期（自然日）
  ---------------------------------------------------------------------------------------

BEGIN
  select max(t.busi_date)
    into v_max_date
    from cf_sett.t_pub_date t
   where substr(t.busi_date, 1, 6) = I_MONTH_ID
     and t.market_no = '1';

 delete from CF_BUSIMG.T_COCKPIT_00177 t where t.BUSI_MONTH = I_MONTH_ID;
  COMMIT;
  INSERT INTO CF_BUSIMG.T_COCKPIT_00177
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
        from cf_busimg.t_cockpit_00146 t
        left join cf_busimg.t_cockpit_00152 c
          on t.project_id = c.project_id
         and c.busi_month = I_MONTH_ID
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
        from CF_BUSIMG.T_COCKPIT_00146 t),
    tmp_re_depart as
     (select b.project_id,
             listagg(b.recommend_depart, '/') within group(order by b.project_id) as recommend_department
        from cf_busimg.t_cockpit_00147 b
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
        from cf_busimg.t_cockpit_00146 a
        left join CF_BUSIMG.T_HYNC65_PRODUCT_RESULT t1
          on a.project_name = t1.securities_name
        left join CF_BUSIMG.T_HYNC65_PRODUCT_BALANCE t2
          on t1.account_period = t2.account_period
         and t1.product_code = t2.product_code
       where t1.account_period = substr(I_MONTH_ID, 1, 4) || '01'
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
        from cf_busimg.t_cockpit_00146 a
        left join CF_BUSIMG.T_HYNC65_PRODUCT_RESULT t1
          on a.project_name = t1.securities_name
        left join CF_BUSIMG.T_HYNC65_PRODUCT_BALANCE t2
          on t1.account_period = t2.account_period
         and t1.product_code = t2.product_code
       where t1.account_period = I_MONTH_ID
       group by a.project_id),
    tmp3 as --初始投资金额（元）
     (select a.project_id,
             sum(t2.local_end_balance) initial_investment_money
        from cf_busimg.t_cockpit_00146 a
        left join CF_BUSIMG.T_HYNC65_PRODUCT_BALANCE t2
          on a.project_name = t2.securities_name
       where t2.account_period = I_MONTH_ID
         and t2.account_code = '11010199'
       group by a.project_id),
    tmp4 as --月末市值（元）
     (select a.project_id, sum(t2.local_end_balance) month_end_market
        from cf_busimg.t_cockpit_00146 a
        left join CF_BUSIMG.T_HYNC65_PRODUCT_BALANCE t2
          on a.project_name = t2.securities_name
       where t2.account_period = I_MONTH_ID
         and (t2.account_code = '11010199' or t2.account_code = '11010201')
       group by a.project_id),
    tmp5 as --本年投资收益（元)
     (select a.project_id,
             sum(case
                   when t2.account_period = I_MONTH_ID then
                    t2.local_end_balance
                   else
                    0
                 end) - sum(case
                              when t2.account_period = substr(I_MONTH_ID, 1, 4) || '01' then
                               t2.local_end_balance
                              else
                               0
                            end) + sum(t1.investment_income) as investment_income
        from cf_busimg.t_cockpit_00146 a
        left join CF_BUSIMG.T_HYNC65_PRODUCT_RESULT t1
          on a.project_name = t1.securities_name
        left join CF_BUSIMG.T_HYNC65_PRODUCT_BALANCE t2
          on t1.account_period = t2.account_period
         and t1.product_code = t2.product_code
       where /*t1.account_period between substr(I_BUSI_MONTH,1,4)||'01' and I_BUSI_MONTH*/
      /*and*/
       t2.account_code = '11010201'
       group by a.project_id),
     tmp6 as --平均成本
     (select a.project_id,
             sum(t1.total_cost) cost
        from cf_busimg.t_cockpit_00146 a
        left join CF_BUSIMG.T_HYNC65_PRODUCT_RESULT t1
          on a.project_name = t1.securities_name
        left join CF_BUSIMG.T_HYNC65_PRODUCT_BALANCE t2
          on t1.account_period = t2.account_period
         and t1.product_code = t2.product_code
       where
       t2.account_code = '11010199'
       group by a.project_id),
      tmp7 as
      (select a.project_id,
             sum(case
                   when t2.account_period = I_MONTH_ID then
                    t2.local_end_balance
                   else
                    0
                 end) - sum(case
                              when t2.account_period = I_MONTH_ID then
                               t2.local_begin_balance
                              else
                               0
                            end) + sum(t1.investment_income) as investment_income
        from cf_busimg.t_cockpit_00146 a
        left join CF_BUSIMG.T_HYNC65_PRODUCT_RESULT t1
          on a.project_name = t1.securities_name
        left join CF_BUSIMG.T_HYNC65_PRODUCT_BALANCE t2
          on t1.account_period = t2.account_period
         and t1.product_code = t2.product_code
       where /*t1.account_period between substr(I_BUSI_MONTH,1,4)||'01' and I_BUSI_MONTH*/
      /*and*/
       t2.account_code = '11010201'
       group by a.project_id)
    select I_MONTH_ID as busi_month,

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
      from cf_busimg.t_cockpit_00146 t
      left join CF_BUSIMG.T_HYNC65_PRODUCT_TYPE a
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
  ------------------------------------------------
  COMMIT;


  O_RETURN_CODE := 0;
  O_RETURN_MSG  := '执行成功';
  -------------------------------------------------------------

  ---------------------------------------------------------------------------------------
  --错误处理部分
  ---------------------------------------------------------------------------------------
EXCEPTION
  when v_userException then
    o_return_code := v_error_code;
    o_return_msg  := v_error_msg;
    ROLLBACK;
    wolf.p_error_log('admin', -- '操作人';
                     v_op_object, -- '操作对象';
                     v_error_code, --'错误代码';
                     v_error_msg, -- '错误信息';
                     '',
                     '1',
                     o_return_msg, --返回信息
                     o_return_code --返回值 0 成功必须返回；-1 失败
                     );
  when OTHERS then
    o_return_code := SQLCODE;
    o_return_msg  := o_return_msg || SQLERRM;
    ROLLBACK;
    v_error_msg  := o_return_msg;
    v_error_code := o_return_code;
    wolf.p_error_log('admin', -- '操作人';
                     v_op_object, -- '操作对象';
                     v_error_code, --'错误代码';
                     v_error_msg, -- '错误信息';
                     '',
                     '2',
                     o_return_msg, --返回信息
                     o_return_code --返回值 0 成功必须返回；-1 失败
                     );
end;

