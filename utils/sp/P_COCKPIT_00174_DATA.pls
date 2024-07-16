CREATE OR REPLACE PROCEDURE CF_BUSIMG.P_COCKPIT_00174_DATA(i_month_id    IN VARCHAR2, --业务日期
                                                           O_RETURN_MSG  OUT VARCHAR2, --返回消息
                                                           O_RETURN_CODE OUT INTEGER --返回代码
                                                           ) IS
  ---------------------------------------------------------------------------------------
  -- copyright              wolf 1.0
  -- func_create_begin
  -- func_id
  -- version                1.0
  -- func_name
  -- func_remark            经纪业务收入落地表
  -- create_date            20240626
  -- create_programer       zzy
  -- modify_remark
  -- func_create_end
  ---------------------------------------------------------------------------------------
  /*
  经纪业务收入=留存手续费收入+利息收入+交易所减收
  留存手续费收入=手续费-上交手续费(资金对账表)
  利息收入=驾驶舱一期业务板块里的息差收入
  交易所减收=内核表-投资者交易所返还计算-二次开发(业务报表-薪酬报表)
  */
  ---------------------------------------------------------------------------------------
  v_op_object  varchar2(50) default 'P_COCKPIT_00174_DATA'; -- '操作对象';
  v_error_msg  varchar2(200); --返回信息
  v_error_code integer;
  v_userException exception;
  ---------------------------------------------------------------------------------------
  --业务变量
  v_begin_date varchar2(8); --开始日期
  v_end_date   varchar2(8); --结束日期

  v_nature_begin_date varchar2(8); --开始日期-自然日
  v_nature_end_date   varchar2(8); --结束日期-自然日

  ---------------------------------------------------------------------------------------
BEGIN
  select min(t.busi_date), max(t.busi_date)
    into v_begin_date, v_end_date
    from cf_sett.t_pub_date t
   where substr(t.busi_date, 1, 6) = i_month_id
     and t.busi_date < to_char(sysdate, 'yyyymmdd')
     and t.trade_flag = '1'
     and t.market_no = '1';

  select min(t.busi_date)
    into v_nature_begin_date
    from cf_sett.t_pub_date t
   where substr(t.busi_date, 1, 6) = i_month_id
     and t.busi_date < to_char(sysdate, 'yyyymmdd')
     and t.market_no = '1';

  v_nature_end_date := wolf.f_get_tradedate(v_end_date, 1);

  --计算留存手续费
  execute immediate 'truncate table cf_busimg.TMP_COCKPIT_00174_1';
  insert into cf_busimg.TMP_COCKPIT_00174_1
    (fund_account_id, remain_transfee)
    select t.fund_account_id, sum(t.REMAIN_TRANSFEE) as remain_transfee --留存手续费
      from cf_stat.T_RPT_06008 t
     where t.n_busi_date between v_begin_date and v_end_date
     group by t.fund_account_id;
  commit;

  --计算利息收入
  execute immediate 'truncate table cf_busimg.tmp_trade_date';
  insert /*+append*/
  into cf_busimg.tmp_trade_date
    (n_busi_date, trade_date, trade_flag)
    select c.busi_date N_busi_date,
           wolf.f_get_tradedate(c.busi_date, 0) trade_date,
           to_number(c.trade_flag) trade_flag
      from CF_sett.t_pub_date c
     where c.market_no = '1'
       and c.busi_date between v_nature_begin_date and v_nature_end_date;
  commit;

  --计算利息收入
  execute immediate 'truncate table cf_busimg.TMP_COCKPIT_00174_2';
  insert into cf_busimg.TMP_COCKPIT_00174_2
    (busi_date, fund_account_id, interest_base, interest_income)
    with tmp as
     (select t.busi_date,
             t.fund_account_id,
             sum(t.rights) - sum(case
                                   when t.impawn_money > t.margin then
                                    t.impawn_money
                                   else
                                    t.margin
                                 end) as interest_base
        from cf_sett.t_client_sett t
       where t.busi_date between v_begin_date and v_end_date
       group by t.busi_date, t.fund_account_id),
    tmp1 as
     (select t.busi_date,
             t.fund_account_id,
             sum(t.interest_base) as interest_base
        from tmp t
       inner join cf_busimg.tmp_trade_date b
          on t.busi_date = b.trade_date
       group by t.busi_date, t.fund_account_id)
    select a.busi_date,
           a.fund_account_id,
           sum(a.interest_base) as interest_base,
           sum(a.interest_base * b.interest_rate / (36000)) as interest_income
      from tmp1 a
     inner join CF_BUSIMG.T_COCKPIT_INTEREST_RATE b
        on a.busi_date between b.begin_date and b.end_date
     group by a.busi_date, a.fund_account_id;
  commit;

  --计算交易所减收
  execute immediate 'truncate table cf_busimg.TMP_COCKPIT_00174_3';
  insert into cf_busimg.TMP_COCKPIT_00174_3
    (FUND_ACCOUNT_ID, MARKET_REDUCT)
    with tmp as
     (select a.INVESTOR_ID as fund_account_id,
             sum(round(case
                         when a.tx_dt >=
                              to_char(to_date(v_begin_date, 'YYYY-MM-DD'), 'YYYY-MM-DD') then
                          a.EXCHANGE_TXFEE_AMT
                         else
                          0
                       end,
                       2)) EXCHANGE_TXFEE_AMT,
             sum(round(case
                         when a.tx_dt >=
                              to_char(to_date(v_begin_date, 'YYYY-MM-DD'), 'YYYY-MM-DD') then
                          a.RET_FEE_AMT_tx
                         else
                          0
                       end,
                       4)) RET_FEE_AMT,
             sum(round(case
                         when a.tx_dt <=
                              to_char(ADD_MONTHS(to_date(v_end_date, 'YYYY-MM-DD'), -1),
                                      'YYYY-MM-DD') then
                          a.RET_FEE_AMT_czce
                         else
                          0
                       end,
                       4)) RET_FEE_AMT_czce,
             sum(round(case
                         when a.tx_dt <=
                              to_char(ADD_MONTHS(to_date(v_end_date, 'YYYY-MM-DD'), -1),
                                      'YYYY-MM-DD') then
                          a.RET_FEE_AMT_dce
                         else
                          0
                       end,
                       4)) RET_FEE_AMT_dce,
             sum(round(case
                         when a.tx_dt >=
                              to_char(to_date(v_begin_date, 'YYYY-MM-DD'), 'YYYY-MM-DD') then
                          RET_FEE_AMT_shfe
                         else
                          0
                       end,
                       4)) RET_FEE_AMT_shfe,
             sum(round(case
                         when a.tx_dt < '2022-05-01' then
                          case
                            when a.tx_dt <=
                                 to_char(ADD_MONTHS(to_date(v_end_date, 'YYYY-MM-DD'), -1),
                                         'YYYY-MM-DD') then
                             RET_FEE_AMT_shfe1
                            else
                             0
                          end
                         else
                          0
                       end,
                       4)) RET_FEE_AMT_shfe1,
             sum(round(case
                         when a.tx_dt <=
                              to_char(ADD_MONTHS(to_date(v_end_date, 'YYYY-MM-DD'), -1),
                                      'YYYY-MM-DD') then
                          RET_FEE_AMT_cffex
                         else
                          0
                       end,
                       4)) RET_FEE_AMT_cffex,
             sum(round(case
                         when a.tx_dt <=
                              to_char(ADD_MONTHS(to_date(v_end_date, 'YYYY-MM-DD'), -1),
                                      'YYYY-MM-DD') then
                          RET_FEE_AMT_cffex2021
                         else
                          0
                       end,
                       4)) RET_FEE_AMT_cffex2021,
             sum(round(case
                         when a.tx_dt <=
                              to_char(ADD_MONTHS(to_date(v_end_date, 'YYYY-MM-DD'), -1),
                                      'YYYY-MM-DD') then
                          RET_FEE_AMT_dce31
                         else
                          '0'
                       end,
                       4)) RET_FEE_AMT_dce31,
             sum(round(case
                         when a.tx_dt <=
                              to_char(ADD_MONTHS(to_date(v_end_date, 'YYYY-MM-DD'), -1),
                                      'YYYY-MM-DD') then
                          RET_FEE_AMT_dce32
                         else
                          '0'
                       end,
                       4)) RET_FEE_AMT_dce32,
             sum(round(case
                         when a.tx_dt <=
                              to_char(ADD_MONTHS(to_date(v_end_date, 'YYYY-MM-DD'), -1),
                                      'YYYY-MM-DD') then
                          RET_FEE_AMT_dce33
                         else
                          '0'
                       end,
                       4)) RET_FEE_AMT_dce33,
             sum(round(case
                         when a.tx_dt <=
                              to_char(ADD_MONTHS(to_date(v_end_date, 'YYYY-MM-DD'), -1),
                                      'YYYY-MM-DD') then
                          a.RET_FEE_AMT_dce1
                         else
                          0
                       end,
                       4)) RET_FEE_AMT_dce1,
             sum(round(case
                         when a.tx_dt <=
                              to_char(ADD_MONTHS(to_date(v_end_date, 'YYYY-MM-DD'), -1),
                                      'YYYY-MM-DD') then
                          RET_FEE_AMT_dce2
                         else
                          0
                       end,
                       4)) RET_FEE_AMT_dce2,
             sum(round(nvl(case
                             when a.tx_dt >=
                                  to_char(to_date(v_begin_date, 'YYYY-MM-DD'), 'YYYY-MM-DD') then
                              a.investor_ret_amt
                             else
                              0
                           end,
                           0),
                       4)) investor_ret_amt
        from CTP63.T_DS_RET_EXCHANGE_RETFEE2 a
       inner join CTP63.T_DS_DC_ORG b
          on a.orig_department_id = b.department_id
       inner join CTP63.T_DS_DC_INVESTOR ff
          on a.investor_id = ff.investor_id
       where ((a.tx_dt between
             to_char(ADD_MONTHS(to_date(v_begin_date, 'YYYY-MM-DD'), -1),
                       'YYYY-MM-DD') and
             to_char(ADD_MONTHS(to_date(v_end_date, 'YYYY-MM-DD'), -1),
                       'YYYY-MM-DD')) or
             (a.tx_dt between
             to_char(to_date(v_begin_date, 'YYYY-MM-DD'), 'YYYY-MM-DD') and
             to_char(to_date(v_end_date, 'YYYY-MM-DD'), 'YYYY-MM-DD')))
       group by a.INVESTOR_ID)
    select a.fund_account_id,
           RET_FEE_AMT + RET_FEE_AMT_czce + RET_FEE_AMT_dce +
           RET_FEE_AMT_cffex + RET_FEE_AMT_cffex2021 + RET_FEE_AMT_shfe +
           RET_FEE_AMT_shfe1 + RET_FEE_AMT_dce1 + RET_FEE_AMT_dce2 +
           RET_FEE_AMT_dce31 + RET_FEE_AMT_dce32 + RET_FEE_AMT_dce33 as market_reduct --交易所减收
      from (select a.fund_account_id,
                   sum(EXCHANGE_TXFEE_AMT) EXCHANGE_TXFEE_AMT,
                   sum(RET_FEE_AMT) RET_FEE_AMT,
                   sum(RET_FEE_AMT_czce) RET_FEE_AMT_czce,
                   sum(RET_FEE_AMT_dce) RET_FEE_AMT_dce, --大连近月

                   sum(RET_FEE_AMT_cffex) RET_FEE_AMT_cffex,
                   sum(RET_FEE_AMT_cffex2021) RET_FEE_AMT_cffex2021,
                   sum(RET_FEE_AMT_dce31) RET_FEE_AMT_dce31,
                   sum(RET_FEE_AMT_dce32) RET_FEE_AMT_dce32,
                   sum(RET_FEE_AMT_dce33) RET_FEE_AMT_dce33,
                   sum(round(RET_FEE_AMT_dce1, 4)) RET_FEE_AMT_dce1,
                   sum(round(RET_FEE_AMT_dce2, 4)) RET_FEE_AMT_dce2,
                   sum(round(RET_FEE_AMT_shfe, 4)) RET_FEE_AMT_shfe,
                   sum(round(RET_FEE_AMT_shfe1, 4)) RET_FEE_AMT_shfe1,
                   sum(investor_ret_amt) investor_ret_amt,
                   1 order_seq
              from tmp a
             group by a.fund_account_id) a;
  commit;

  delete from cf_busimg.t_cockpit_00174 t where t.BUSI_MONTH = i_month_id;
  commit;

  insert into cf_busimg.t_cockpit_00174
    (busi_month,
     fund_account_id,
     branch_id,
     remain_transfee,
     interest_income,
     market_reduct,
     feature_income_total)
    with tmp as
     (select x.fund_account_id, sum(x.interest_income) as interest_income
        from cf_busimg.TMP_COCKPIT_00174_2 x
       group by x.fund_account_id),
    tmp1 as
     (select i_month_id as busi_month,
             t.fund_account_id,
             t.branch_id,
             nvl(a.remain_transfee, 0) as remain_transfee, --留存手续费
             nvl(b.interest_income, 0) as INTEREST_INCOME, --利息收入
             nvl(c.market_reduct, 0) as market_reduct, --交易所减收
             (nvl(a.remain_transfee, 0) + nvl(b.interest_income, 0) +
             nvl(c.market_reduct, 0)) as FEATURE_INCOME_TOTAL --经纪业务收入总计
        from cf_sett.t_fund_account t
        left join cf_busimg.TMP_COCKPIT_00174_1 a
          on t.fund_account_id = a.fund_account_id
        left join tmp b
          on t.fund_account_id = b.fund_account_id
        left join cf_busimg.TMP_COCKPIT_00174_3 c
          on t.fund_account_id = c.fund_account_id)
    select t.busi_month,
           t.fund_account_id,
           t.branch_id,
           t.remain_transfee,
           t.interest_income,
           t.market_reduct,
           t.feature_income_total
      from tmp1 t
     where t.FEATURE_INCOME_TOTAL <> 0;
  commit;

  o_return_code := 0;
  o_return_msg  := '执行成功';
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
