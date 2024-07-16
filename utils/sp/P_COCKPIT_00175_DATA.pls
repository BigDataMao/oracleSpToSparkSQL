CREATE OR REPLACE PROCEDURE CF_BUSIMG.P_COCKPIT_00175_DATA(I_MONTH_ID    IN VARCHAR2,
                                                           O_RETURN_MSG  OUT VARCHAR2, --返回消息
                                                           O_RETURN_CODE OUT INTEGER --返回代码
                                                           ) IS
  ---------------------------------------------------------------------------------------
  -- copyright              wolf 1.0
  -- func_create_begin
  -- func_id                P_COCKPIT_00175_DATA
  -- version                1.0
  -- func_name
  -- func_remark            IB协同收入调整表-按月落地
  -- create_date            20240628
  -- create_programer       zhongying.zhang
  -- modify_remark
  -- func_create_end
  ---------------------------------------------------------------------------------------
  --固定变量

  ---------------------------------------------------------------------------------------
  v_op_object  VARCHAR2(50) DEFAULT 'P_COCKPIT_00175_DATA'; -- '操作对象';
  v_error_msg  VARCHAR2(200); --返回信息
  v_error_code INTEGER;
  v_userException EXCEPTION;
  ---------------------------------------------------------------------------------------
  --业务变量
  v_trade_das          number;
  V_CONFIRM_BEGIN_DATE VARCHAR2(8);
  V_CONFIRM_END_DATE   VARCHAR2(8);
  ---------------------------------------------------------------------------------------

BEGIN
  select count(1), MIN(T.BUSI_DATE), MAX(T.BUSI_DATE)
    into v_trade_das, V_CONFIRM_BEGIN_DATE, V_CONFIRM_END_DATE
    from cf_sett.t_pub_date t
   where SUBSTR(t.busi_date, 1, 6) = I_MONTH_ID
     and t.market_no = '1';

  --计算  期初权益  期末权益  日均权益  留存手续费
  execute immediate 'truncate table CF_BUSIMG.TMP_COCKPIT_00125_1';
  insert into CF_BUSIMG.TMP_COCKPIT_00125_1
    (BUSI_DATE,
     FUND_ACCOUNT_ID,
     YES_RIGHTS,
     END_RIGHTS,
     AVG_RIGHTS,
     REMAIN_TRANSFEE)
    with tmp as
     (select greatest(t.regist_date, V_CONFIRM_BEGIN_DATE) as busi_date,
             t.fund_account_id
        from CF_BUSIMG.T_COCKPIT_00107 t
       where t.regist_date <= V_CONFIRM_END_DATE

         and t.service_type = '1' --服务类型(1:IB协同服务，2：IB驻点服务)
       group by t.regist_date, t.fund_account_id)
    select /*+leading(t) use_hash(t,a)*/
     t.busi_date,
     t.fund_account_id,
     sum(t.yes_rights) as yes_rights,
     sum(t.rights) as end_rights,
     sum(t.rights) as avg_rights,
     sum(t.transfee + t.delivery_transfee + t.strikefee - t.market_transfee -
         t.market_delivery_transfee - t.market_strikefee) as remain_transfee
      from cf_sett.t_client_sett t
     inner join tmp a
    --on t.busi_date = a.busi_date
        on t.fund_account_id = a.fund_account_id
     where t.busi_date between a.busi_date and V_CONFIRM_END_DATE
     group by t.busi_date, t.fund_account_id;
  commit;

  --计算 利息净收入=利息收入—客户和居间返还  （从薪酬模板-客户利返计算表 取累计息资金*利率*天数/360-利息）
  --利率表 CF_BUSIMG.T_COCKPIT_INTEREST_RATE
  execute immediate 'truncate table CF_BUSIMG.TMP_COCKPIT_00125_4';
  insert into CF_BUSIMG.TMP_COCKPIT_00125_4
    (FUND_ACCOUNT_ID, INTEREST_CLEAR_INCOME)
    with tmp as
     (select a.investor_id as fund_account_id,
             a.calint_days, --计息基准天数
             replace(a.date_str, '-', '') as busi_date,
             round(sum(a.calint_amt), 2) as SUM_CALINT_AMT, --累计计息资金
             sum(a.int_amt_d) as int_amt_d --利息
        from CTP63.T_DS_RET_INVESTORRET_INT a
       inner join CTP63.T_DS_DC_INVESTOR c
          on a.investor_id = c.investor_id
       where replace(a.date_str, '-', '') between V_CONFIRM_BEGIN_DATE and
             V_CONFIRM_END_DATE
         and a.today_ri_amt <> -a.calint_amt
       group by a.investor_id, a.calint_days, replace(a.date_str, '-', '')),
    tmp1 as
     (select t.fund_account_id,
             t.calint_days,
             a.INTEREST_RATE,
             sum(t.SUM_CALINT_AMT) as SUM_CALINT_AMT, --累计计息资金
             sum(t.int_amt_d) as int_amt_d
        from tmp t
       inner join CF_BUSIMG.T_COCKPIT_INTEREST_RATE a
          on t.busi_date between a.BEGIN_DATE and a.end_date
       group by t.fund_account_id, t.calint_days, a.INTEREST_RATE)
    select t.fund_account_id,
           (case
             when t.calint_days > 0 then
              t.SUM_CALINT_AMT * t.INTEREST_RATE * v_trade_das /
              t.calint_days
             else
              0
           end) - t.int_amt_d as interest_clear_income --利息净收入
      from tmp1 t
     inner join CF_BUSIMG.T_COCKPIT_00107 c
        on t.fund_account_id = c.fund_account_id
     where /*c.regist_date between I_CONFIRM_BEGIN_DATE and I_CONFIRM_END_DATE
                               and*/
     c.service_type = '1' --服务类型(1:IB协同服务，2：IB驻点服务)
    ;

  commit;

  --计算 减免收入=减免返还收入-减免返还支出(减免返还收入来自内核表-投资者交易所返还计算-二次开发的交易所减收，减免返还支出来自客户出入金流水)
  --减免返还收入
  execute immediate 'truncate table CF_BUSIMG.TMP_COCKPIT_00125_2';
  insert into CF_BUSIMG.TMP_COCKPIT_00125_2
    (FUND_ACCOUNT_ID, MARKET_REDUCT)

    with tmp as
     (select a.INVESTOR_ID as fund_account_id,
             sum(round(case
                         when a.tx_dt >=
                              to_char(to_date(V_CONFIRM_BEGIN_DATE, 'YYYY-MM-DD'),
                                      'YYYY-MM-DD') then
                          a.EXCHANGE_TXFEE_AMT
                         else
                          0
                       end,
                       2)) EXCHANGE_TXFEE_AMT,
             sum(round(case
                         when a.tx_dt >=
                              to_char(to_date(V_CONFIRM_BEGIN_DATE, 'YYYY-MM-DD'),
                                      'YYYY-MM-DD') then
                          a.RET_FEE_AMT_tx
                         else
                          0
                       end,
                       4)) RET_FEE_AMT,
             sum(round(case
                         when a.tx_dt <= to_char(ADD_MONTHS(to_date(V_CONFIRM_END_DATE,
                                                                    'YYYY-MM-DD'),
                                                            -1),
                                                 'YYYY-MM-DD') then
                          a.RET_FEE_AMT_czce
                         else
                          0
                       end,
                       4)) RET_FEE_AMT_czce,
             sum(round(case
                         when a.tx_dt <= to_char(ADD_MONTHS(to_date(V_CONFIRM_END_DATE,
                                                                    'YYYY-MM-DD'),
                                                            -1),
                                                 'YYYY-MM-DD') then
                          a.RET_FEE_AMT_dce
                         else
                          0
                       end,
                       4)) RET_FEE_AMT_dce,
             sum(round(case
                         when a.tx_dt >=
                              to_char(to_date(V_CONFIRM_BEGIN_DATE, 'YYYY-MM-DD'),
                                      'YYYY-MM-DD') then
                          RET_FEE_AMT_shfe
                         else
                          0
                       end,
                       4)) RET_FEE_AMT_shfe,
             sum(round(case
                         when a.tx_dt < '2022-05-01' then
                          case
                            when a.tx_dt <= to_char(ADD_MONTHS(to_date(V_CONFIRM_END_DATE,
                                                                       'YYYY-MM-DD'),
                                                               -1),
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
                         when a.tx_dt <= to_char(ADD_MONTHS(to_date(V_CONFIRM_END_DATE,
                                                                    'YYYY-MM-DD'),
                                                            -1),
                                                 'YYYY-MM-DD') then
                          RET_FEE_AMT_cffex
                         else
                          0
                       end,
                       4)) RET_FEE_AMT_cffex,
             sum(round(case
                         when a.tx_dt <= to_char(ADD_MONTHS(to_date(V_CONFIRM_END_DATE,
                                                                    'YYYY-MM-DD'),
                                                            -1),
                                                 'YYYY-MM-DD') then
                          RET_FEE_AMT_cffex2021
                         else
                          0
                       end,
                       4)) RET_FEE_AMT_cffex2021,
             sum(round(case
                         when a.tx_dt <= to_char(ADD_MONTHS(to_date(V_CONFIRM_END_DATE,
                                                                    'YYYY-MM-DD'),
                                                            -1),
                                                 'YYYY-MM-DD') then
                          RET_FEE_AMT_dce31
                         else
                          '0'
                       end,
                       4)) RET_FEE_AMT_dce31,
             sum(round(case
                         when a.tx_dt <= to_char(ADD_MONTHS(to_date(V_CONFIRM_END_DATE,
                                                                    'YYYY-MM-DD'),
                                                            -1),
                                                 'YYYY-MM-DD') then
                          RET_FEE_AMT_dce32
                         else
                          '0'
                       end,
                       4)) RET_FEE_AMT_dce32,
             sum(round(case
                         when a.tx_dt <= to_char(ADD_MONTHS(to_date(V_CONFIRM_END_DATE,
                                                                    'YYYY-MM-DD'),
                                                            -1),
                                                 'YYYY-MM-DD') then
                          RET_FEE_AMT_dce33
                         else
                          '0'
                       end,
                       4)) RET_FEE_AMT_dce33,
             sum(round(case
                         when a.tx_dt <= to_char(ADD_MONTHS(to_date(V_CONFIRM_END_DATE,
                                                                    'YYYY-MM-DD'),
                                                            -1),
                                                 'YYYY-MM-DD') then
                          a.RET_FEE_AMT_dce1
                         else
                          0
                       end,
                       4)) RET_FEE_AMT_dce1,
             sum(round(case
                         when a.tx_dt <= to_char(ADD_MONTHS(to_date(V_CONFIRM_END_DATE,
                                                                    'YYYY-MM-DD'),
                                                            -1),
                                                 'YYYY-MM-DD') then
                          RET_FEE_AMT_dce2
                         else
                          0
                       end,
                       4)) RET_FEE_AMT_dce2,
             sum(round(nvl(case
                             when a.tx_dt >=
                                  to_char(to_date(V_CONFIRM_BEGIN_DATE, 'YYYY-MM-DD'),
                                          'YYYY-MM-DD') then
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
       inner join CF_BUSIMG.T_COCKPIT_00107 c
          on a.investor_id = c.fund_account_id
       where ((a.tx_dt between
             to_char(ADD_MONTHS(to_date(V_CONFIRM_BEGIN_DATE, 'YYYY-MM-DD'),
                                  -1),
                       'YYYY-MM-DD') and
             to_char(ADD_MONTHS(to_date(V_CONFIRM_END_DATE, 'YYYY-MM-DD'),
                                  -1),
                       'YYYY-MM-DD')) or
             (a.tx_dt between
             to_char(to_date(V_CONFIRM_BEGIN_DATE, 'YYYY-MM-DD'),
                       'YYYY-MM-DD') and
             to_char(to_date(V_CONFIRM_END_DATE, 'YYYY-MM-DD'),
                       'YYYY-MM-DD')))
         and c.service_type = '1' --服务类型(1:IB协同服务，2：IB驻点服务)
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
             group by a.fund_account_id) a

    ;
  commit;
  --减免返还支出
  execute immediate 'truncate table CF_BUSIMG.TMP_COCKPIT_00125_3';
  insert into CF_BUSIMG.TMP_COCKPIT_00125_3
    select t.fund_account_id, sum(t.occur_money) as occur_money
      from CF_SETT.T_FUND_JOUR t
     inner join CF_BUSIMG.T_COCKPIT_00107 c
        on t.fund_account_id = c.fund_account_id
     where t.fund_type = '3' --公司调整
       and t.fund_direct = '1' --入金
       and c.service_type = '1' --服务类型(1:IB协同服务，2：IB驻点服务)
       and t.busi_date between V_CONFIRM_BEGIN_DATE and V_CONFIRM_END_DATE
     group by t.fund_account_id;
  commit;

  delete from CF_BUSIMG.T_COCKPIT_00175 t where t.Month_Id = I_MONTH_ID;
  COMMIT;
  INSERT INTO CF_BUSIMG.T_COCKPIT_00175
    (MONTH_ID, BRANCH_ID, XT_INCOME)
    with tmp as
     (
      select I_MONTH_ID,
              V_CONFIRM_BEGIN_DATE || '-' || V_CONFIRM_END_DATE, --登记日期
              t.IB_BRANCH_ID, --证券营业部编号
              t.FUND_ACCOUNT_ID, --资金账号
              t.futu_service_name, --期货服务人员
              t.branch_id, --期货营业部代码
              sum(a.yes_rights), --期初权益
              sum(a.end_rights), --期末权益,
              sum(a.avg_rights), --日均权益,
              sum(a.remain_transfee), --留存手续费
              sum(a.remain_transfee * t.coope_income_reate) as coope_income, --IB协同收入
              sum(nvl(b.interest_clear_income, 0)) as interest_clear_income, --利息收入
              sum(nvl(c.market_reduct, 0) - nvl(d.occur_money, 0)) as market_reduct_income, --减免收入
              t.COOPE_INCOME_REATE, --比例
              sum(nvl(b.interest_clear_income, 0) * t.COOPE_INCOME_REATE) as xt_interest_clear_income, --协同利息收入=利息收入*比例
              sum((nvl(c.market_reduct, 0) - nvl(d.occur_money, 0)) *
                  t.COOPE_INCOME_REATE) as xt_market_reduct_income --协同减免收入=减免收入*比例
        from CF_BUSIMG.T_COCKPIT_00107 t
       inner join CF_BUSIMG.TMP_COCKPIT_00125_1 a
          on t.fund_account_id = a.fund_account_id
        left join CF_BUSIMG.TMP_COCKPIT_00125_4 b
          on t.fund_account_id = b.fund_account_id
        left join CF_BUSIMG.TMP_COCKPIT_00125_2 c
          on t.fund_account_id = c.fund_account_id
        left join CF_BUSIMG.TMP_COCKPIT_00125_3 d
          on t.fund_account_id = d.fund_account_id
       where t.service_type = '1' --服务类型(1:IB协同服务，2：IB驻点服务)
       group by t.IB_BRANCH_ID,
                 t.FUND_ACCOUNT_ID,
                 t.futu_service_name,
                 t.branch_id,
                 t.COOPE_INCOME_REATE),
    tmp1 as
     (
      --因为 期货服务人员会重复，所以要先去重
      select t.FUND_ACCOUNT_ID,
              t.coope_income, --IB协同收入
              t.xt_interest_clear_income, --协同利息收入
              t.xt_market_reduct_income --协同减免收入
        from tmp t
       group by t.FUND_ACCOUNT_ID,
                 t.coope_income, --IB协同收入
                 t.xt_interest_clear_income, --协同利息收入
                 t.xt_market_reduct_income)
    select I_MONTH_ID,
           b.branch_id,
           sum(t.coope_income + t.xt_interest_clear_income +
               t.xt_market_reduct_income) as xt_income
      from tmp1 t
      left join cf_sett.t_fund_account b
        on t.fund_account_id = b.fund_account_id
     group by b.branch_id
    ;
  commit;
  -------------------------------------------------------------
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
