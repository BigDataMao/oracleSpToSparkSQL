create or replace procedure cf_busimg.P_COCKPIT_CLIENT_ANALYSE_DATA(i_busi_date   in varchar2,
                                                                    o_return_code out integer,
                                                                    o_return_msg  out varchar2) is
  --========================================================================================================================
  --系统名称:生成驾驶舱数据
  --模块名称:
  --模块编号:
  --模块描述： CF_BUSIMG.T_COCKPIT_CLIENT_ANALYSE  客户分析落地表(客户分析-业务单位)
  --开发人员:zhongying.zhang
  --目前版本:
  --创建时间:20230328
  --版    权:
  --修改历史:

  --========================================================================================================================
  v_op_object  VARCHAR2(50) DEFAULT 'P_COCKPIT_CLIENT_ANALYSE_DATA'; -- '操作对象';
  v_error_msg  VARCHAR2(200); --返回信息
  v_error_code INTEGER;
  v_userException EXCEPTION;

  v_busi_month      varchar2(6);
  v_last_busi_month varchar2(6);
  v_begin_date      varchar2(8);
  v_end_date        varchar2(8);
  v_last_begin_date varchar2(8);
  v_last_end_date   varchar2(8);

  v_trade_days      number; --本月交易日天数
  v_last_trade_days number; --同比月份交易日天数
  v_open_begin_date varchar2(8); --开户日期开始时间
  v_open_end_date   varchar2(8); --开户日期结束时间

  v_busi_year_trade_days number; --本年交易日天数

  v_last_open_begin_date varchar2(8); --去年开户开始日期
  v_last_open_end_date   varchar2(8); --全年开户结束日期
  v_last_year_trade_days number; --去年交易日天数
  v_jgx_busi_date        varchar(8); --净贡献日期，净贡献数据特殊，日期为每月1号
begin
  v_busi_month           := substr(i_busi_date, 1, 6);
  v_jgx_busi_date        := v_busi_month || '01';
  v_last_busi_month      := substr(to_char(add_months(to_date(I_BUSI_DATE,
                                                              'yyyymmdd'),
                                                      -12),
                                           'yyyymmdd'),
                                   1,
                                   6);
  v_last_open_begin_date := substr(v_last_busi_month, 1, 4) || '0101';
  v_last_open_end_date   := substr(v_last_busi_month, 1, 4) || '1231';

  v_open_begin_date := substr(i_busi_date, 1, 4) || '0101';
  v_open_end_date   := v_busi_month || '31';

  select min(t.busi_date), max(t.busi_date), count(1)
    into v_begin_date, v_end_date, v_trade_days
    from cf_sett.t_pub_date t
   where substr(t.busi_date, 1, 6) = v_busi_month
     and t.market_no = '1'
     and t.trade_flag = '1'
     and t.busi_date <= i_busi_date;

  select min(t.busi_date), max(t.busi_date), count(1)
    into v_last_begin_date, v_last_end_date, v_last_trade_days
    from cf_sett.t_pub_date t
   where substr(t.busi_date, 1, 6) = v_last_busi_month
     and t.market_no = '1'
     and t.trade_flag = '1';

  --本年交易日天数
  select count(1)
    into v_busi_year_trade_days
    from cf_sett.t_pub_date t
   where t.busi_date between v_open_begin_date and v_end_date
     and t.market_no = '1'
     and t.trade_flag = '1';

  --去年交易日天数
  select count(1)
    into v_last_year_trade_days
    from cf_sett.t_pub_date t
   where t.busi_date between v_last_open_begin_date and
         v_last_open_end_date
     and t.market_no = '1'
     and t.trade_flag = '1';

  delete from CF_BUSIMG.T_COCKPIT_CLIENT_ANALYSE t
   where t.busi_month = v_busi_month

  ;
  commit;

  --初始化数据
  insert into CF_BUSIMG.T_COCKPIT_CLIENT_ANALYSE
    (BUSI_MONTH, OA_BRANCH_ID)
    select v_busi_month as busi_month, t.departmentid
      from cf_busimg.T_OA_BRANCH t
     where t.canceled is null;
  commit;

  --权益结构
  merge into CF_BUSIMG.T_COCKPIT_CLIENT_ANALYSE a
  using (
    with tmp as
     (select t.fund_account_id,
             b.client_type,
             c.oa_branch_id,
             sum(t.rights) as rights
        from cf_sett.t_client_sett t
        left join cf_sett.t_fund_account b
          on t.fund_account_id = b.fund_account_id
       inner join cf_busimg.t_ctp_branch_oa_rela c
          on b.branch_id = c.ctp_branch_id
       where t.busi_date between v_begin_date and v_end_date
         and c.oa_branch_id is not null
       group by t.fund_account_id, b.client_type, c.oa_branch_id),
    tmp_last as
     (select t.fund_account_id,
             b.client_type,
             c.oa_branch_id,
             sum(t.rights) as rights
        from cf_sett.t_client_sett t
        left join cf_sett.t_fund_account b
          on t.fund_account_id = b.fund_account_id
       inner join cf_busimg.t_ctp_branch_oa_rela c
          on b.branch_id = c.ctp_branch_id
       where t.busi_date between v_last_begin_date and v_last_end_date
         and c.oa_branch_id is not null
       group by t.fund_account_id, b.client_type, c.oa_branch_id

      ),
    tmp_1 as
     (select t.oa_branch_id,

             sum(t.rights) / v_trade_days as total_rights,
             sum(case
                   when t.client_type = '0' then
                    t.rights
                   else
                    0
                 end) / v_trade_days as rights_0,
             sum(case
                   when t.client_type = '1' then
                    t.rights
                   else
                    0
                 end) / v_trade_days as rights_1,
             sum(case
                   when t.client_type in ('3', '4') then
                    t.rights
                   else
                    0
                 end) / v_trade_days as rights_3
        from tmp t
       group by t.oa_branch_id

      ),
    tmp_2 as
     (select t.oa_branch_id,

             sum(t.rights) / v_last_trade_days as total_rights,
             sum(case
                   when t.client_type = '0' then
                    t.rights
                   else
                    0
                 end) / v_last_trade_days as rights_0,
             sum(case
                   when t.client_type = '1' then
                    t.rights
                   else
                    0
                 end) / v_last_trade_days as rights_1,
             sum(case
                   when t.client_type in ('3', '4') then
                    t.rights
                   else
                    0
                 end) / v_last_trade_days as rights_3
        from tmp_last t
       group by t.oa_branch_id

      ),
    tmp_result as
     (select t.oa_branch_id,
             (case
               when nvl(a.total_rights, 0) <> 0 then
                nvl(a.rights_1, 0) / nvl(a.total_rights, 0)
               else
                0
             end) * 100 as rights_1,
             (case
               when nvl(a.total_rights, 0) <> 0 then
                nvl(a.rights_3, 0) / nvl(a.total_rights, 0)
               else
                0
             end) * 100 as rights_3,
             (case
               when nvl(a.total_rights, 0) <> 0 then
                nvl(a.rights_0, 0) / nvl(a.total_rights, 0)
               else
                0
             end) * 100 as rights_0,
             (case
               when nvl(b.total_rights, 0) <> 0 then
                nvl(b.rights_1, 0) / nvl(b.total_rights, 0)
               else
                0
             end) * 100 as last_rights_1,
             (case
               when nvl(b.total_rights, 0) <> 0 then
                nvl(b.rights_3, 0) / nvl(b.total_rights, 0)
               else
                0
             end) * 100 as last_rights_3,
             (case
               when nvl(b.total_rights, 0) <> 0 then
                nvl(b.rights_0, 0) / nvl(b.total_rights, 0)
               else
                0
             end) * 100 as last_rights_0
        from CF_BUSIMG.T_COCKPIT_CLIENT_ANALYSE t
        left join tmp_1 a
          on t.oa_branch_id = a.oa_branch_id
        left join tmp_2 b
          on t.oa_branch_id = b.oa_branch_id
       where t.BUSI_MONTH = v_busi_month)
    select t.oa_branch_id,
           t.rights_1,
           (case
             when nvl(t.last_rights_1, 0) <> 0 then
              t.rights_1 / nvl(t.last_rights_1, 0) - 1
             else
              0
           end

           ) * 100 as rights_1_yoy,
           t.rights_3,
           (case
             when nvl(t.last_rights_3, 0) <> 0 then
              t.rights_3 / nvl(t.last_rights_3, 0) - 1
             else
              0
           end

           ) * 100 as rights_3_yoy,
           t.rights_0,
           (case
             when nvl(t.last_rights_0, 0) <> 0 then
              t.rights_0 / nvl(t.last_rights_0, 0) - 1
             else
              0
           end

           ) * 100 as rights_0_yoy
      from tmp_result t) y
        on (a.oa_branch_id = y.oa_branch_id and a.busi_month = v_busi_month) when
     matched then
      update
         set a.rights_1     = y.rights_1,
             a.rights_1_yoy = y.rights_1_yoy,
             a.rights_3     = y.rights_3,
             a.rights_3_yoy = y.rights_3_yoy,
             a.rights_0     = y.rights_0,
             a.rights_0_yoy = y.rights_0_yoy;
  commit;

  --新增权益结构
  merge into CF_BUSIMG.T_COCKPIT_CLIENT_ANALYSE a
  using (
    with tmp as
     (select t.fund_account_id,
             b.client_type,
             c.oa_branch_id,
             sum(t.rights) as rights
        from cf_sett.t_client_sett t
        left join cf_sett.t_fund_account b
          on t.fund_account_id = b.fund_account_id
       inner join cf_busimg.t_ctp_branch_oa_rela c
          on b.branch_id = c.ctp_branch_id
       where t.busi_date between v_begin_date and v_end_date
         and c.oa_branch_id is not null
         and b.open_date between v_open_begin_date and v_open_end_date --当年开户
       group by t.fund_account_id, b.client_type, c.oa_branch_id),

    tmp_1 as
     (select t.oa_branch_id,

             sum(t.rights) / v_trade_days as total_rights,
             sum(case
                   when t.client_type = '0' then
                    t.rights
                   else
                    0
                 end) / v_trade_days as rights_0,
             sum(case
                   when t.client_type = '1' then
                    t.rights
                   else
                    0
                 end) / v_trade_days as rights_1,
             sum(case
                   when t.client_type in ('3', '4') then
                    t.rights
                   else
                    0
                 end) / v_trade_days as rights_3
        from tmp t
       group by t.oa_branch_id

      ),

    tmp_result as
     (select t.oa_branch_id,
             (case
               when nvl(a.total_rights, 0) <> 0 then
                nvl(a.rights_1, 0) / nvl(a.total_rights, 0)
               else
                0
             end) * 100 as NEW_RIGHTS_1,
             (case
               when nvl(a.total_rights, 0) <> 0 then
                nvl(a.rights_3, 0) / nvl(a.total_rights, 0)
               else
                0
             end) * 100 as NEW_RIGHTS_3,
             (case
               when nvl(a.total_rights, 0) <> 0 then
                nvl(a.rights_0, 0) / nvl(a.total_rights, 0)
               else
                0
             end) * 100 as NEW_RIGHTS_0

        from CF_BUSIMG.T_COCKPIT_CLIENT_ANALYSE t
        left join tmp_1 a
          on t.oa_branch_id = a.oa_branch_id

       where t.BUSI_MONTH = v_busi_month)
    select t.oa_branch_id, t.NEW_RIGHTS_1, t.NEW_RIGHTS_3, t.NEW_RIGHTS_0
      from tmp_result t) y
        on (a.oa_branch_id = y.oa_branch_id and a.busi_month = v_busi_month) when
     matched then
      update
         set a.NEW_RIGHTS_1 = y.NEW_RIGHTS_1,
             a.NEW_RIGHTS_3 = y.NEW_RIGHTS_3,
             a.NEW_RIGHTS_0 = y.NEW_RIGHTS_0;
  commit;

  --成交量结构
  --成交额结构
  merge into CF_BUSIMG.T_COCKPIT_CLIENT_ANALYSE a
  using (
    with tmp as
     (select t.fund_account_id,
             b.client_type,
             c.oa_branch_id,
             sum(t.done_amt) as done_amount,
             sum(t.done_sum) as done_money
        from cf_sett.t_hold_balance t
        left join cf_sett.t_fund_account b
          on t.fund_account_id = b.fund_account_id
       inner join cf_busimg.t_ctp_branch_oa_rela c
          on b.branch_id = c.ctp_branch_id
       where t.busi_date between v_begin_date and v_end_date
         and c.oa_branch_id is not null
       group by t.fund_account_id, b.client_type, c.oa_branch_id),
    tmp_last as
     (select t.fund_account_id,
             b.client_type,
             c.oa_branch_id,
             sum(t.done_amt) as done_amount,
             sum(t.done_sum) as done_money
        from cf_sett.t_hold_balance t
        left join cf_sett.t_fund_account b
          on t.fund_account_id = b.fund_account_id
       inner join cf_busimg.t_ctp_branch_oa_rela c
          on b.branch_id = c.ctp_branch_id
       where t.busi_date between v_last_begin_date and v_last_end_date
         and c.oa_branch_id is not null
       group by t.fund_account_id, b.client_type, c.oa_branch_id

      ),
    tmp_1 as
     (select t.oa_branch_id,
             sum(t.done_amount) as total_done_amount,
             sum(t.done_money) as total_done_money,
             sum(case
                   when t.client_type = '0' then
                    t.done_amount
                   else
                    0
                 end) as DONE_AMOUNT_0,
             sum(case
                   when t.client_type = '1' then
                    t.done_amount
                   else
                    0
                 end) as DONE_AMOUNT_1,
             sum(case
                   when t.client_type in ('3', '4') then
                    t.done_amount
                   else
                    0
                 end) as DONE_AMOUNT_3,
             sum(case
                   when t.client_type = '0' then
                    t.done_money
                   else
                    0
                 end) as done_money_0,
             sum(case
                   when t.client_type = '1' then
                    t.done_money
                   else
                    0
                 end) as done_money_1,
             sum(case
                   when t.client_type in ('3', '4') then
                    t.done_money
                   else
                    0
                 end) as done_money_3
        from tmp t
       group by t.oa_branch_id

      ),
    tmp_2 as
     (select t.oa_branch_id,
             sum(t.done_amount) as total_done_amount,
             sum(t.done_money) as total_done_money,
             sum(case
                   when t.client_type = '0' then
                    t.done_amount
                   else
                    0
                 end) as DONE_AMOUNT_0,
             sum(case
                   when t.client_type = '1' then
                    t.done_amount
                   else
                    0
                 end) as DONE_AMOUNT_1,
             sum(case
                   when t.client_type in ('3', '4') then
                    t.done_amount
                   else
                    0
                 end) as DONE_AMOUNT_3,
             sum(case
                   when t.client_type = '0' then
                    t.done_money
                   else
                    0
                 end) as done_money_0,
             sum(case
                   when t.client_type = '1' then
                    t.done_money
                   else
                    0
                 end) as done_money_1,
             sum(case
                   when t.client_type in ('3', '4') then
                    t.done_money
                   else
                    0
                 end) as done_money_3
        from tmp_last t
       group by t.oa_branch_id

      ),
    tmp_result as
     (select t.oa_branch_id,
             (case
               when nvl(a.total_done_amount, 0) <> 0 then
                nvl(a.DONE_AMOUNT_1, 0) / nvl(a.total_done_amount, 0)
               else
                0
             end) as DONE_AMOUNT_1,
             (case
               when nvl(a.total_done_amount, 0) <> 0 then
                nvl(a.DONE_AMOUNT_3, 0) / nvl(a.total_done_amount, 0)
               else
                0
             end) as DONE_AMOUNT_3,
             (case
               when nvl(a.total_done_amount, 0) <> 0 then
                nvl(a.DONE_AMOUNT_0, 0) / nvl(a.total_done_amount, 0)
               else
                0
             end) as DONE_AMOUNT_0,

             (case
               when nvl(b.total_done_amount, 0) <> 0 then
                nvl(b.DONE_AMOUNT_1, 0) / nvl(b.total_done_amount, 0)
               else
                0
             end) as last_DONE_AMOUNT_1,
             (case
               when nvl(b.total_done_amount, 0) <> 0 then
                nvl(b.DONE_AMOUNT_3, 0) / nvl(b.total_done_amount, 0)
               else
                0
             end) as last_DONE_AMOUNT_3,
             (case
               when nvl(b.total_done_amount, 0) <> 0 then
                nvl(b.DONE_AMOUNT_0, 0) / nvl(b.total_done_amount, 0)
               else
                0
             end) as last_DONE_AMOUNT_0,
             (case
               when nvl(a.total_done_money, 0) <> 0 then
                nvl(a.done_money_1, 0) / nvl(a.total_done_money, 0)
               else
                0
             end) as done_money_1,
             (case
               when nvl(a.total_done_money, 0) <> 0 then
                nvl(a.done_money_3, 0) / nvl(a.total_done_money, 0)
               else
                0
             end) as done_money_3,
             (case
               when nvl(a.total_done_money, 0) <> 0 then
                nvl(a.done_money_0, 0) / nvl(a.total_done_money, 0)
               else
                0
             end) as done_money_0,

             (case
               when nvl(b.total_done_money, 0) <> 0 then
                nvl(b.done_money_1, 0) / nvl(b.total_done_money, 0)
               else
                0
             end) as last_done_money_1,
             (case
               when nvl(b.total_done_money, 0) <> 0 then
                nvl(b.done_money_3, 0) / nvl(b.total_done_money, 0)
               else
                0
             end) as last_done_money_3,
             (case
               when nvl(b.total_done_money, 0) <> 0 then
                nvl(b.done_money_0, 0) / nvl(b.total_done_money, 0)
               else
                0
             end) as last_done_money_0
        from CF_BUSIMG.T_COCKPIT_CLIENT_ANALYSE t
        left join tmp_1 a
          on t.oa_branch_id = a.oa_branch_id
        left join tmp_2 b
          on t.oa_branch_id = b.oa_branch_id
       where t.BUSI_MONTH = v_busi_month)
    select t.oa_branch_id,
           t.DONE_AMOUNT_1 * 100 as DONE_AMOUNT_1,
           (case
             when nvl(t.last_done_amount_1, 0) <> 0 then
              t.DONE_AMOUNT_1 / nvl(t.last_done_amount_1, 0) - 1
             else
              0
           end

           ) * 100 as DONE_AMOUNT_1_yoy,
           t.DONE_AMOUNT_3 * 100 as DONE_AMOUNT_3,
           (case
             when nvl(t.last_DONE_AMOUNT_3, 0) <> 0 then
              t.DONE_AMOUNT_3 / nvl(t.last_DONE_AMOUNT_3, 0) - 1
             else
              0
           end

           ) * 100 as DONE_AMOUNT_3_yoy,
           t.DONE_AMOUNT_0 * 100 as DONE_AMOUNT_0,
           (case
             when nvl(t.last_DONE_AMOUNT_0, 0) <> 0 then
              t.DONE_AMOUNT_0 / nvl(t.last_DONE_AMOUNT_0, 0) - 1
             else
              0
           end

           ) * 100 as DONE_AMOUNT_0_yoy,
           t.done_money_1 * 100 as done_money_1,
           (case
             when nvl(t.last_done_money_1, 0) <> 0 then
              t.done_money_1 / nvl(t.last_done_money_1, 0) - 1
             else
              0
           end

           ) * 100 as done_money_1_yoy,
           t.done_money_3 * 100 as done_money_3,
           (case
             when nvl(t.last_done_money_3, 0) <> 0 then
              t.done_money_3 / nvl(t.last_done_money_3, 0) - 1
             else
              0
           end

           ) * 100 as done_money_3_yoy,
           t.done_money_0 * 100 as done_money_0,
           (case
             when nvl(t.last_done_money_0, 0) <> 0 then
              t.done_money_0 / nvl(t.last_done_money_0, 0) - 1
             else
              0
           end

           ) * 100 as done_money_0_yoy
      from tmp_result t) y
        on (a.oa_branch_id = y.oa_branch_id and a.busi_month = v_busi_month) when
     matched then
      update
         set a.DONE_AMOUNT_1     = y.DONE_AMOUNT_1,
             a.DONE_AMOUNT_1_YOY = y.DONE_AMOUNT_1_YOY,
             a.DONE_AMOUNT_3     = y.DONE_AMOUNT_3,
             a.DONE_AMOUNT_3_YOY = y.DONE_AMOUNT_3_YOY,
             a.DONE_AMOUNT_0     = y.DONE_AMOUNT_0,
             a.DONE_AMOUNT_0_YOY = y.DONE_AMOUNT_0_YOY,
             a.DONE_MONEY_1      = y.DONE_MONEY_1,
             a.DONE_MONEY_1_YOY  = y.DONE_MONEY_1_YOY,
             a.DONE_MONEY_3      = y.DONE_MONEY_3,
             a.DONE_MONEY_3_YOY  = y.DONE_MONEY_3_YOY,
             a.DONE_MONEY_0      = y.DONE_MONEY_0,
             a.DONE_MONEY_0_YOY  = y.DONE_MONEY_0_YOY;
  commit;
  --------------------------------------------------------------------------------------------------------------
  --经纪业务收入结构
  --与一期逻辑一样取，取驾驶舱-业务板块-经纪业务净收入总计一致
  --20240626
  /*
  经纪业务收入=留存手续费收入+利息收入+交易所减收
  留存手续费收入=手续费-上交手续费(资金对账表)
  利息收入=驾驶舱一期业务板块里的息差收入
  交易所减收=内核表-投资者交易所返还计算-二次开发(业务报表-薪酬报表)
  */
  merge into CF_BUSIMG.T_COCKPIT_CLIENT_ANALYSE a
  using (
    with tmp as
     (
      --当月数据
      select t.fund_account_id,
              b.client_type,
              c.oa_branch_id,
              t.feature_income_total
        from cf_busimg.t_cockpit_00174 t
        left join cf_sett.t_fund_account b
          on t.fund_account_id = b.fund_account_id
       inner join cf_busimg.t_ctp_branch_oa_rela c
          on b.branch_id = c.ctp_branch_id
       where t.busi_month = v_busi_month
         and c.oa_branch_id is not null),
    tmp_last as
     (
      --同比月份数据
      select t.fund_account_id,
              b.client_type,
              c.oa_branch_id,
              t.feature_income_total
        from cf_busimg.t_cockpit_00174 t
        left join cf_sett.t_fund_account b
          on t.fund_account_id = b.fund_account_id
       inner join cf_busimg.t_ctp_branch_oa_rela c
          on b.branch_id = c.ctp_branch_id
       where t.busi_month = v_last_busi_month
         and c.oa_branch_id is not null

      ),
    tmp_1 as
     (
      --当月各客户类型数据
      select t.oa_branch_id,
              sum(t.feature_income_total) as total_qh_income,
              sum(case
                    when t.client_type = '0' then
                     t.feature_income_total
                    else
                     0
                  end) as qh_income_0,
              sum(case
                    when t.client_type = '1' then
                     t.feature_income_total
                    else
                     0
                  end) as qh_income_1,
              sum(case
                    when t.client_type in ('3', '4') then
                     t.feature_income_total
                    else
                     0
                  end) as qh_income_3
        from tmp t
       group by t.oa_branch_id),
    tmp_2 as
     (
      --同比月份
      select t.oa_branch_id,
              sum(t.feature_income_total) as total_qh_income,
              sum(case
                    when t.client_type = '0' then
                     t.feature_income_total
                    else
                     0
                  end) as qh_income_0,
              sum(case
                    when t.client_type = '1' then
                     t.feature_income_total
                    else
                     0
                  end) as qh_income_1,
              sum(case
                    when t.client_type in ('3', '4') then
                     t.feature_income_total
                    else
                     0
                  end) as qh_income_3
        from tmp_last t
       group by t.oa_branch_id),
    tmp_result as
     (select t.oa_branch_id,
             (case
               when nvl(a.total_qh_income, 0) <> 0 then
                nvl(a.qh_income_0, 0) / nvl(a.total_qh_income, 0)
               else
                0
             end) as qh_income_0,
             (case
               when nvl(a.total_qh_income, 0) <> 0 then
                nvl(a.qh_income_1, 0) / nvl(a.total_qh_income, 0)
               else
                0
             end) as QH_INCOME_1,
             (case
               when nvl(a.total_qh_income, 0) <> 0 then
                nvl(a.qh_income_3, 0) / nvl(a.total_qh_income, 0)
               else
                0
             end) as QH_INCOME_3,
             (case
               when nvl(b.total_qh_income, 0) <> 0 then
                nvl(b.qh_income_0, 0) / nvl(b.total_qh_income, 0)
               else
                0
             end) as last_qh_income_0,
             (case
               when nvl(b.total_qh_income, 0) <> 0 then
                nvl(b.qh_income_1, 0) / nvl(b.total_qh_income, 0)
               else
                0
             end) as last_qh_income_1,
             (case
               when nvl(b.total_qh_income, 0) <> 0 then
                nvl(b.qh_income_3, 0) / nvl(b.total_qh_income, 0)
               else
                0
             end) as last_qh_income_3
        from CF_BUSIMG.T_COCKPIT_CLIENT_ANALYSE t
        left join tmp_1 a
          on t.oa_branch_id = a.oa_branch_id
        left join tmp_2 b
          on t.oa_branch_id = b.oa_branch_id
       where t.BUSI_MONTH = v_busi_month)
    select t.oa_branch_id,
           t.QH_INCOME_1 * 100 as QH_INCOME_1,
           (case
             when nvl(t.last_QH_INCOME_1, 0) <> 0 then
              t.QH_INCOME_1 / nvl(t.last_QH_INCOME_1, 0) - 1
             else
              0
           end) * 100 as QH_INCOME_1_YOY,
           t.QH_INCOME_3 * 100 as QH_INCOME_3,
           (case
             when nvl(t.last_QH_INCOME_3, 0) <> 0 then
              t.QH_INCOME_3 / nvl(t.last_QH_INCOME_3, 0) - 1
             else
              0
           end) * 100 as QH_INCOME_3_YOY,
           t.QH_INCOME_0 * 100 as QH_INCOME_0,
           (case
             when nvl(t.last_QH_INCOME_0, 0) <> 0 then
              t.QH_INCOME_0 / nvl(t.last_QH_INCOME_0, 0) - 1
             else
              0
           end) * 100 as QH_INCOME_0_YOY

      from tmp_result t

    ) y
        on (a.oa_branch_id = y.oa_branch_id and a.busi_month = v_busi_month) when
     matched then
      update
         set a.QH_INCOME_1     = y.QH_INCOME_1,
             a.QH_INCOME_1_YOY = y.QH_INCOME_1_YOY,
             a.QH_INCOME_3     = y.QH_INCOME_3,
             a.QH_INCOME_3_YOY = y.QH_INCOME_3_YOY,
             a.QH_INCOME_0     = y.QH_INCOME_0,
             a.QH_INCOME_0_YOY = y.QH_INCOME_0_YOY;
  commit;
  -------------------------------------------------------------------------------------------------------------------

  --增量权益分析-当年新开客户权益(万元)
  merge into CF_BUSIMG.T_COCKPIT_CLIENT_ANALYSE a
  using (
    with tmp as
     (select t.fund_account_id, c.oa_branch_id, sum(t.rights) as rights
        from cf_sett.t_client_sett t
        left join cf_sett.t_fund_account b
          on t.fund_account_id = b.fund_account_id
       inner join cf_busimg.t_ctp_branch_oa_rela c
          on b.branch_id = c.ctp_branch_id
       where t.busi_date between v_open_begin_date and v_end_date
         and c.oa_branch_id is not null
         and b.open_date between v_open_begin_date and v_open_end_date --当年开户
       group by t.fund_account_id, c.oa_branch_id

      )
    select t.oa_branch_id,
           sum(t.rights) / v_busi_year_trade_days as RIGHTS_ADD_NEW
      from tmp t
     group by t.oa_branch_id

    ) y
        on (a.oa_branch_id = y.oa_branch_id and a.busi_month = v_busi_month) when
     matched then
      update set a.RIGHTS_ADD_NEW = y.RIGHTS_ADD_NEW / 10000;
  commit;

  --增量权益分析-老客户权益变化值(万元)
  /*
  排除当年新增客户日均权益的所有客户的日群权益，减掉去年全年客户日均权益
  */
  merge into CF_BUSIMG.T_COCKPIT_CLIENT_ANALYSE a
  using (
    with tmp as
     (select t.fund_account_id, c.oa_branch_id, sum(t.rights) as rights
        from cf_sett.t_client_sett t
        left join cf_sett.t_fund_account b
          on t.fund_account_id = b.fund_account_id
       inner join cf_busimg.t_ctp_branch_oa_rela c
          on b.branch_id = c.ctp_branch_id
       where t.busi_date between v_open_begin_date and v_end_date
         and c.oa_branch_id is not null
         and b.open_date < v_open_begin_date --排除掉当年新增客户权益
       group by t.fund_account_id, c.oa_branch_id

      ),
    tmp_last as
     (
      --去年全年日均权益
      select t.fund_account_id, c.oa_branch_id, sum(t.rights) as rights
        from cf_sett.t_client_sett t
        left join cf_sett.t_fund_account b
          on t.fund_account_id = b.fund_account_id
       inner join cf_busimg.t_ctp_branch_oa_rela c
          on b.branch_id = c.ctp_branch_id
       where t.busi_date between v_last_open_begin_date and
             v_last_open_end_date
         and c.oa_branch_id is not null
       group by t.fund_account_id, c.oa_branch_id),
    tmp_result1 as
     (select t.oa_branch_id,
             sum(t.rights) / v_busi_year_trade_days as avg_rights
        from tmp t
       group by t.oa_branch_id),
    tmp_result2 as
     (select t.oa_branch_id,
             sum(t.rights) / v_last_year_trade_days as avg_rights
        from tmp_last t
       group by t.oa_branch_id)
    select t.oa_branch_id,
           nvl(a.avg_rights, 0) - nvl(b.avg_rights, 0) as RIGHTS_ADD_OLD
      from CF_BUSIMG.T_COCKPIT_CLIENT_ANALYSE t
      left join tmp_result1 a
        on t.oa_branch_id = a.oa_branch_id
      left join tmp_result2 b
        on t.oa_branch_id = b.oa_branch_id

     where t.BUSI_MONTH = v_busi_month) y on (a.oa_branch_id = y.oa_branch_id and a.busi_month = v_busi_month) when matched then
      update set a.RIGHTS_ADD_OLD = y.RIGHTS_ADD_OLD / 10000;
  commit;

  --客户年龄结构
  --有效客户年龄结构
  merge into CF_BUSIMG.T_COCKPIT_CLIENT_ANALYSE a
  using (
    with tmp as
     (
      --有交易
      select t.fund_account_id, c.oa_branch_id
        from cf_sett.t_hold_balance t
        left join cf_sett.t_fund_account b
          on t.fund_account_id = b.fund_account_id
       inner join cf_busimg.t_ctp_branch_oa_rela c
          on b.branch_id = c.ctp_branch_id
       where t.busi_date between v_begin_date and v_end_date
         and c.oa_branch_id is not null

       group by t.fund_account_id, c.oa_branch_id),
    tmp_1 as
     (select t.fund_account_id,
             t.oa_branch_id,
             CF_BUSIMG.f_get_birth(b.id_no) as birth
        from tmp t
        left join cf_sett.t_fund_account b
          on t.fund_account_id = b.fund_account_id)
    select t.oa_branch_id,
           sum(case
                 when t.birth <= '19691231' then
                  1
                 else
                  0
               end) as AGE_60, --60后
           sum(case
                 when (t.birth >= '19700101' and t.birth <= '19791231') then
                  1
                 else
                  0
               end) as AGE_70, --70后
           sum(case
                 when (t.birth >= '19800101' and t.birth <= '19891231') then
                  1
                 else
                  0
               end) as AGE_80, --80后
           sum(case
                 when (t.birth >= '19900101' and t.birth <= '19991231') then
                  1
                 else
                  0
               end) as AGE_90, --90后
           sum(case
                 when (t.birth >= '20000101') then
                  1
                 else
                  0
               end) as AGE_00 --00后
      from tmp_1 t
     group by t.oa_branch_id

    ) y
        on (a.oa_branch_id = y.oa_branch_id and a.busi_month = v_busi_month) when
     matched then
      update
         set a.AGE_60 = y.AGE_60,
             a.AGE_70 = y.AGE_70,
             a.AGE_80 = y.AGE_80,
             a.AGE_90 = y.AGE_90,
             a.AGE_00 = y.AGE_00;
  commit;

  --客户年龄结构
  --新增有效客户年龄结构
  --新开户
  merge into CF_BUSIMG.T_COCKPIT_CLIENT_ANALYSE a
  using (
    with tmp as
     (
      --有交易
      select t.fund_account_id, c.oa_branch_id
        from cf_sett.t_hold_balance t
        left join cf_sett.t_fund_account b
          on t.fund_account_id = b.fund_account_id
       inner join cf_busimg.t_ctp_branch_oa_rela c
          on b.branch_id = c.ctp_branch_id
       where t.busi_date between v_begin_date and v_end_date
         and b.open_date between v_begin_date and v_end_date
         and c.oa_branch_id is not null

       group by t.fund_account_id, c.oa_branch_id),
    tmp_1 as
     (select t.fund_account_id,
             t.oa_branch_id,
             CF_BUSIMG.f_get_birth(b.id_no) as birth
        from tmp t
        left join cf_sett.t_fund_account b
          on t.fund_account_id = b.fund_account_id)
    select t.oa_branch_id,
           sum(case
                 when t.birth <= '19691231' then
                  1
                 else
                  0
               end) as AGE_60_NEW, --60后
           sum(case
                 when (t.birth >= '19700101' and t.birth <= '19791231') then
                  1
                 else
                  0
               end) as AGE_70_NEW, --70后
           sum(case
                 when (t.birth >= '19800101' and t.birth <= '19891231') then
                  1
                 else
                  0
               end) as AGE_80_NEW, --80后
           sum(case
                 when (t.birth >= '19900101' and t.birth <= '19991231') then
                  1
                 else
                  0
               end) as AGE_90_NEW, --90后
           sum(case
                 when (t.birth >= '20000101') then
                  1
                 else
                  0
               end) as AGE_00_NEW --00后
      from tmp_1 t
     group by t.oa_branch_id

    ) y
        on (a.oa_branch_id = y.oa_branch_id and a.busi_month = v_busi_month) when
     matched then
      update
         set a.AGE_60_NEW = y.AGE_60_NEW,
             a.AGE_70_NEW = y.AGE_70_NEW,
             a.AGE_80_NEW = y.AGE_80_NEW,
             a.AGE_90_NEW = y.AGE_90_NEW,
             a.AGE_00_NEW = y.AGE_00_NEW;
  commit;


  --计算客户的净贡献
  execute immediate 'truncate table CF_BUSIMG.tmp_COCKPIT_CLIENT_jgx';
  INSERT INTO CF_BUSIMG.tmp_COCKPIT_CLIENT_jgx
    (CLIENT_ID, JGX)
    select a.investor_id as client_id,

           sum(a.subsistence_fee_amt *
               decode(1, 2, a2.pct, 3, a2.data_pct, 1)) +
           round(sum(a.int_amt * decode(1, 2, a2.pct, 3, a2.data_pct, 1)),
                 2) + round(sum(a.exchangeret_amt *
                                decode(1, 2, a2.pct, 3, a2.data_pct, 1)),
                            2) +
           sum(a.oth_amt * decode(1, 2, a2.pct, 3, a2.data_pct, 1)) -
           round(sum(a.fd_amt * decode(1, 2, a2.pct, 3, a2.data_pct, 1)), 2) -
           round(sum(a.i_int_amt * decode(1, 2, a2.pct, 3, a2.data_pct, 1)),
                 2) -
           sum(a.i_exchangeret_amt *
               decode(1, 2, a2.pct, 3, a2.data_pct, 1)) -
           sum(a.broker_amt * decode(1, 2, a2.pct, 3, a2.data_pct, 1)) -
           sum(a.soft_amt * decode(1, 2, a2.pct, 3, a2.data_pct, 1)) -
           sum(a.i_oth_amt * decode(1, 2, a2.pct, 3, a2.data_pct, 1)) -
           round(sum(a.broker_int_amt *
                     decode(1, 2, a2.pct, 3, a2.data_pct, 1)),
                 2) - round(sum(a.broker_eret_amt *
                                decode(1, 2, a2.pct, 3, a2.data_pct, 1)),
                            2) -
           round(sum(nvl(a2.ib_amt,
                         a.STAFF_AMT *
                         decode(1, 2, a2.pct, 3, a2.data_pct, 1))),
                 2) - round(sum(nvl(a2.STAFF_INT_AMT,
                                    a.STAFF_INT_AMT *
                                    decode(1, 2, a2.pct, 3, a2.data_pct, 1))),
                            2) -
           round(sum(a.staff_eret_amt *
                     decode(1, 2, a2.pct, 3, a2.data_pct, 1)),
                 2) as jgx
      from CTP63.T_DS_ADM_INVESTOR_VALUE a
      JOIN CTP63.T_DS_ADM_BROKERDATA_DETAIL a2
        on a.date_dt = a2.tx_dt
       and a.investor_id = a2.investor_id
       and a2.rec_freq = 'M'
     where replace(a.date_dt, '-', '') between v_jgx_busi_date and
           v_end_date

     group by a.investor_id;
  commit;
  -- 千万工程-有效客户数,
  -- 千万工程-日均权益, (亿元)
  -- 千万工程-成交量, (万手)
  -- 千万工程-成交额, (亿元)
  -- 千万工程-净贡献(万元)
  merge into CF_BUSIMG.T_COCKPIT_CLIENT_ANALYSE a
  using (
    with tmp as
     (
      --千万工程客户号
      select b.oa_branch_id, a.fund_account_id
        from CF_CRMMG.T_HIS_LABEL_CLIENT t
       inner join cf_crmmg.t_label c
          on t.label_id = c.label_id
         and c.label_id = 'BQ4909' --千万工程标签
       inner join cf_sett.t_fund_account a
          on t.client_id = a.fund_account_id
       inner join cf_busimg.t_ctp_branch_oa_rela b
          on a.branch_id = b.ctp_branch_id
       where b.oa_branch_id is not null
         AND t.months = v_busi_month

      ),
    tmp_0 as
     (select a.oa_branch_id, count(1) as client_num
        from tmp a
       group by a.oa_branch_id),
    tmp_1 as
     (
      --日均权益
      select /*+leading(t) use_hash(t,a)*/
       a.oa_branch_id,

        sum(t.rights) / v_trade_days as avg_rights
        from cf_sett.t_client_sett t
       inner join tmp a
          on t.fund_account_id = a.fund_account_id
       where t.busi_date between v_begin_date and v_end_date
       group by a.oa_branch_id),
    tmp_2 as
     (
      --成交量
      --成交额
      select /*+leading(t) use_hash(t,a)*/
       a.oa_branch_id,
        sum(t.done_amt) as done_amount,
        sum(t.done_sum) as done_sum
        from cf_sett.t_hold_balance t
       inner join tmp a
          on t.fund_account_id = a.fund_account_id
       where t.busi_date between v_begin_date and v_end_date
       group by a.oa_branch_id),
    tmp_3 as
     (
      --净贡献
      select /*+leading(t) use_hash(t,a)*/
       a.oa_branch_id, sum(t.jgx) as jgx
        from CF_BUSIMG.tmp_COCKPIT_CLIENT_jgx t
       inner join tmp a
          on t.CLIENT_ID = a.fund_account_id
       group by a.oa_branch_id)
    select t.oa_branch_id,
           nvl(a.client_num, 0) as QW_CLIENT_NUM,
           nvl(b.avg_rights, 0) as QW_AVG_RIGHTS,
           nvl(c.done_amount, 0) as QW_DONE_AMOUNT,
           nvl(c.done_sum, 0) as QW_DONE_MONEY,
           nvl(d.jgx, 0) as QW_NET_CONTRIBUTION
      from CF_BUSIMG.T_COCKPIT_CLIENT_ANALYSE t
      left join tmp_0 a
        on t.oa_branch_id = a.oa_branch_id
      left join tmp_1 b
        on t.oa_branch_id = b.oa_branch_id
      left join tmp_2 c
        on t.oa_branch_id = c.oa_branch_id
      left join tmp_3 d
        on t.oa_branch_id = d.oa_branch_id

     where t.busi_month = v_busi_month) y on (a.oa_branch_id = y.oa_branch_id and a.busi_month = v_busi_month) when matched then
      update
         set a.QW_CLIENT_NUM       = y.QW_CLIENT_NUM,
             a.QW_AVG_RIGHTS       = y.QW_AVG_RIGHTS / 100000000,
             a.QW_DONE_AMOUNT      = y.QW_DONE_AMOUNT / 10000,
             a.QW_DONE_MONEY       = y.QW_DONE_MONEY / 100000000,
             a.QW_NET_CONTRIBUTION = y.QW_NET_CONTRIBUTION / 10000;
  commit;


  -- 高频客户-有效客户数,
  -- 高频客户-日均权益,
  -- 高频客户-成交量,
  -- 高频客户-成交额,
  -- 高频客户-净贡献
  merge into CF_BUSIMG.T_COCKPIT_CLIENT_ANALYSE a
  using (
    with tmp as
     (
      --高频客户客户号
      select b.oa_branch_id, a.fund_account_id
        from CF_CRMMG.T_HIS_LABEL_CLIENT t
       inner join cf_crmmg.t_label c
          on t.label_id = c.label_id
         and c.label_id = 'BQ4908' --高频客户
       inner join cf_sett.t_fund_account a
          on t.client_id = a.fund_account_id
       inner join cf_busimg.t_ctp_branch_oa_rela b
          on a.branch_id = b.ctp_branch_id
       where b.oa_branch_id is not null
         AND t.months = v_busi_month

      ),
    tmp_0 as
     (
      --有效客户数
      select a.oa_branch_id, count(1) as client_num
        from tmp a
       group by a.oa_branch_id),
    tmp_1 as
     (
      --日均权益
      select /*+leading(t) use_hash(t,a)*/
       a.oa_branch_id,

        sum(t.rights) / v_trade_days as avg_rights
        from cf_sett.t_client_sett t
       inner join tmp a
          on t.fund_account_id = a.fund_account_id
       where t.busi_date between v_begin_date and v_end_date
       group by a.oa_branch_id),
    tmp_2 as
     (
      --成交量
      --成交额
      select /*+leading(t) use_hash(t,a)*/
       a.oa_branch_id,
        sum(t.done_amt) as done_amount,
        sum(t.done_sum) as done_sum
        from cf_sett.t_hold_balance t
       inner join tmp a
          on t.fund_account_id = a.fund_account_id
       where t.busi_date between v_begin_date and v_end_date
       group by a.oa_branch_id),
    tmp_3 as
     (
      --净贡献
      select /*+leading(t) use_hash(t,a)*/
       a.oa_branch_id, sum(t.jgx) as jgx
        from CF_BUSIMG.tmp_COCKPIT_CLIENT_jgx t
       inner join tmp a
          on t.CLIENT_ID = a.fund_account_id
       group by a.oa_branch_id)
    select t.oa_branch_id,
           nvl(a.client_num, 0) as GP_CLIENT_NUM,
           nvl(b.avg_rights, 0) as GP_AVG_RIGHTS,
           nvl(c.done_amount, 0) as GP_DONE_AMOUNT,
           nvl(c.done_sum, 0) as GP_DONE_MONEY,
           nvl(d.jgx, 0) as GP_NET_CONTRIBUTION
      from CF_BUSIMG.T_COCKPIT_CLIENT_ANALYSE t
      left join tmp_0 a
        on t.oa_branch_id = a.oa_branch_id
      left join tmp_1 b
        on t.oa_branch_id = b.oa_branch_id
      left join tmp_2 c
        on t.oa_branch_id = c.oa_branch_id
      left join tmp_3 d
        on t.oa_branch_id = d.oa_branch_id

     where t.busi_month = v_busi_month) y on (a.oa_branch_id = y.oa_branch_id and a.busi_month = v_busi_month) when matched then
      update
         set a.GP_CLIENT_NUM       = y.GP_CLIENT_NUM,
             a.GP_AVG_RIGHTS       = y.GP_AVG_RIGHTS / 100000000,
             a.GP_DONE_AMOUNT      = y.GP_DONE_AMOUNT / 10000,
             a.GP_DONE_MONEY       = y.GP_DONE_MONEY / 100000000,
             a.GP_NET_CONTRIBUTION = y.GP_NET_CONTRIBUTION / 10000;
  commit;


  -- 高净值客户-有效客户数,
  -- 高净值客户-日均权益,
  -- 高净值客户-成交量,
  -- 高净值客户-成交额,
  -- 高净值客户-净贡献
  merge into CF_BUSIMG.T_COCKPIT_CLIENT_ANALYSE a
  using (
    with tmp as
     (
      --高净值客户客户号
      select b.oa_branch_id, a.fund_account_id
        from CF_CRMMG.T_HIS_LABEL_CLIENT t
       inner join cf_crmmg.t_label c
          on t.label_id = c.label_id
         and c.label_id = 'BQ4910' --高净值客户
       inner join cf_sett.t_fund_account a
          on t.client_id = a.fund_account_id
       inner join cf_busimg.t_ctp_branch_oa_rela b
          on a.branch_id = b.ctp_branch_id
       where b.oa_branch_id is not null
         AND t.months = v_busi_month

      ),
    tmp_0 as
     (
      --有效客户数
      select a.oa_branch_id, count(1) as client_num
        from tmp a
       group by a.oa_branch_id),
    tmp_1 as
     (
      --日均权益
      select /*+leading(t) use_hash(t,a)*/
       a.oa_branch_id,

        sum(t.rights) / v_trade_days as avg_rights
        from cf_sett.t_client_sett t
       inner join tmp a
          on t.fund_account_id = a.fund_account_id
       where t.busi_date between v_begin_date and v_end_date
       group by a.oa_branch_id),
    tmp_2 as
     (
      --成交量
      --成交额
      select /*+leading(t) use_hash(t,a)*/
       a.oa_branch_id,
        sum(t.done_amt) as done_amount,
        sum(t.done_sum) as done_sum
        from cf_sett.t_hold_balance t
       inner join tmp a
          on t.fund_account_id = a.fund_account_id
       where t.busi_date between v_begin_date and v_end_date
       group by a.oa_branch_id),
    tmp_3 as
     (
      --净贡献
      select /*+leading(t) use_hash(t,a)*/
       a.oa_branch_id, sum(t.jgx) as jgx
        from CF_BUSIMG.tmp_COCKPIT_CLIENT_jgx t
       inner join tmp a
          on t.CLIENT_ID = a.fund_account_id
       group by a.oa_branch_id)
    select t.oa_branch_id,
           nvl(a.client_num, 0) as GJZ_CLIENT_NUM,
           nvl(b.avg_rights, 0) as GJZ_AVG_RIGHTS,
           nvl(c.done_amount, 0) as GJZ_DONE_AMOUNT,
           nvl(c.done_sum, 0) as GJZ_DONE_MONEY,
           nvl(d.jgx, 0) as GJZ_NET_CONTRIBUTION
      from CF_BUSIMG.T_COCKPIT_CLIENT_ANALYSE t
      left join tmp_0 a
        on t.oa_branch_id = a.oa_branch_id
      left join tmp_1 b
        on t.oa_branch_id = b.oa_branch_id
      left join tmp_2 c
        on t.oa_branch_id = c.oa_branch_id
      left join tmp_3 d
        on t.oa_branch_id = d.oa_branch_id

     where t.busi_month = v_busi_month) y on (a.oa_branch_id = y.oa_branch_id and a.busi_month = v_busi_month) when matched then
      update
         set a.GJZ_CLIENT_NUM       = y.GJZ_CLIENT_NUM,
             a.GJZ_AVG_RIGHTS       = y.GJZ_AVG_RIGHTS / 100000000,
             a.GJZ_DONE_AMOUNT      = y.GJZ_DONE_AMOUNT / 10000,
             a.GJZ_DONE_MONEY       = y.GJZ_DONE_MONEY / 100000000,
             a.GJZ_NET_CONTRIBUTION = y.GJZ_NET_CONTRIBUTION / 10000;
  commit;



  -- 套保客户-有效客户数,
  -- 套保客户-日均权益,
  -- 套保客户-成交量,
  -- 套保客户-成交额,
  -- 套保客户-净贡献
  merge into CF_BUSIMG.T_COCKPIT_CLIENT_ANALYSE a
  using (
    with tmp as
     (
      --套保客户客户号
      select b.oa_branch_id, a.fund_account_id
        from CF_CRMMG.T_HIS_LABEL_CLIENT t
       inner join cf_crmmg.t_label c
          on t.label_id = c.label_id
         and c.label_id = 'BQ4907' --套保客户
       inner join cf_sett.t_fund_account a
          on t.client_id = a.fund_account_id
       inner join cf_busimg.t_ctp_branch_oa_rela b
          on a.branch_id = b.ctp_branch_id
       where b.oa_branch_id is not null
         AND t.months = v_busi_month

      ),
    tmp_0 as
     (
      --有效客户数
      select a.oa_branch_id, count(1) as client_num
        from tmp a
       group by a.oa_branch_id),
    tmp_1 as
     (
      --日均权益
      select /*+leading(t) use_hash(t,a)*/
       a.oa_branch_id,

        sum(t.rights) / v_trade_days as avg_rights
        from cf_sett.t_client_sett t
       inner join tmp a
          on t.fund_account_id = a.fund_account_id
       where t.busi_date between v_begin_date and v_end_date
       group by a.oa_branch_id),
    tmp_2 as
     (
      --成交量
      --成交额
      select /*+leading(t) use_hash(t,a)*/
       a.oa_branch_id,
        sum(t.done_amt) as done_amount,
        sum(t.done_sum) as done_sum
        from cf_sett.t_hold_balance t
       inner join tmp a
          on t.fund_account_id = a.fund_account_id
       where t.busi_date between v_begin_date and v_end_date
       group by a.oa_branch_id),
    tmp_3 as
     (
      --净贡献
      select /*+leading(t) use_hash(t,a)*/
       a.oa_branch_id, sum(t.jgx) as jgx
        from CF_BUSIMG.tmp_COCKPIT_CLIENT_jgx t
       inner join tmp a
          on t.CLIENT_ID = a.fund_account_id
       group by a.oa_branch_id)
    select t.oa_branch_id,
           nvl(a.client_num, 0) as TB_CLIENT_NUM,
           nvl(b.avg_rights, 0) as TB_AVG_RIGHTS,
           nvl(c.done_amount, 0) as TB_DONE_AMOUNT,
           nvl(c.done_sum, 0) as TB_DONE_MONEY,
           nvl(d.jgx, 0) as TB_NET_CONTRIBUTION
      from CF_BUSIMG.T_COCKPIT_CLIENT_ANALYSE t
      left join tmp_0 a
        on t.oa_branch_id = a.oa_branch_id
      left join tmp_1 b
        on t.oa_branch_id = b.oa_branch_id
      left join tmp_2 c
        on t.oa_branch_id = c.oa_branch_id
      left join tmp_3 d
        on t.oa_branch_id = d.oa_branch_id

     where t.busi_month = v_busi_month) y on (a.oa_branch_id = y.oa_branch_id and a.busi_month = v_busi_month) when matched then
      update
         set a.tb_CLIENT_NUM       = y.tb_CLIENT_NUM,
             a.tb_AVG_RIGHTS       = y.tb_AVG_RIGHTS / 100000000,
             a.tb_DONE_AMOUNT      = y.tb_DONE_AMOUNT / 10000,
             a.tb_DONE_MONEY       = y.tb_DONE_MONEY / 100000000,
             a.tb_NET_CONTRIBUTION = y.tb_NET_CONTRIBUTION / 10000;
  commit;
  O_RETURN_CODE := 0;
  O_RETURN_MSG  := '执行成功';
EXCEPTION
  when v_userException then
    ROLLBACK;
    v_error_msg  := o_return_msg;
    v_error_code := o_return_code;
    wolf.p_error_log('admin', -- '操作人';
                     v_op_object, -- '操作对象';
                     v_error_code, --'错误代码';
                     v_error_msg, -- '错误信息';
                     '',
                     '',
                     o_return_msg, --返回信息
                     o_return_code --返回值 0 成功必须返回；-1 失败
                     );

  WHEN OTHERS THEN
    O_RETURN_CODE := SQLCODE;
    O_RETURN_MSG  := O_RETURN_MSG || SQLERRM;
    V_ERROR_CODE  := SQLCODE;
    V_ERROR_MSG   := SQLERRM;
    WOLF.P_ERROR_LOG('admin',
                     V_OP_OBJECT,
                     V_ERROR_CODE,
                     V_ERROR_MSG,
                     '',
                     '',
                     O_RETURN_MSG,
                     O_RETURN_CODE);
    COMMIT;
END;
