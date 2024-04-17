create or replace procedure cf_busimg.P_COCKPIT_ANAL_LINE_TOP_DATA(i_busi_date   in varchar2,
                                                                   o_return_code out integer,
                                                                   o_return_msg  out varchar2) is
  --========================================================================================================================
  --系统名称:生成驾驶舱数据
  --模块名称:
  --模块编号:
  --模块描述： CF_BUSIMG.T_COCKPIT_CLIENT_ANAL_LINE_TOP  客户分析落地表(客户分析-业务条线-)
  --开发人员:zhongying.zhang
  --目前版本:
  --创建时间:20230408
  --版    权:
  --修改历史:

  --========================================================================================================================
  v_op_object  VARCHAR2(50) DEFAULT 'P_COCKPIT_ANALYSE_TOP_DATA'; -- '操作对象';
  v_error_msg  VARCHAR2(200); --返回信息
  v_error_code INTEGER;
  v_userException EXCEPTION;

  v_busi_week  integer; --保存传入日期为本年第几周
  v_begin_date varchar2(8); --本周开始日期，星期一
  v_end_date   varchar2(8); --本周结束日期 ，星期日
  v_whoweek    char(10);
  v_busi_year  varchar2(4); --年份
  v_rank_no    number; --取前几名
begin
  ----传入的周期取得年
  v_busi_year := substr(I_BUSI_DATE, 1, 4);
  v_rank_no   := 9;

  --取得该日期是本年第几周
  v_busi_week := to_number(to_char(to_date(I_BUSI_DATE, 'YYYY-MM-DD'), 'iw'));

  --取得当天日期是星期几
  select to_char(to_date(I_BUSI_DATE, 'YYYY-MM-DD'),
                 'Day',
                 'NLS_DATE_LANGUAGE = ''SIMPLIFIED CHINESE''')
    into v_whoweek
    from dual;

  --获取本周的第一个交易日
  case TRIM(v_whoweek) --已经限制传入日期一定是交易日了
    when '星期一' then
      v_begin_date := to_char(to_date(I_BUSI_DATE, 'YYYY-MM-DD') - 0,
                              'YYYYMMDD');
      v_end_date   := to_char(to_date(I_BUSI_DATE, 'YYYY-MM-DD') + 6,
                              'YYYYMMDD');
    when '星期二' then
      v_begin_date := to_char(to_date(I_BUSI_DATE, 'YYYY-MM-DD') - 1,
                              'YYYYMMDD');
      v_end_date   := to_char(to_date(I_BUSI_DATE, 'YYYY-MM-DD') + 5,
                              'YYYYMMDD');
    when '星期三' then
      v_begin_date := to_char(to_date(I_BUSI_DATE, 'YYYY-MM-DD') - 2,
                              'YYYYMMDD');
      v_end_date   := to_char(to_date(I_BUSI_DATE, 'YYYY-MM-DD') + 4,
                              'YYYYMMDD');
    when '星期四' then
      v_begin_date := to_char(to_date(I_BUSI_DATE, 'YYYY-MM-DD') - 3,
                              'YYYYMMDD');
      v_end_date   := to_char(to_date(I_BUSI_DATE, 'YYYY-MM-DD') + 3,
                              'YYYYMMDD');
    when '星期五' then
      v_begin_date := to_char(to_date(I_BUSI_DATE, 'YYYY-MM-DD') - 4,
                              'YYYYMMDD');
      v_end_date   := to_char(to_date(I_BUSI_DATE, 'YYYY-MM-DD') + 2,
                              'YYYYMMDD');
    when '星期六' then
      v_begin_date := to_char(to_date(I_BUSI_DATE, 'YYYY-MM-DD') - 5,
                              'YYYYMMDD');
      v_end_date   := to_char(to_date(I_BUSI_DATE, 'YYYY-MM-DD') + 1,
                              'YYYYMMDD');
    when '星期日' then
      v_begin_date := to_char(to_date(I_BUSI_DATE, 'YYYY-MM-DD') - 6,
                              'YYYYMMDD');
      v_end_date   := to_char(to_date(I_BUSI_DATE, 'YYYY-MM-DD') + 0,
                              'YYYYMMDD');
    ELSE
      v_begin_date := to_char(to_date(I_BUSI_DATE, 'YYYY-MM-DD') - 0,
                              'YYYYMMDD');
      v_end_date   := to_char(to_date(I_BUSI_DATE, 'YYYY-MM-DD') + 6,
                              'YYYYMMDD');
  END CASE;

  delete from CF_BUSIMG.T_COCKPIT_CLIENT_ANAL_LINE_TOP t
   where t.busi_week = v_busi_week
     and t.busi_year = v_busi_year;
  commit;

  --指标类型(1:入金前9名，2：出金前9名，3：盈利前9名，4：亏损前9名，5：收入贡献前9名，6：成交量前9名，7：成交额前9名)
  --1:入金前9名
  INSERT INTO CF_BUSIMG.T_COCKPIT_CLIENT_ANAL_LINE_TOP
    (BUSI_WEEK,
     BUSI_YEAR,
     business_line_id,
     BEGIN_DATE,
     END_DATE,
     CLIENT_ID,
     CLIENT_NAME,
     RANK_NO,
     INDEX_TYPE)
    with tmp as
     (select t.fund_account_id,
             b.client_name,
             d.business_line_id,
             sum(t.fund_in) as fund_in
        from cf_sett.t_client_sett t
        left join cf_sett.t_fund_account b
          on t.fund_account_id = b.fund_account_id
       inner join cf_busimg.t_ctp_branch_oa_rela c
          on b.branch_id = c.ctp_branch_id
       inner join CF_BUSIMG.T_OA_BRANCH d
          on c.oa_branch_id = d.departmentid
       where t.busi_date between v_begin_date and v_end_date
         and c.oa_branch_id is not null
         AND D.BUSINESS_LINE_ID IS NOT NULL
       group by t.fund_account_id, b.client_name, D.BUSINESS_LINE_ID),
    tmp_rank as
     (select t.fund_account_id,
             t.client_name,
             t.business_line_id,
             t.fund_in,
             RANK() OVER(partition by t.business_line_id ORDER BY t.fund_in DESC) as rank_no
        from tmp t),
    tmp_result as
     (select v_busi_week as BUSI_WEEK,
             v_busi_year as BUSI_YEAR,
             t.business_line_id,
             v_begin_date as BEGIN_DATE,
             v_end_date as END_DATE,
             t.fund_account_id as CLIENT_ID,
             t.CLIENT_NAME,
             t.RANK_NO,
             '1' as INDEX_TYPE
        from tmp_rank t
       where t.rank_no <= v_rank_no)
    select BUSI_WEEK,
           BUSI_YEAR,
           business_line_id,
           BEGIN_DATE,
           END_DATE,
           CLIENT_ID,
           CLIENT_NAME,
           RANK_NO,
           INDEX_TYPE
      from tmp_result t

    ;
  commit;

  --2：出金前9名
  INSERT INTO CF_BUSIMG.T_COCKPIT_CLIENT_ANAL_LINE_TOP
    (BUSI_WEEK,
     BUSI_YEAR,
     business_line_id,
     BEGIN_DATE,
     END_DATE,
     CLIENT_ID,
     CLIENT_NAME,
     RANK_NO,
     INDEX_TYPE)
    with tmp as
     (select t.fund_account_id,
             b.client_name,
             D.business_line_id,
             sum(t.fund_out) as fund_out
        from cf_sett.t_client_sett t
        left join cf_sett.t_fund_account b
          on t.fund_account_id = b.fund_account_id
       inner join cf_busimg.t_ctp_branch_oa_rela c
          on b.branch_id = c.ctp_branch_id
       inner join CF_BUSIMG.T_OA_BRANCH d
          on c.oa_branch_id = d.departmentid
       where t.busi_date between v_begin_date and v_end_date
         and c.Oa_Branch_Id is not null
         AND D.BUSINESS_LINE_ID IS NOT NULL
       group by t.fund_account_id, b.client_name, D.business_line_id),
    tmp_rank as
     (select t.fund_account_id,
             t.client_name,
             t.business_line_id,
             t.fund_out,
             RANK() OVER(partition by t.business_line_id ORDER BY t.fund_out DESC) as rank_no
        from tmp t),
    tmp_result as
     (select v_busi_week as BUSI_WEEK,
             v_busi_year as BUSI_YEAR,
             t.business_line_id,
             v_begin_date as BEGIN_DATE,
             v_end_date as END_DATE,
             t.fund_account_id as CLIENT_ID,
             t.CLIENT_NAME,
             t.RANK_NO,
             '2' as INDEX_TYPE
        from tmp_rank t
       where t.rank_no <= v_rank_no)
    select BUSI_WEEK,
           BUSI_YEAR,
           business_line_id,
           BEGIN_DATE,
           END_DATE,
           CLIENT_ID,
           CLIENT_NAME,
           RANK_NO,
           INDEX_TYPE
      from tmp_result t;

  commit;

  --3：盈利前9名
  INSERT INTO CF_BUSIMG.T_COCKPIT_CLIENT_ANAL_LINE_TOP
    (BUSI_WEEK,
     BUSI_YEAR,
     business_line_id,
     BEGIN_DATE,
     END_DATE,
     CLIENT_ID,
     CLIENT_NAME,
     RANK_NO,
     INDEX_TYPE)
    with tmp as
     (select t.fund_account_id,
             b.client_name,
             D.business_line_id,
             sum(t.today_profit) as today_profit
        from cf_sett.t_client_sett t
        left join cf_sett.t_fund_account b
          on t.fund_account_id = b.fund_account_id
       inner join cf_busimg.t_ctp_branch_oa_rela c
          on b.branch_id = c.ctp_branch_id
       inner join CF_BUSIMG.T_OA_BRANCH d
          on c.oa_branch_id = d.departmentid
       where t.busi_date between v_begin_date and v_end_date
         and c.Oa_Branch_Id is not null
         AND D.BUSINESS_LINE_ID IS NOT NULL
       group by t.fund_account_id, b.client_name, D.business_line_id),
    tmp_rank as
     (select t.fund_account_id,
             t.client_name,
             t.business_line_id,
             t.today_profit,
             RANK() OVER(partition by t.business_line_id ORDER BY t.today_profit DESC) as rank_no
        from tmp t
       where today_profit > 0 --盈利
      ),
    tmp_result as
     (select v_busi_week as BUSI_WEEK,
             v_busi_year as BUSI_YEAR,
             t.business_line_id,
             v_begin_date as BEGIN_DATE,
             v_end_date as END_DATE,
             t.fund_account_id as CLIENT_ID,
             t.CLIENT_NAME,
             t.RANK_NO,
             '3' as INDEX_TYPE
        from tmp_rank t
       where t.rank_no <= v_rank_no)
    select BUSI_WEEK,
           BUSI_YEAR,
           business_line_id,
           BEGIN_DATE,
           END_DATE,
           CLIENT_ID,
           CLIENT_NAME,
           RANK_NO,
           INDEX_TYPE
      from tmp_result t;

  commit;

  --4：亏损前9名
  INSERT INTO CF_BUSIMG.T_COCKPIT_CLIENT_ANAL_LINE_TOP
    (BUSI_WEEK,
     BUSI_YEAR,
     business_line_id,
     BEGIN_DATE,
     END_DATE,
     CLIENT_ID,
     CLIENT_NAME,
     RANK_NO,
     INDEX_TYPE)
    with tmp as
     (select t.fund_account_id,
             b.client_name,
             D.business_line_id,
             sum(t.today_profit) as today_profit
        from cf_sett.t_client_sett t
        left join cf_sett.t_fund_account b
          on t.fund_account_id = b.fund_account_id
       inner join cf_busimg.t_ctp_branch_oa_rela c
          on b.branch_id = c.ctp_branch_id
       inner join CF_BUSIMG.T_OA_BRANCH d
          on c.oa_branch_id = d.departmentid
       where t.busi_date between v_begin_date and v_end_date
         and c.Oa_Branch_Id is not null
         AND D.BUSINESS_LINE_ID IS NOT NULL
       group by t.fund_account_id, b.client_name, D.business_line_id),
    tmp_rank as
     (select t.fund_account_id,
             t.client_name,
             t.business_line_id,
             t.today_profit,
             RANK() OVER(partition by t.business_line_id ORDER BY t.today_profit DESC) as rank_no
        from tmp t
       where t.today_profit < 0 --亏损
      ),
    tmp_result as
     (select v_busi_week as BUSI_WEEK,
             v_busi_year as BUSI_YEAR,
             t.business_line_id,
             v_begin_date as BEGIN_DATE,
             v_end_date as END_DATE,
             t.fund_account_id as CLIENT_ID,
             t.CLIENT_NAME,
             t.RANK_NO,
             '4' as INDEX_TYPE
        from tmp_rank t
       where t.rank_no <= v_rank_no)
    select BUSI_WEEK,
           BUSI_YEAR,
           business_line_id,
           BEGIN_DATE,
           END_DATE,
           CLIENT_ID,
           CLIENT_NAME,
           RANK_NO,
           INDEX_TYPE
      from tmp_result t;

  commit;

  --5：收入贡献前9名
  INSERT INTO CF_BUSIMG.T_COCKPIT_CLIENT_ANAL_LINE_TOP
    (BUSI_WEEK,
     BUSI_YEAR,
     business_line_id,
     BEGIN_DATE,
     END_DATE,
     CLIENT_ID,
     CLIENT_NAME,
     RANK_NO,
     INDEX_TYPE)
    with tmp as
     (select t.fund_account_id,
             b.client_name,
             D.business_line_id,
             sum(t.transfee + t.strikefee + t.delivery_transfee -
                 t.market_transfee - t.market_delivery_transfee -
                 t.market_strikefee) as remain_transfee
        from cf_sett.t_client_sett t
        left join cf_sett.t_fund_account b
          on t.fund_account_id = b.fund_account_id
       inner join cf_busimg.t_ctp_branch_oa_rela c
          on b.branch_id = c.ctp_branch_id
       inner join CF_BUSIMG.T_OA_BRANCH d
          on c.oa_branch_id = d.departmentid
       where t.busi_date between v_begin_date and v_end_date
         and c.Oa_Branch_Id is not null
         AND D.BUSINESS_LINE_ID IS NOT NULL
       group by t.fund_account_id, b.client_name, D.business_line_id),
    tmp_rank as
     (select t.fund_account_id,
             t.client_name,
             t.business_line_id,
             t.remain_transfee,
             RANK() OVER(partition by t.business_line_id ORDER BY t.remain_transfee DESC) as rank_no
        from tmp t),
    tmp_result as
     (select v_busi_week as BUSI_WEEK,
             v_busi_year as BUSI_YEAR,
             t.business_line_id,
             v_begin_date as BEGIN_DATE,
             v_end_date as END_DATE,
             t.fund_account_id as CLIENT_ID,
             t.CLIENT_NAME,
             t.RANK_NO,
             '5' as INDEX_TYPE
        from tmp_rank t
       where t.rank_no <= v_rank_no)
    select BUSI_WEEK,
           BUSI_YEAR,
           business_line_id,
           BEGIN_DATE,
           END_DATE,
           CLIENT_ID,
           CLIENT_NAME,
           RANK_NO,
           INDEX_TYPE
      from tmp_result t;

  commit;

  --6：成交量前9名
  INSERT INTO CF_BUSIMG.T_COCKPIT_CLIENT_ANAL_LINE_TOP
    (BUSI_WEEK,
     BUSI_YEAR,
     business_line_id,
     BEGIN_DATE,
     END_DATE,
     CLIENT_ID,
     CLIENT_NAME,
     RANK_NO,
     INDEX_TYPE)
    with tmp as
     (select t.fund_account_id,
             b.client_name,
             D.business_line_id,
             sum(t.done_amt) as done_amount
        from cf_sett.t_hold_balance t
        left join cf_sett.t_fund_account b
          on t.fund_account_id = b.fund_account_id
       inner join cf_busimg.t_ctp_branch_oa_rela c
          on b.branch_id = c.ctp_branch_id
       inner join CF_BUSIMG.T_OA_BRANCH d
          on c.oa_branch_id = d.departmentid
       where t.busi_date between v_begin_date and v_end_date
         and c.Oa_Branch_Id is not null
         AND D.BUSINESS_LINE_ID IS NOT NULL
       group by t.fund_account_id, b.client_name, D.business_line_id),
    tmp_rank as
     (select t.fund_account_id,
             t.client_name,
             t.business_line_id,
             t.done_amount,
             RANK() OVER(partition by t.business_line_id ORDER BY t.done_amount DESC) as rank_no
        from tmp t),
    tmp_result as
     (select v_busi_week as BUSI_WEEK,
             v_busi_year as BUSI_YEAR,
             t.business_line_id,
             v_begin_date as BEGIN_DATE,
             v_end_date as END_DATE,
             t.fund_account_id as CLIENT_ID,
             t.CLIENT_NAME,
             t.RANK_NO,
             '6' as INDEX_TYPE
        from tmp_rank t
       where t.rank_no <= v_rank_no)
    select BUSI_WEEK,
           BUSI_YEAR,
           business_line_id,
           BEGIN_DATE,
           END_DATE,
           CLIENT_ID,
           CLIENT_NAME,
           RANK_NO,
           INDEX_TYPE
      from tmp_result t;

  commit;
  --7：成交额前9名
  INSERT INTO CF_BUSIMG.T_COCKPIT_CLIENT_ANAL_LINE_TOP
    (BUSI_WEEK,
     BUSI_YEAR,
     business_line_id,
     BEGIN_DATE,
     END_DATE,
     CLIENT_ID,
     CLIENT_NAME,
     RANK_NO,
     INDEX_TYPE)
    with tmp as
     (select t.fund_account_id,
             b.client_name,
             D.business_line_id,
             sum(t.done_sum) as done_sum
        from cf_sett.t_hold_balance t
        left join cf_sett.t_fund_account b
          on t.fund_account_id = b.fund_account_id
       inner join cf_busimg.t_ctp_branch_oa_rela c
          on b.branch_id = c.ctp_branch_id
       inner join CF_BUSIMG.T_OA_BRANCH d
          on c.oa_branch_id = d.departmentid
       where t.busi_date between v_begin_date and v_end_date
         and c.Oa_Branch_Id is not null
         AND D.BUSINESS_LINE_ID IS NOT NULL
       group by t.fund_account_id, b.client_name, D.business_line_id),
    tmp_rank as
     (select t.fund_account_id,
             t.client_name,
             t.business_line_id,
             t.done_sum,
             RANK() OVER(partition by t.business_line_id ORDER BY t.done_sum DESC) as rank_no
        from tmp t),
    tmp_result as
     (select v_busi_week as BUSI_WEEK,
             v_busi_year as BUSI_YEAR,
             t.business_line_id,
             v_begin_date as BEGIN_DATE,
             v_end_date as END_DATE,
             t.fund_account_id as CLIENT_ID,
             t.CLIENT_NAME,
             t.RANK_NO,
             '7' as INDEX_TYPE
        from tmp_rank t
       where t.rank_no <= v_rank_no)
    select BUSI_WEEK,
           BUSI_YEAR,
           business_line_id,
           BEGIN_DATE,
           END_DATE,
           CLIENT_ID,
           CLIENT_NAME,
           RANK_NO,
           INDEX_TYPE
      from tmp_result t;

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

