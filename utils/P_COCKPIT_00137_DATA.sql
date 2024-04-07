CREATE OR REPLACE PROCEDURE CF_BUSIMG.P_COCKPIT_00137_DATA(I_MONTH_ID    IN VARCHAR2,
                                                           O_RETURN_MSG  OUT VARCHAR2, --返回消息
                                                           O_RETURN_CODE OUT INTEGER --返回代码
                                                           ) IS
  ---------------------------------------------------------------------------------------
  -- copyright              wolf 1.0
  -- func_create_begin
  -- func_id
  -- version                1.2
  -- func_name
  -- func_remark            经营目标完成情况-数据落地
  -- create_date            20230822
  -- create_programer       zzy
  -- modify_remark
  -- func_create_end
  ---------------------------------------------------------------------------------------
  --固定变量
  ---------------------------------------------------------------------------------------
  v_op_object     varchar2(50) default 'P_COCKPIT_00137_DATA'; -- '操作对象';
  v_error_msg     varchar2(200); --返回信息
  v_error_code    integer;
  v_userException exception;
  ---------------------------------------------------------------------------------------
  --业务变量
  v_begin_trade_date varchar2(8); --开始交易时间
  v_end_trade_date   varchar2(8); --结束交易时间
  v_begin_date       varchar2(8); --开始日期
  v_end_date         varchar2(8); --结束日期
  v_year_id          varchar2(4); --当年年份
  v_trade_days       number; --交易日天数
  --取得账期
  ---------------------------------------------------------------------------------------
BEGIN
  -------------------------------------------------------------
  --开始交易时间，结束交易时间
  select min(t.busi_date), max(t.busi_date), count(1)
    into v_begin_trade_date, v_end_trade_date, v_trade_days
    from cf_sett.t_pub_date t
   where substr(t.busi_date, 1, 6) = I_MONTH_ID
     and t.market_no = '1'
     and t.trade_flag = '1';

  --开始日期，结束日期
  select min(t.busi_date), max(t.busi_date)
    into v_begin_date, v_end_date
    from cf_sett.t_pub_date t
   where substr(t.busi_date, 1, 6) = I_MONTH_ID
     and t.market_no = '1';
  v_year_id := substr(I_MONTH_ID, 1, 4);

  DELETE FROM cf_busimg.t_cockpit_00137 T
   WHERE T.Busi_Month = I_MONTH_ID
     and t.year_id = v_year_id;
  COMMIT;

  --初始化数据
  INSERT INTO cf_busimg.t_cockpit_00137
    (year_id,
     busi_month,
     oa_branch_id,
     oa_branch_name,
     index_id,
     index_asses_benchmark,
     index_type,
     index_name)
    SELECT t.year_id,
           I_MONTH_ID,
           t.oa_branch_id,
           t.oa_branch_name,
           t.index_id,
           t.index_asses_benchmark,
           t.index_type,
           t.index_name
      from cf_busimg.T_COCKPIT_00135 t
     where not exists (select 1
              from CF_BUSIMG.T_COCKPIT_00137 a
             where t.year_id = a.year_id
               and t.oa_branch_id = a.oa_branch_id
               and t.index_id = a.index_id)
       and t.year_id = v_YEAR_ID
       and t.check_result = '1' --校验结果(0：异常，1：正常)
       and t.index_status = '1'; --状态(0:待审核，1：审核通过，2：审核未通过)
  COMMIT;

  --更新数据  分支机构当月值
  --营业收入完成情况（万）  取数源：财务内核表——营业收入，调整后的收入
  --来源表 cf_busimg.t_cockpit_00127
  merge into cf_busimg.t_cockpit_00137 a
  using (select b.oa_branch_id, sum(t.b5) as INDEX_VALUE_BRANCH --营业收入完成情况（万）
           from cf_busimg.t_cockpit_00127 t
          inner join cf_busimg.t_yy_branch_oa_rela b
             on t.book_id = b.yy_book_id
          where t.month_id = I_MONTH_ID
          group by b.oa_branch_id) y
  on (a.YEAR_ID = v_YEAR_ID and a.busi_month = I_MONTH_ID and a.oa_branch_id = y.oa_branch_id and a.INDEX_ASSES_BENCHMARK = '1' --营业收入
  and a.INDEX_TYPE = '0' --绝对指标
  )
  when matched then
    update set a.INDEX_VALUE_BRANCH = y.INDEX_VALUE_BRANCH;
  commit;

  --经纪业务手续费收入市占率（万分之）
  --经纪业务手续费收入市占率 取数源：财务内核表：手续费及佣金净收入+交易所减免返还/行业手续费收入
  merge into cf_busimg.t_cockpit_00137 a
  using (
    with tmp as
     (select SUM(t.index_value * 100000000) as index_value_now --经纪业务手续费收入 -本期
        from CF_BUSIMG.T_COCKPIT_INDUSTRY_MANAGE T
       WHERE t.index_name = '手续费收入'
         and t.etl_month = I_MONTH_ID),
    tmp1 as
     (select b.oa_branch_id, sum(t.b6 + b8) as INDEX_VALUE_BRANCH --经纪业务手续费收入市占率
        from cf_busimg.t_cockpit_00127 t
       inner join cf_busimg.t_yy_branch_oa_rela b
          on t.book_id = b.yy_book_id
       where t.month_id = I_MONTH_ID
       group by b.oa_branch_id)
    select t1.oa_branch_id,
           (case
             when nvl(t.index_value_now, 0) <> 0 then
              nvl(t1.INDEX_VALUE_BRANCH, 0) / nvl(t.index_value_now, 0)
             else
              0
           end) as INDEX_VALUE_BRANCH,
           t.index_value_now as INDEX_VALUE_INDUST
      from tmp t, tmp1 t1) y
        on (a.YEAR_ID = v_YEAR_ID and a.busi_month = I_MONTH_ID and
           a.oa_branch_id = y.oa_branch_id and
           a.INDEX_ASSES_BENCHMARK = '1' --营业收入
           and a.INDEX_TYPE = '1' --相对指标
           ) when matched then
      update
         set a.INDEX_VALUE_BRANCH = y.INDEX_VALUE_BRANCH,
             a.INDEX_VALUE_INDUST = y.INDEX_VALUE_INDUST;
  commit;

  --客户资产规模 日均客户保证金完成情况 取数源：交易统计表——日均权益（考核口径），调整后的权益。
  --后续修改调整
  merge into cf_busimg.t_cockpit_00137 a
  using (select c.OA_BRANCH_ID,
                sum(case
                      when v_trade_days > 0 then
                       t.avg_trade_rights / v_trade_days
                      else
                       0
                    end) as INDEX_VALUE_BRANCH
           from CF_STAT.T_CLIENT_SETT_DATA t
           left join cf_sett.t_fund_account b
             on t.fund_account_id = b.fund_account_id
          inner join cf_busimg.t_ctp_branch_oa_rela c
             on b.branch_id = c.CTP_BRANCH_ID
          where t.busi_date_during = I_MONTH_ID
          group by c.OA_BRANCH_ID) y
  on (a.YEAR_ID = v_YEAR_ID and a.busi_month = I_MONTH_ID and a.oa_branch_id = y.oa_branch_id and a.INDEX_ASSES_BENCHMARK = '2' --客户资产规模
  and a.INDEX_TYPE = '0' --绝对指标
  )
  when matched then
    update set a.INDEX_VALUE_BRANCH = y.INDEX_VALUE_BRANCH;
  commit;

  --日均客户保证金市占率  取数源：交易统计表——日均权益（全口径），调整后的权益/行业
  merge into cf_busimg.t_cockpit_00137 a
  using (
    with tmp as
     (select SUM(case
                   when v_trade_days <> 0 then
                    t.index_value * 100000000 / v_trade_days
                   else
                    0
                 end) as index_value_now
        from CF_BUSIMG.T_COCKPIT_INDUSTRY_MANAGE T
       WHERE t.index_name = '客户权益'
         and t.etl_month = I_MONTH_ID),
    tmp1 as
     (select c.OA_BRANCH_ID,
             sum(case
                   when v_trade_days <> 0 then
                    t.avg_trade_rights / v_trade_days
                   else
                    0
                 end) as INDEX_VALUE_BRANCH
        from CF_STAT.T_CLIENT_SETT_DATA t
        left join cf_sett.t_fund_account b
          on t.fund_account_id = b.fund_account_id
       inner join cf_busimg.t_ctp_branch_oa_rela c
          on b.branch_id = c.CTP_BRANCH_ID
       where t.busi_date_during = I_MONTH_ID
       group by c.OA_BRANCH_ID)
    select t.OA_BRANCH_ID,
           (case
             when nvl(t1.index_value_now, 0) <> 0 then
              t.INDEX_VALUE_BRANCH / t1.index_value_now
             else
              0
           end) as INDEX_VALUE_BRANCH
      from tmp1 t, tmp t1

    ) y
        on (a.YEAR_ID = v_YEAR_ID and a.busi_month = I_MONTH_ID and
           a.oa_branch_id = y.oa_branch_id and
           a.INDEX_ASSES_BENCHMARK = '2' --客户资产规模
           and a.INDEX_TYPE = '1' --相对指标
           ) when matched then
      update set a.INDEX_VALUE_BRANCH = y.INDEX_VALUE_BRANCH;
  commit;

  --考核利润 绝对指标 考核利润完成情况（万） 取数源：财务内核表：三、营业利润（亏损以“-”号填列）　
  --来源表 cf_busimg.t_cockpit_00127
  merge into cf_busimg.T_COCKPIT_00137 a
  using (select b.oa_branch_id, sum(t.b18) as INDEX_VALUE_BRANCH --营业收入完成情况（万）
           from cf_busimg.t_cockpit_00127 t
          inner join cf_busimg.t_yy_branch_oa_rela b
             on t.book_id = b.yy_book_id
          where t.month_id = I_MONTH_ID
          group by b.oa_branch_id) y
  on (a.YEAR_ID = v_YEAR_ID and a.busi_month = I_MONTH_ID and a.oa_branch_id = y.oa_branch_id and a.INDEX_ASSES_BENCHMARK = '3' --考核利润
  and a.INDEX_TYPE = '0' --绝对指标
  )
  when matched then
    update set a.INDEX_VALUE_BRANCH = y.INDEX_VALUE_BRANCH;
  commit;

  --成交额 绝对指标  客户交易成交金额（双边） 分支机构是双边计算的，直接取CTP的数据，成交额（调整后数据）
  merge into cf_busimg.T_COCKPIT_00137 a
  using (
    with tmp as
     (select c.oa_branch_id, sum(t.done_money) as INDEX_VALUE_BRANCH
        from CF_STAT.T_TRADE_SUM_DATA t
        left join cf_sett.t_fund_account b
          on t.fund_account_id = b.fund_account_id
       inner join cf_busimg.t_ctp_branch_oa_rela c
          on b.branch_id = c.CTP_BRANCH_ID
       where t.busi_date_during = I_MONTH_ID
       group by c.oa_branch_id),
    tmp1 as
     (select SUM(t.TRAD_AMT * 2 * 100000000) as index_value_now --成交额 -本期

        from CF_BUSIMG.T_COCKPIT_INDUSTRY_TRAD T
       where t.ETL_MONTH = I_MONTH_ID)
    select t.oa_branch_id,
           t.INDEX_VALUE_BRANCH,
           nvl(t1.index_value_now, 0) as INDEX_VALUE_INDUST
      from tmp t, tmp1 t1

    ) y
        on (a.YEAR_ID = v_YEAR_ID and a.busi_month = I_MONTH_ID and
           a.oa_branch_id = y.oa_branch_id and
           a.INDEX_ASSES_BENCHMARK = '4' --成交额
           and a.INDEX_TYPE = '0' --绝对指标
           ) when matched then
      update
         set a.INDEX_VALUE_BRANCH = y.INDEX_VALUE_BRANCH,
             a.INDEX_VALUE_INDUST = y.INDEX_VALUE_INDUST;
  commit;

  --成交额 相对指标  成交额市占率(双边） 分支机构是双边计算的，直接取CTP的数据，成交额（调整后数据）/行业
  merge into cf_busimg.T_COCKPIT_00137 a
  using (
    with tmp as
     (select c.oa_branch_id, sum(t.done_money) as INDEX_VALUE_BRANCH
        from CF_STAT.T_TRADE_SUM_DATA t
        left join cf_sett.t_fund_account b
          on t.fund_account_id = b.fund_account_id
       inner join cf_busimg.t_ctp_branch_oa_rela c
          on b.branch_id = c.CTP_BRANCH_ID
       where t.busi_date_during = I_MONTH_ID
       group by c.oa_branch_id),
    tmp1 as
     (select SUM(t.TRAD_AMT * 2 * 100000000) as index_value_now --成交额 -本期

        from CF_BUSIMG.T_COCKPIT_INDUSTRY_TRAD T
       where t.ETL_MONTH = I_MONTH_ID)
    select t.oa_branch_id,
           (case
             when nvl(t1.index_value_now, 0) <> 0 then
              t.INDEX_VALUE_BRANCH / nvl(t1.index_value_now, 0)
             else
              0
           end) as INDEX_VALUE_BRANCH
      from tmp t, tmp1 t1) y
        on (a.YEAR_ID = v_YEAR_ID and a.busi_month = I_MONTH_ID and
           a.oa_branch_id = y.oa_branch_id and
           a.INDEX_ASSES_BENCHMARK = '4' --成交额
           and a.INDEX_TYPE = '1' --相对指标
           ) when matched then
      update set a.INDEX_VALUE_BRANCH = y.INDEX_VALUE_BRANCH;
  commit;

  --成交量 绝对指标  客户交易成交量（双边） 分支机构是双边计算的，直接取CTP的数据，成交量（调整后数据）
  merge into cf_busimg.T_COCKPIT_00137 a
  using (
    with tmp as
     (select c.oa_branch_id, sum(t.done_amount) as INDEX_VALUE_BRANCH
        from CF_STAT.T_TRADE_SUM_DATA t
        left join cf_sett.t_fund_account b
          on t.fund_account_id = b.fund_account_id
       inner join cf_busimg.t_ctp_branch_oa_rela c
          on b.branch_id = c.CTP_BRANCH_ID
       where t.busi_date_during = I_MONTH_ID
       group by c.oa_branch_id),
    tmp1 as
     (select SUM(t.TRAD_NUM * 2 * 100000000) as index_value_now --成交量 -本期
        from CF_BUSIMG.T_COCKPIT_INDUSTRY_TRAD T
       where t.ETL_MONTH = I_MONTH_ID)
    select t.oa_branch_id,
           t.INDEX_VALUE_BRANCH,
           nvl(t1.index_value_now, 0) as INDEX_VALUE_INDUST
      from tmp t, tmp1 t1) y
        on (a.YEAR_ID = v_YEAR_ID and a.busi_month = I_MONTH_ID and
           a.oa_branch_id = y.oa_branch_id and
           a.INDEX_ASSES_BENCHMARK = '5' --成交量
           and a.INDEX_TYPE = '0' --绝对指标
           ) when matched then
      update
         set a.INDEX_VALUE_BRANCH = y.INDEX_VALUE_BRANCH,
             a.INDEX_VALUE_INDUST = y.INDEX_VALUE_INDUST;
  commit;

  --成交量 相对指标  成交量市占率(双边） 分支机构是双边计算的，直接取CTP的数据，成交量（调整后数据）/行业
  merge into cf_busimg.T_COCKPIT_00137 a
  using (
    with tmp as
     (select c.oa_branch_id, sum(t.done_amount) as INDEX_VALUE_BRANCH
        from CF_STAT.T_TRADE_SUM_DATA t
        left join cf_sett.t_fund_account b
          on t.fund_account_id = b.fund_account_id
       inner join cf_busimg.t_ctp_branch_oa_rela c
          on b.branch_id = c.CTP_BRANCH_ID
       where t.busi_date_during = I_MONTH_ID
       group by c.oa_branch_id),
    tmp1 as
     (select SUM(t.TRAD_NUM * 2 * 100000000) as index_value_now --成交量 -本期

        from CF_BUSIMG.T_COCKPIT_INDUSTRY_TRAD T
       where t.ETL_MONTH = I_MONTH_ID)
    select t.oa_branch_id,
           (case
             when nvl(t1.index_value_now, 0) <> 0 then
              t.INDEX_VALUE_BRANCH / nvl(t1.index_value_now, 0)
             else
              0
           end) as INDEX_VALUE_BRANCH
      from tmp t, tmp1 t1) y
        on (a.YEAR_ID = v_YEAR_ID and a.busi_month = I_MONTH_ID and
           a.oa_branch_id = y.oa_branch_id and
           a.INDEX_ASSES_BENCHMARK = '5' --成交量
           and a.INDEX_TYPE = '1' --相对指标
           ) when matched then
      update set a.INDEX_VALUE_BRANCH = y.INDEX_VALUE_BRANCH;
  commit;

  --新增有效客户数 绝对指标  新增直接开发有效客户数量
  /*
  （1）所选择的月份区间中，新开户的客户数量
  （2）减掉:
  业绩关系查询中，所选的月份区间中，满足以下三个
  a.关系类型为“居间关系”，
  b.关系状态为“有效”，
  c.审批状态为“审批通过”，
  筛选条件得到的客户。
  （3）剩余客户数量，
  在所选择的月份区间，有交易的客户数量
    */
  --b.关系状态为“有效”，
  --c.审批状态为“审批通过”，
  execute immediate 'truncate table  CF_BUSIMG.Tmp_COCKPIT_00137_1';
  insert into CF_BUSIMG.Tmp_COCKPIT_00137_1
    (FUND_ACCOUNT_ID, BROKER_RELA_TYPE)
    WITH TMP AS
     (select a.BROKER_ID,
             b.broker_nam as BROKER_NAME,
             trim(a.INVESTOR_ID) as FUND_ACCOUNT_ID,
             c.investor_nam AS CLIENT_NAME,
             replace(a.ST_DT, '-', '') as BEGIN_DATE,
             replace(a.END_DT, '-', '') as END_DATE,
             decode(a.BROKER_RELA_TYP,
                    '301',
                    '居间关系',
                    '001',
                    '开发关系',
                    '002',
                    '服务关系',
                    '003',
                    '维护关系',
                    '-') BROKER_RELA_TYPE,
             a.data_pct,
             decode(a.RELA_STS, 'A', '有效', 'S', '停止使用', '-') RELA_STATUS,
             a.APPROVE_DT,
             decode(a.APPROVE_STS,
                    '0',
                    '审核通过',
                    '1',
                    '审核不通过',
                    '2',
                    '等待审核',
                    '-') APPROVE_STS,
             a.comment_desc,
             a.check_comments,
             (case
               when replace(a.ST_DT, '-', '') < v_begin_date then
                v_begin_date
               when replace(a.ST_DT, '-', '') >= v_begin_date then
                replace(a.ST_DT, '-', '')
               else
                ''
             end) as REAL_BEGIN_DATE,
             (case
               when replace(a.END_DT, '-', '') <= v_end_date then
                replace(a.END_DT, '-', '')
               when replace(a.END_DT, '-', '') > v_end_date then
                v_end_date
               else
                ''
             end) as REAL_END_DATE

        from CTP63.T_DS_CRM_BROKER_INVESTOR_RELA a
        join CTP63.T_DS_CRM_BROKER b
          on a.broker_id = b.broker_id
        join CTP63.T_DS_MDP_DEPT00 f
          on b.department_id = f.chdeptcode
        join CTP63.T_DS_DC_INVESTOR c
          on a.investor_id = c.investor_id
        left join cf_sett.t_fund_account x
          on c.investor_id = x.fund_account_id
        left join cf_sett.t_branch x1
          on x.branch_id = x1.branch_id
        left join cf_busimg.t_ctp_branch_oa_rela x2
          on x1.branch_id = x2.ctp_branch_id
       where a.RELA_STS = 'A' --有效
         AND A.APPROVE_STS = '0' --审核通过
         and a.data_pct is not null)
    SELECT T.FUND_ACCOUNT_ID, t.BROKER_RELA_TYPE
      FROM TMP T
     WHERE T.REAL_BEGIN_DATE <= t.REAL_END_DATE
     group by t.fund_account_id, t.BROKER_RELA_TYPE;
  commit;

  merge into cf_busimg.T_COCKPIT_00137 a
  using (
    with tmp as
     (
      --新开客户，且排除居间关系的客户
      select t.fund_account_id
        from cf_sett.t_fund_account t
       where not exists (select 1
                from CF_BUSIMG.Tmp_COCKPIT_00135_1 a
               where t.fund_account_id = a.fund_account_id
                 and a.broker_rela_type = '居间关系')
         and t.open_date between v_begin_date and v_end_date

      ),
    tmp1 as
     (select t.fund_account_id
        from CF_STAT.T_TRADE_SUM_DATA t
       inner join tmp a
          on t.fund_account_id = a.fund_account_id
       where t.BUSI_DATE_DURING = I_MONTH_ID
         and t.TOTAL_TRANSFEE <> 0 --手续费不为0，有交易
       group by t.fund_account_id)
    select c.oa_branch_id, count(1) as INDEX_VALUE_BRANCH
      from tmp1 t
      left join cf_sett.t_fund_account b
        on t.fund_account_id = b.fund_account_id
     inner join cf_busimg.t_ctp_branch_oa_rela c
        on b.branch_id = c.CTP_BRANCH_ID
     group by c.oa_branch_id

    ) y
        on (a.YEAR_ID = v_YEAR_ID and a.busi_month = I_MONTH_ID and
           a.oa_branch_id = y.oa_branch_id and
           a.INDEX_ASSES_BENCHMARK = '6' --新增有效客户数
           and a.INDEX_TYPE = '0' --绝对指标
           and a.index_name like '%新增直接开发有效客户数量%') when matched then
      update set a.INDEX_VALUE_BRANCH = y.INDEX_VALUE_BRANCH;
  commit;

  --新增有效客户数  绝对指标 新增有效客户数量（户）
  /*
    （1）所选择的月份区间中，新开户的客户数量
  （2）减掉:
  业绩关系查询中，所选的月份区间中，满足以下两个
  a.关系状态为"有效"，
  b.审批状态为"审批通过“，
  筛选条件得到的客户。
  （3）剩余客户数量，
  在所选择的月份区间，有交易的客户数量
    */
  merge into cf_busimg.T_COCKPIT_00137 a
  using (
    with tmp as
     (
      --新开客户，且排除居间关系的客户
      select t.fund_account_id
        from cf_sett.t_fund_account t
       where not exists (select 1
                from CF_BUSIMG.Tmp_COCKPIT_00135_1 a
               where t.fund_account_id = a.fund_account_id)
         and t.open_date between v_begin_date and v_end_date

      ),
    tmp1 as
     (select t.fund_account_id
        from CF_STAT.T_TRADE_SUM_DATA t
       inner join tmp a
          on t.fund_account_id = a.fund_account_id
       where t.BUSI_DATE_DURING = I_MONTH_ID
         and t.TOTAL_TRANSFEE <> 0 --手续费不为0，有交易
       group by t.fund_account_id)
    select c.oa_branch_id, count(1) as INDEX_VALUE_BRANCH
      from tmp1 t
      left join cf_sett.t_fund_account b
        on t.fund_account_id = b.fund_account_id
     inner join cf_busimg.t_ctp_branch_oa_rela c
        on b.branch_id = c.CTP_BRANCH_ID
     group by c.oa_branch_id

    ) y
        on (a.YEAR_ID = v_YEAR_ID and a.busi_month = I_MONTH_ID and
           a.oa_branch_id = y.oa_branch_id and
           a.INDEX_ASSES_BENCHMARK = '6' --新增有效客户数
           and a.INDEX_TYPE = '0' --绝对指标
           and a.index_name like '%新增有效客户数量%') when matched then
      update set a.INDEX_VALUE_BRANCH = y.INDEX_VALUE_BRANCH;
  commit;

  --产品销售（万） 绝对指标  新增公司统一组织的产品销售额  （资管表3月度销售人员保有奖励分配情况—新增量）

  merge into cf_busimg.T_COCKPIT_00137 a
  using (

         select c.oa_branch_id,
                 sum(case
                       when a.wh_trade_type in ('0', '1') then
                        a.confirm_share
                       else
                        0
                     end) as INDEX_VALUE_BRANCH --新增量
           from CF_BUSIMG.T_COCKPIT_00096 a
          inner join cf_sett.t_fund_account b
             on a.id_no = b.id_no
            and a.client_name = b.client_name
          inner join cf_busimg.t_ctp_branch_oa_rela c
             on b.branch_id = c.CTP_BRANCH_ID
          where a.BUSI_DATE between v_begin_date and v_end_date
          group by c.oa_branch_id) y
  on (a.YEAR_ID = v_YEAR_ID and a.busi_month = I_MONTH_ID and a.oa_branch_id = y.oa_branch_id and a.INDEX_ASSES_BENCHMARK = '7' --产品销售
  and a.INDEX_TYPE = '0' --绝对指标
  )
  when matched then
    update set a.INDEX_VALUE_BRANCH = y.INDEX_VALUE_BRANCH;
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

