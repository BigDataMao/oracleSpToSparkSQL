CREATE OR REPLACE PROCEDURE CF_BUSIMG.P_COCKPIT_00179_DATA(I_MONTH_ID    IN VARCHAR2,
                                                           O_RETURN_MSG  OUT VARCHAR2, --返回消息
                                                           O_RETURN_CODE OUT INTEGER --返回代码
                                                           ) IS
  ---------------------------------------------------------------------------------------
  -- copyright              wolf 1.0
  -- func_create_begin
  -- func_id                P_COCKPIT_00179_DATA
  -- version                1.0
  -- func_name
  -- func_remark            驻点人员营销统计数据表（最终呈现表）-落地数据
  -- create_date            2023/07/26 10:24:37
  -- create_programer       zhongying.zhang
  -- modify_remark
  -- func_create_end
  ---------------------------------------------------------------------------------------
  --固定变量
  ---------------------------------------------------------------------------------------
  v_op_object  VARCHAR2(50) DEFAULT 'P_COCKPIT_00110'; -- '操作对象';
  v_error_msg  VARCHAR2(200); --返回信息
  v_error_code INTEGER;
  v_userException EXCEPTION;
  ---------------------------------------------------------------------------------------
  --业务变量
  v_begin_trade_date varchar2(8); --开始交易日期
  v_end_trade_date   varchar2(8); --结束交易日期
  v_trade_days       number; --交易日天数

  v_begin_date varchar2(8);
  v_end_date   varchar2(8);
  ---------------------------------------------------------------------------------------

BEGIN
  select min(t.busi_date), max(t.busi_date), count(1)
    into v_begin_trade_date, v_end_trade_date, v_trade_days
    from cf_sett.t_pub_date t
   where substr(t.busi_date, 1, 6) = I_MONTH_ID
     and t.market_no = '1'
     and t.trade_flag = '1';

  select min(t.busi_date), max(t.busi_date)
    into v_begin_date, v_end_date
    from cf_sett.t_pub_date t
   where substr(t.busi_date, 1, 6) = I_MONTH_ID
     and t.market_no = '1';

  --根据查询日期获取业务人员关系数据
  execute immediate 'truncate table CF_BUSIMG.TMP_COCKPIT_00110_1';
  insert into CF_BUSIMG.TMP_COCKPIT_00110_1
    (BROKER_ID,
     BROKER_NAME,
     FUND_ACCOUNT_ID,
     CLIENT_NAME,
     BEGIN_DATE,
     END_DATE,
     BROKER_RELA_TYPE,
     DATA_PCT,
     RELA_STATUS,
     APPROVE_DT,
     APPROVE_STS,
     COMMENT_DESC,
     CHECK_COMMENTS,
     REAL_BEGIN_DATE,
     REAL_END_DATE)
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
               when replace(a.ST_DT, '-', '') < v_BEGIN_DATE then
                v_BEGIN_DATE
               when replace(a.ST_DT, '-', '') >= v_BEGIN_DATE then
                replace(a.ST_DT, '-', '')
               else
                ''
             end) as REAL_BEGIN_DATE,
             (case
               when replace(a.END_DT, '-', '') <= v_END_DATE then
                replace(a.END_DT, '-', '')
               when replace(a.END_DT, '-', '') > v_END_DATE then
                v_END_DATE
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
       where b.broker_id like 'ZD%' --只取ZD开头的数据

         and a.RELA_STS = 'A' --有效
         AND A.APPROVE_STS = '0' --审核通过
      /*and a.data_pct is not null*/
      )
    SELECT T.BROKER_ID,
           T.BROKER_NAME,
           T.FUND_ACCOUNT_ID,
           T.CLIENT_NAME,
           T.BEGIN_DATE,
           T.END_DATE,
           T.BROKER_RELA_TYPE,
           T.DATA_PCT,
           T.RELA_STATUS,
           T.APPROVE_DT,
           T.APPROVE_STS,
           T.COMMENT_DESC,
           T.CHECK_COMMENTS,
           T.REAL_BEGIN_DATE,
           T.REAL_END_DATE
      FROM TMP T
     WHERE T.REAL_BEGIN_DATE <= t.REAL_END_DATE
       and replace(substr(t.approve_dt, 1, 10), '-', '') <= v_END_DATE;
  commit;

  --获取有关系的的客户
  execute immediate 'truncate table CF_BUSIMG.TMP_COCKPIT_00110_2';
  insert into CF_BUSIMG.TMP_COCKPIT_00110_2
    (FUND_ACCOUNT_ID, real_begin_date, real_end_date)
    select t.fund_account_id, t.real_begin_date, t.real_end_date
      from CF_BUSIMG.TMP_COCKPIT_00110_1 t
     group by t.fund_account_id, t.real_begin_date, t.real_end_date;
  commit;

  --按照日期计算 期末权益  日均权益
  execute immediate 'truncate table CF_BUSIMG.TMP_COCKPIT_00110_3';
  insert into CF_BUSIMG.TMP_COCKPIT_00110_3
    ( /*BUSI_DATE,*/ FUND_ACCOUNT_ID, END_RIGHTS, avg_rights)

    select /*t.busi_date,*/
     t.fund_account_id,
     sum(case
           when t.busi_date = v_end_trade_date then
            t.rights
           else
            0
         end) as end_rights,
     sum(case
           when v_trade_days > 0 then
            t.rights / v_trade_days
           else
            0
         end) as avg_rights
      from cf_sett.t_client_sett t
     inner join CF_BUSIMG.TMP_COCKPIT_00110_2 a
        on t.fund_account_id = a.fund_account_id
       and t.busi_date between a.real_begin_date and a.real_end_date
     where t.busi_date between v_BEGIN_DATE and v_END_DATE
     group by /*t.busi_date, */ t.fund_account_id;
  commit;

  --按照日期计算 成交手数 成交金额
  execute immediate 'truncate table CF_BUSIMG.TMP_COCKPIT_00110_4';
  insert into CF_BUSIMG.TMP_COCKPIT_00110_4
    ( /*BUSI_DATE,*/ FUND_ACCOUNT_ID, DONE_AMOUNT, DONE_MONEY)
    with tmp as
     (select t.busi_date,
             t.fund_account_id,
             sum(t.done_amt) as done_amount,
             sum(t.done_sum) as done_money
        from cf_sett.t_hold_balance t
       where t.busi_date between v_BEGIN_DATE and v_END_DATE
       group by t.busi_date, t.fund_account_id)
    select /*t.busi_date,*/
     t.fund_account_id,
     sum(t.done_amount) as done_amount,
     sum(t.done_money) as done_money
      from tmp t
     inner join CF_BUSIMG.TMP_COCKPIT_00110_2 a
        on t.fund_account_id = a.fund_account_id
       and t.busi_date between a.real_begin_date and a.real_end_date
     group by /*t.busi_date,*/ t.fund_account_id;
  commit;

  /*  --按照结束日期所在月份获取 “权益分配表——FOF产品”涉及到IB业务部客户的字段“分配日均权益合计” 如果本月没有数据，取上月数据
  --CF_BUSIMG.TMP_COCKPIT_00110_7
  CF_BUSIMG.P_COCKPIT_00110_BEFORE(V_END_MONTH,
                                   O_RETURN_MSG,
                                   O_RETURN_CODE);
  select count(1) into v_fof_count from CF_BUSIMG.TMP_COCKPIT_00110_7 t;

  if v_fof_count = 0 then
    --本月没有数据，取上月数据
    CF_BUSIMG.P_COCKPIT_00110_BEFORE(v_last_month,
                                     O_RETURN_MSG,
                                     O_RETURN_CODE);
  end if;*/

  --产品对应的客户总销售份额和业务人员对应的销售占比
  execute immediate 'truncate table CF_BUSIMG.TMP_COCKPIT_00110_6';
  INSERT INTO CF_BUSIMG.TMP_COCKPIT_00110_6
    (FILING_CODE,
     OA_BRANCH_ID,
     OA_BROKER_NAME,
     PRODUCT_TOTAL_SHARE,
     CLIENT_CONFIRM_SHARE,
     OA_BROKER_NAME_PROP)
    with tmp as
     (
      --客户证件号码，客户保有份额
      select a.filing_code,
              a.product_name,
              a.client_name,
              a.id_no,
              sum(a.confirm_share) as client_confirm_share
        from CF_BUSIMG.T_COCKPIT_00096 a
       where a.busi_date between v_BEGIN_DATE and v_END_DATE

       group by a.filing_code, a.product_name, a.client_name, a.id_no),
    tmp1 as
     ( --产品总分配份额
      select t.month_id,
              t.filing_code,
              t.product_name,
              t.PRODUCT_TOTAL_SHARE,
              row_number() over(partition by t.filing_code order by t.month_id desc) as rn
        from CF_BUSIMG.T_COCKPIT_00095 t),
    tmp2 as
     (select t.filing_code, t.PRODUCT_TOTAL_SHARE from tmp1 t where rn = '1'

      ),
    tmp3 as
     (select t.filing_code,
             t.product_name,
             b.product_total_share,
             t.client_name,
             a.id_no,
             a.client_confirm_share,
             t.oa_branch_id,
             t.oa_branch_name,
             t.oa_broker_name,
             t.oa_broker_name_rate
        from CF_BUSIMG.T_COCKPIT_00097 t
        left join tmp a
          on t.client_name = a.client_name
         and t.filing_code = a.filing_code
        left join tmp2 b
          on t.filing_code = b.filing_code),
    tmp4 as
     (select t.filing_code,
             t.oa_branch_id,
             t.oa_broker_name,
             t.product_total_share,
             sum(t.client_confirm_share) as client_confirm_share, --客户当前持有份额
             sum(t.client_confirm_share * t.oa_broker_name_rate) as oa_broker_product_share
        from tmp3 t
       group by t.filing_code,
                t.oa_branch_id,
                t.oa_broker_name,
                t.product_total_share)
    select t.filing_code,
           t.oa_branch_id,
           t.oa_broker_name,
           t.product_total_share,
           t.client_confirm_share,
           (case
             when t.PRODUCT_TOTAL_SHARE <> 0 then
              nvl(t.oa_broker_product_share, 0) / t.PRODUCT_TOTAL_SHARE
             else
              0
           end) as oa_broker_name_prop
      from tmp4 t;

  commit;

  execute immediate 'truncate table CF_BUSIMG.TMP_COCKPIT_00110_7';
  INSERT INTO CF_BUSIMG.TMP_COCKPIT_00110_7
    (FILING_CODE,
     PRODUCT_NAME,
     OA_BRANCH_ID,
     OA_BRANCH_NAME,
     OA_BROKER_ID,
     OA_BROKER_NAME,
     SALES_PRODUCT_AVG_RIGHTS,
     ADVISOR_AVG_RIGHTS,
     SUM_AVG_RIGHTS,
     SALES_SHARE,
     ADVISOR_NAME,
     FOF_AVG_RIGHTS)
    with tmp as
     (select a.filing_code,
             a.product_name,
             sum(case
                   when v_trade_days > 0 then
                    t.rights / v_trade_days
                   else
                    0
                 end) as avg_rights
        from cf_sett.t_client_sett t
       inner join CF_BUSIMG.T_COCKPIT_00095_1 a
          on t.fund_account_id = a.fund_account_id
         and a.month_id = I_MONTH_ID
       where t.busi_date between v_begin_date and v_end_date
       group by a.filing_code, a.product_name),
    tmp1 as
     (select t.filing_code,
             t.product_name, --FOF产品
             t.oa_branch_id,
             t.oa_branch_name, --部门名称
             T.OA_BROKER_ID,
             t.oa_broker_name, --业务人员
             (nvl(a.avg_rights, 0) * (1 - t.OA_BRANCH_ADVISOR_RATE) *
             nvl(b.oa_broker_name_prop, 0)) as sales_product_avg_rights, --销售产品
             (nvl(a.avg_rights, 0) * t.OA_BRANCH_ADVISOR_RATE *
             t.OA_BROKER_ADVISOR_RATE) as advisor_avg_rights, --推荐投顾   日均权益*部门推荐投顾比例*人员推荐投顾比例
             --合计(销售产品+推荐投顾)
             nvl(b.client_confirm_share, 0) as sales_share, --销售份额
             t.ADVISOR_NAME, --推荐投顾名称
             nvl(a.avg_rights, 0) as fof_avg_rights --FOF产品落地经纪端本月日均权益
        from CF_BUSIMG.T_COCKPIT_00100 t
        left join tmp a
          on t.filing_code = a.filing_code
        left join CF_BUSIMG.TMP_COCKPIT_00110_6 b
          on t.FILING_CODE = b.filing_code
         and t.OA_BRANCH_ID = b.oa_branch_id
         and t.OA_BROKER_NAME = b.oa_broker_name
       where t.busi_month = I_MONTH_ID

      )
    select t.FILING_CODE,
           t.PRODUCT_NAME,
           t.OA_BRANCH_ID,
           t.OA_BRANCH_NAME,
           T.OA_BROKER_ID,
           t.OA_BROKER_NAME,
           t.SALES_PRODUCT_AVG_RIGHTS,
           t.ADVISOR_AVG_RIGHTS,
           (t.SALES_PRODUCT_AVG_RIGHTS + t.ADVISOR_AVG_RIGHTS) as SUM_AVG_RIGHTS,
           t.SALES_SHARE,
           t.ADVISOR_NAME,
           t.FOF_AVG_RIGHTS
      from tmp1 t;

  commit;

  --按照业务人员汇总数据
  delete from CF_BUSIMG.T_COCKPIT_00179 t where t.month_id = I_MONTH_ID;
  commit;
  INSERT INTO CF_BUSIMG.T_COCKPIT_00179
    (month_id,
     oa_branch_id,
     END_RIGHTS,
     AVG_RIGHTS,
     DONE_AMOUNT,
     DONE_MONEY)
    with tmp as
     (select v_BEGIN_DATE || '-' || v_END_DATE as busi_date, --日期

             e.oa_branch_id,
             t.fund_account_id,
             nvl(b.end_rights, 0) * nvl(t.DATA_PCT, 1) as end_rights, --期末权益
             nvl(b.avg_rights, 0) * nvl(t.DATA_PCT, 1) as avg_rights, --日均权益
             nvl(c.done_amount, 0) * nvl(t.DATA_PCT, 1) as done_amount, --成交手数
             nvl(c.done_money, 0) * nvl(t.DATA_PCT, 1) as done_money -- 成交金额
        from CF_BUSIMG.TMP_COCKPIT_00110_1 t

        left join CF_BUSIMG.TMP_COCKPIT_00110_3 b
          on t.fund_account_id = b.fund_account_id
        left join CF_BUSIMG.TMP_COCKPIT_00110_4 c
          on t.fund_account_id = c.fund_account_id
        left join cf_sett.t_fund_account d
          on t.fund_account_id = d.fund_account_id
       inner join cf_busimg.t_ctp_branch_oa_rela e
          on d.branch_id = e.ctp_branch_id
       where e.oa_branch_id is not null),
    tmp1 as
     (select t.oa_branch_id,
             sum(t.end_rights) as end_rights, --期末权益
             sum(t.avg_rights) as avg_rights, --日均权益
             sum(t.done_amount) as done_amount, --成交手数
             sum(t.done_money) as done_money -- 成交金额
        from tmp t
       group by t.oa_branch_id),
    tmp2 as
     (
      --权益分配表——FOF产品 的日均权益合计
      select t.OA_BRANCH_ID, sum(t.SUM_AVG_RIGHTS) as SUM_AVG_RIGHTS
        from CF_BUSIMG.TMP_COCKPIT_00110_7 t
       group by t.OA_BRANCH_ID)
    select I_MONTH_ID as month_id,
           t.OA_BRANCH_ID,
           t.end_rights,
           t.avg_rights + nvl(a.SUM_AVG_RIGHTS, 0) as avg_rights,
           t.done_amount,
           t.done_money
      from tmp1 t
      left join tmp2 a
        on t.OA_BRANCH_ID = a.OA_BRANCH_ID;
  COMMIT;

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

