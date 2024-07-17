CREATE OR REPLACE PROCEDURE CF_BUSIMG.P_COCKPIT_00178_DATA(I_MONTH_ID    IN VARCHAR2,
                                                           O_RETURN_MSG  OUT VARCHAR2, --返回消息
                                                           O_RETURN_CODE OUT INTEGER --返回代码
                                                           ) IS
  ---------------------------------------------------------------------------------------
  -- copyright              wolf 1.0
  -- func_create_begin
  -- func_id                P_COCKPIT_00178_DATA
  -- version                1.0
  -- func_name
  -- func_remark            IB协同收入统计汇总表-落地数据
  -- create_date            20240629
  -- create_programer       zhongying.zhang
  -- modify_remark
  -- func_create_end
  ---------------------------------------------------------------------------------------
  --固定变量
  ---------------------------------------------------------------------------------------
  v_op_object  VARCHAR2(50) DEFAULT 'P_COCKPIT_00178_DATA'; -- '操作对象';
  v_error_msg  VARCHAR2(200); --返回信息
  v_error_code INTEGER;
  v_userException EXCEPTION;
  ---------------------------------------------------------------------------------------
  --业务变量
  v_begin_trade_date varchar2(8); --开始交易日期
  v_end_trade_date   varchar2(8); --结束交易日期
  v_trade_days       number; --交易日天数
  ---------------------------------------------------------------------------------------

BEGIN
  select min(t.busi_date), max(t.busi_date), count(1)
    into v_begin_trade_date, v_end_trade_date, v_trade_days
    from cf_sett.t_pub_date t
   where substr(t.busi_date, 1, 6) = I_MONTH_ID
     and t.market_no = '1'
     and t.trade_flag = '1';

  --计算  期初权益  期末权益  日均权益  留存手续费
  execute immediate 'truncate table CF_BUSIMG.TMP_COCKPIT_00109_1';
  insert into CF_BUSIMG.TMP_COCKPIT_00109_1
    (BRANCH_ID,
     BRANCH_NAME,
     YES_RIGHTS,
     END_RIGHTS,
     AVG_RIGHTS,
     REMAIN_TRANSFEE)
    with tmp as
     (select t.regist_date as busi_date,
             t.fund_account_id,
             t.branch_id,
             t.branch_name
        from CF_BUSIMG.T_COCKPIT_00107 t

       where t.service_type = '1' --服务类型(1:IB协同服务，2：IB驻点服务)
       group by t.regist_date, t.fund_account_id, t.branch_id, t.branch_name)
    select a.branch_id,
           a.branch_name,
           sum(case
                 when t.busi_date = v_begin_trade_date then
                  t.yes_rights
                 else
                  0
               end) as yes_rights,
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
               end) as avg_rights,
           sum(t.transfee + t.delivery_transfee + t.strikefee -
               t.market_transfee - t.market_delivery_transfee -
               t.market_strikefee) as remain_transfee
      from cf_sett.t_client_sett t
     inner join tmp a
        on t.fund_account_id = a.fund_account_id
     where t.busi_date between v_begin_trade_date and v_end_trade_date
     group by a.branch_id, a.branch_name;
  commit;

  --计算IB协同收入
  execute immediate 'truncate table CF_BUSIMG.TMP_COCKPIT_00109_2';
  insert into CF_BUSIMG.TMP_COCKPIT_00109_2
    (BRANCH_ID, BRANCH_NAME, COOPE_INCOME)
    with tmp as
     (select t.regist_date as busi_date, t.fund_account_id
        from CF_BUSIMG.T_COCKPIT_00107 t
       where t.service_type = '1' --服务类型(1:IB协同服务，2：IB驻点服务)
       group by t.regist_date, t.fund_account_id),
    tmp1 as --留存手续费
     (select t.busi_date,
             t.fund_account_id,
             sum(t.transfee + t.delivery_transfee + t.strikefee -
                 t.market_transfee - t.market_delivery_transfee -
                 t.market_strikefee) as remain_transfee
        from cf_sett.t_client_sett t
       inner join tmp a
          on t.fund_account_id = a.fund_account_id
       where t.busi_date between v_begin_trade_date and v_end_trade_date
       group by t.busi_date, t.fund_account_id),
    tmp2 as
     (select a.busi_date,
             t.branch_id,
             t.branch_name,
             t.fund_account_id,
             (a.remain_transfee * t.coope_income_reate) as coope_income
        from CF_BUSIMG.T_COCKPIT_00107 t
       inner join tmp1 a
          on t.fund_account_id = a.fund_account_id)
    select t.branch_id, t.branch_name, sum(t.coope_income) as coope_income
      from tmp2 t
     group by t.branch_id, t.branch_name;
  commit;

  DELETE FROM CF_BUSIMG.T_COCKPIT_00178 T WHERE T.MONTH_ID = I_MONTH_ID;
  COMMIT;

  INSERT INTO CF_BUSIMG.T_COCKPIT_00178
    (MONTH_ID,
     BRANCH_ID,
     YES_RIGHTS,
     END_RIGHTS,
     AVG_RIGHTS,
     REMAIN_TRANSFEE,
     COOPE_INCOME)
    select I_MONTH_ID AS MONTH_ID,
           t.branch_id,
           t.yes_rights, --期初权益
           t.end_rights, --期末权益
           t.avg_rights, --日均权益
           t.remain_transfee, --留存手续费
           a.coope_income --IB协同收入
      from CF_BUSIMG.TMP_COCKPIT_00109_1 t
      left join CF_BUSIMG.TMP_COCKPIT_00109_2 a
        on t.branch_id = a.branch_id

    ;
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

