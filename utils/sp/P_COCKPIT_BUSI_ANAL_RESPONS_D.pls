create or replace procedure cf_busimg.P_COCKPIT_BUSI_ANAL_RESPONS_D(i_busi_date   in varchar2,
                                                                  o_return_code out integer,
                                                                  o_return_msg  out varchar2) is
  --========================================================================================================================
  --系统名称:生成驾驶舱数据
  --模块名称:
  --模块编号:
  --模块描述:经营分析-分管部门-单日期落地
  --开发人员:zhongying.zhang
  --目前版本:
  --创建时间:20230606
  --版    权:
  --修改历史:

  --========================================================================================================================
  v_op_object  VARCHAR2(50) DEFAULT 'P_COCKPIT_BUSI_ANA_LINE_D_DATA'; -- '操作对象';
  v_error_msg  VARCHAR2(200); --返回信息
  v_error_code INTEGER;
  v_userException EXCEPTION;

  v_new_begin_date varchar2(8); --新增客户开始日期
  v_new_end_date   varchar2(8); --新增客户结束日期
  v_yoy_busi_date  varchar2(8); --同比日期

begin

  v_new_begin_date := substr(i_busi_date, 1, 4) || '0101';
  v_new_end_date   := i_busi_date;

  --同比日期
  v_yoy_busi_date := to_char(add_months(to_date(I_BUSI_DATE, 'yyyymmdd'),
                                        -12),
                             'yyyymmdd')

   ;

  select max(t.busi_date)
    into v_yoy_busi_date
    from cf_sett.t_pub_date t
   where t.busi_date <= v_yoy_busi_date
     and t.market_no = '1'
     and t.trade_flag = '1';


  DELETE FROM CF_BUSIMG.T_COCKPIT_BUSI_ANAL_RESPONS_D T
  WHERE T.BUSI_DATE=i_busi_date;
  COMMIT;

  --初始化数据
  insert into CF_BUSIMG.T_COCKPIT_BUSI_ANAL_RESPONS_D
    (BUSI_DATE, RESPONS_LINE_ID)
   select i_busi_date as busi_month, t.RESPONS_LINE_ID
      from CF_BUSIMG.t_Respons_Line t
      where t.if_use='1';
  commit;

  --更新数据
  --业务结构-期末权益-存量客户
  --业务结构-期末权益-存量客户占比
  --业务结构-期末权益-新增客户     新增按照当年1月1日，新增数据
  --业务结构-期末权益-新增客户占比
  --业务指标-期末权益
  --业务指标-期末权益同比
  merge into CF_BUSIMG.T_COCKPIT_BUSI_ANAL_RESPONS_D a
  using (
    with tmp_new as
     (
      --新增客户
      select t.fund_account_id,
              (case
                when b.open_date between v_new_begin_date and v_new_end_date then
                 '1'
                else
                 '0'
              end) as is_new_flag,
              d.RESPONS_LINE_ID,
              sum(t.rights) as end_rights
        from cf_sett.t_client_sett t
        left join cf_sett.t_fund_account b
          on t.fund_account_id = b.fund_account_id
       inner join cf_busimg.t_ctp_branch_oa_rela c
          on b.branch_id = c.ctp_branch_id
         inner join CF_BUSIMG.T_OA_BRANCH d
          on c.oa_branch_id = d.departmentid
       where t.busi_date = i_busi_date
         and d.RESPONS_LINE_ID is not null
       group by t.fund_account_id,
                 (case
                   when b.open_date between v_new_begin_date and v_new_end_date then
                    '1'
                   else
                    '0'
                 end),
                 d.RESPONS_LINE_ID),
    tmp_yoy as
     (
      --同比日期
      select t.fund_account_id, d.RESPONS_LINE_ID, sum(t.rights) as end_rights
        from cf_sett.t_client_sett t
        left join cf_sett.t_fund_account b
          on t.fund_account_id = b.fund_account_id
       inner join cf_busimg.t_ctp_branch_oa_rela c
          on b.branch_id = c.ctp_branch_id
          inner join CF_BUSIMG.T_OA_BRANCH d
          on c.oa_branch_id = d.departmentid
       where t.busi_date = v_yoy_busi_date
         and d.RESPONS_LINE_ID is not null
       group by t.fund_account_id, d.RESPONS_LINE_ID),
    tmp_result as
     (select t.RESPONS_LINE_ID,
             sum(t.end_rights) as total_end_rights,
             sum(case
                   when t.is_new_flag = '1' then
                    t.end_rights
                   else
                    0
                 end) as new_end_rights,
             sum(case
                   when t.is_new_flag = '0' then
                    t.end_rights
                   else
                    0
                 end) as stock_end_rights
        from tmp_new t
       group by t.RESPONS_LINE_ID),
    tmp_result1 as
     (select t.RESPONS_LINE_ID, sum(t.end_rights) as total_end_rights

        from tmp_yoy t
       group by t.RESPONS_LINE_ID)
    select t.RESPONS_LINE_ID,
           t.stock_end_rights/10000 as stock_end_rights,
           (case
             when t.total_end_rights <> 0 then
              t.stock_end_rights / t.total_end_rights
             else
              0
           end)*100 as STOCK_END_RIGHTS_PROP,
           t.NEW_END_RIGHTS/10000 as NEW_END_RIGHTS ,
           (case
             when t.total_end_rights <> 0 then
              t.NEW_END_RIGHTS / t.total_end_rights
             else
              0
           end)*100 as NEW_END_RIGHTS_PROP,
           t.total_end_rights/10000 as END_RIGHTS,
           (case
             when a.total_end_rights <> 0 then
              t.total_end_rights / a.total_end_rights - 1
             else
              0
           end)*100 as END_RIGHTS_YOY
      from tmp_result t
      left join tmp_result1 a
        on t.RESPONS_LINE_ID = a.RESPONS_LINE_ID

     ) y on (a.RESPONS_LINE_ID = y.RESPONS_LINE_ID and a.busi_date = i_busi_date) when matched then
      update
         set a.STOCK_END_RIGHTS      = y.STOCK_END_RIGHTS,
             a.STOCK_END_RIGHTS_PROP = y.STOCK_END_RIGHTS_PROP,
             a.NEW_END_RIGHTS        = y.NEW_END_RIGHTS,
             a.NEW_END_RIGHTS_PROP   = y.NEW_END_RIGHTS_PROP,
             a.END_RIGHTS            = y.END_RIGHTS,
             a.END_RIGHTS_YOY        = y.END_RIGHTS_YOY;
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
