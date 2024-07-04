create or replace procedure cf_busimg.P_COCKPIT_BU_ANAL_RESP_TOP_PD(I_MONTH_ID    in varchar2,
                                                                     o_return_code out integer,
                                                                     o_return_msg  out varchar2) is
  --========================================================================================================================
  --系统名称:生成驾驶舱数据
  --模块名称:
  --模块编号:
  --模块描述： CF_BUSIMG.T_COCKPIT_BU_ANAL_RESP_TOP_PD 经营分析-分管部门-成交品种排名落地
  --开发人员:zhongying.zhang
  --目前版本:
  --创建时间:20230615
  --版    权:
  --修改历史:

  --========================================================================================================================
  v_op_object  VARCHAR2(50) DEFAULT 'P_COCKPIT_BU_ANAL_RESP_TOP_PD'; -- '操作对象';
  v_error_msg  VARCHAR2(200); --返回信息
  v_error_code INTEGER;
  v_userException EXCEPTION;

  v_begin_date varchar2(8);
  v_end_date   varchar2(8);
begin
  select min(t.busi_date), max(t.busi_date)
    into v_begin_date, v_end_date
    from cf_sett.t_pub_date t
   where substr(t.busi_date,1,6)=I_MONTH_ID
     and t.market_no = '1'
     and t.trade_flag = '1';

  delete from CF_BUSIMG.T_COCKPIT_BU_ANAL_RESP_TOP_PD t
   where t.busi_month = I_MONTH_ID;
  commit;


  INSERT INTO CF_BUSIMG.T_COCKPIT_BU_ANAL_RESP_TOP_PD
    (BUSI_MONTH,
     RESPONS_LINE_ID,
     PRODUCT_ID,
     PRODUCT_NAME,
     DONE_AMOUNT,
     DONE_MONEY,
     RANK_DONE_AMOUNT,
     RANK_DONE_MONEY)
    with tmp as
     (SELECT t.product_id,
             d.product_name,
             e.RESPONS_LINE_ID,
             SUM(T.Done_Amt) AS Done_amount,
             SUM(t.done_sum) AS done_money
        FROM CF_SETT.t_Hold_Balance T
        LEFT JOIN CF_SETT.T_FUND_ACCOUNT B
          ON T.FUND_ACCOUNT_ID = B.FUND_ACCOUNT_ID
       INNER JOIN CF_BUSIMG.T_CTP_BRANCH_OA_RELA C
          ON B.BRANCH_ID = C.CTP_BRANCH_ID
        left join cf_sett.t_product d
          on t.product_id = d.product_id
         and t.market_id = d.market_id
         and t.trade_type = d.trade_type
        inner join CF_BUSIMG.T_OA_BRANCH e
          on c.oa_branch_id = e.departmentid
       WHERE T.BUSI_DATE BETWEEN v_begin_date AND v_end_date
         AND C.oa_branch_id IS NOT NULL
           and e.RESPONS_LINE_ID is not null
       GROUP BY t.product_id, d.product_name, e.RESPONS_LINE_ID)
    select I_MONTH_ID as busi_month,
           t.RESPONS_LINE_ID,
           t.product_id,
           t.product_name,
            t.Done_amount/10000 as Done_amount, --万手 20240626
           t.done_money/100000000 as done_money, --亿元 20240626
           row_number() over(partition by t.RESPONS_LINE_ID order by t.Done_amount desc) as RANK_DONE_AMOUNT,
           row_number() over(partition by t.RESPONS_LINE_ID order by t.done_money desc) RANK_DONE_MONEY
      from tmp t;

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
