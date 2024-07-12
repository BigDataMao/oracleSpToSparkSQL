create or replace procedure cf_busimg.P_COCKPIT_BUSI_ANAL_TAR_RESP_Q(I_MONTH_ID    in varchar2,
                                                                     o_return_code out integer,
                                                                     o_return_msg  out varchar2) is
  --========================================================================================================================
  --系统名称:生成驾驶舱数据
  --模块名称:
  --模块编号:
  --模块描述： CF_BUSIMG.T_COCKPIT_BUSI_ANAL_TAR_RESP_Q  经营分析-分管部门-经营目标完成情况-按季度落地
  --开发人员:zhongying.zhang
  --目前版本:
  --创建时间:20230408
  --版    权:
  --修改历史:

  --========================================================================================================================
  v_op_object  VARCHAR2(50) DEFAULT 'P_COCKPIT_BUSI_ANAL_TAR_RESP_Q'; -- '操作对象';
  v_error_msg  VARCHAR2(200); --返回信息
  v_error_code INTEGER;
  v_userException EXCEPTION;
  v_busi_year    varchar2(4); --业务年份
  v_BUSI_QUARTER varchar2(10); --业务季度(1:第一季度，2：第二季度，3：第三季度，4：第四季度)

  v_busi_month number; --月份

begin
  v_busi_year := substr(I_MONTH_ID, 1, 4);

  v_busi_month := TRUNC(to_number(substr(I_MONTH_ID, 5, 2)) / 3);

  if v_busi_month = 1 then
    v_BUSI_QUARTER := '1';
  elsif v_busi_month = 2 then
    v_BUSI_QUARTER := '2';
  elsif v_busi_month = 3 then
    v_BUSI_QUARTER := '3';

  elsif v_busi_month = 4 then
    v_BUSI_QUARTER := '4';
  end if;

  delete from CF_BUSIMG.T_COCKPIT_BUSI_ANAL_TAR_RESP_Q t
   where t.busi_year = v_busi_year
   and t.busi_quarter=v_BUSI_QUARTER
   ;
  commit;

  /*
  考核指标：
  001：考核收入
  002：经纪业务手续费收入市占率
  003：考核日均权益
  004：日均权益市占率
  005：考核利润
  006：成交额
  007：成交额市占率
  008：成交量
  009：成交量市占率
  010：新增直接开发有效客户数量
  011：新增有效客户数量
  012：产品销售额
  */
  --初始化数据
  INSERT INTO CF_BUSIMG.T_COCKPIT_BUSI_ANAL_TAR_RESP_Q
    (BUSI_YEAR,BUSI_QUARTER, RESPONS_LINE_ID, BUSI_TYPE, BUSI_TYPE_NAME)
    select v_busi_year as BUSI_YEAR,
           v_BUSI_QUARTER as BUSI_QUARTER,
           t.RESPONS_LINE_ID,
           a.busi_type,
           a.busi_type_name
      from CF_BUSIMG.t_Respons_Line t
     inner join CF_BUSIMG.T_BUSI_ANAL_TARGET_TYPE a
        on 1 = 1
     where t.if_use='1';
  commit;

   --更新各个指标的数据
  merge into CF_BUSIMG.T_COCKPIT_BUSI_ANAL_TAR_RESP_Q x
  using (
    with tmp as
     (select a.respons_line_id,
             t.QUARTER_ID,
             (case
               when (t.index_type = '0' and t.index_asses_benchmark = '1') then
                '001' --考核收入
               when (t.index_type = '1' and t.index_asses_benchmark = '1') then
                '002' --经纪业务手续费收入市占率
               when (t.index_type = '0' and t.index_asses_benchmark = '2') then
                '003' --考核日均权益
               when (t.index_type = '1' and t.index_asses_benchmark = '2') then
                '004' --日均权益市占率
               when (t.index_type = '0' and t.index_asses_benchmark = '3') then
                '005' --考核利润
               when (t.index_type = '0' and t.index_asses_benchmark = '4') then
                '006' --成交额
               when (t.index_type = '1' and t.index_asses_benchmark = '4') then
                '007' --成交额市占率
               when (t.index_type = '0' and t.index_asses_benchmark = '5') then
                '008' --成交量
               when (t.index_type = '1' and t.index_asses_benchmark = '5') then
                '009' --成交量市占率
               when (t.index_type = '0' and t.index_asses_benchmark = '6' and
                    t.index_name like '%新增直接开发有效客户数量%') then
                '010' --新增直接开发有效客户数量
               when (t.index_type = '0' and t.index_asses_benchmark = '6' and
                    t.index_name like '%新增有效客户数量%') then
                '011' --新增有效客户数量
               when (t.index_type = '0' and t.index_asses_benchmark = '7') then
                '012' --产品销售额
               else
                ''
             end) as busi_type, --001：考核收入
             sum(t.year_target_value) as year_target_value, --年度目标值
             sum(t.complet_value) as complete_value --完成值
        from cf_busimg.t_cockpit_00138 t
       inner join CF_BUSIMG.T_OA_BRANCH a
          on t.oa_branch_id = a.departmentid
       where t.year_id = v_busi_year
         and t.quarter_id = v_BUSI_QUARTER
         and a.respons_line_id is not null
       group by a.respons_line_id,
                t.QUARTER_ID,
                (case
                  when (t.index_type = '0' and t.index_asses_benchmark = '1') then
                   '001' --考核收入
                  when (t.index_type = '1' and t.index_asses_benchmark = '1') then
                   '002' --经纪业务手续费收入市占率
                  when (t.index_type = '0' and t.index_asses_benchmark = '2') then
                   '003' --考核日均权益
                  when (t.index_type = '1' and t.index_asses_benchmark = '2') then
                   '004' --日均权益市占率
                  when (t.index_type = '0' and t.index_asses_benchmark = '3') then
                   '005' --考核利润
                  when (t.index_type = '0' and t.index_asses_benchmark = '4') then
                   '006' --成交额
                  when (t.index_type = '1' and t.index_asses_benchmark = '4') then
                   '007' --成交额市占率
                  when (t.index_type = '0' and t.index_asses_benchmark = '5') then
                   '008' --成交量
                  when (t.index_type = '1' and t.index_asses_benchmark = '5') then
                   '009' --成交量市占率
                  when (t.index_type = '0' and t.index_asses_benchmark = '6' and
                       t.index_name like '%新增直接开发有效客户数量%') then
                   '010' --新增直接开发有效客户数量
                  when (t.index_type = '0' and t.index_asses_benchmark = '6' and
                       t.index_name like '%新增有效客户数量%') then
                   '011' --新增有效客户数量
                  when (t.index_type = '0' and t.index_asses_benchmark = '7') then
                   '012' --产品销售额
                  else
                   ''
                end))
    select t.respons_line_id,
           t.QUARTER_ID,
           t.busi_type,
           t.complete_value,
           (case
             when t.year_target_value <> 0 then
              t.complete_value / t.year_target_value
             else
              0
           end)*100 as complete_value_rate
      from tmp t) y
        on (x.busi_year = v_busi_year and x.BUSI_QUARTER = y.QUARTER_ID and
           x.respons_line_id = y.respons_line_id and
           x.busi_type = y.busi_type) when matched then
      update
         set x.complete_value      = y.complete_value,
             x.complete_value_rate = y.complete_value_rate;
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
