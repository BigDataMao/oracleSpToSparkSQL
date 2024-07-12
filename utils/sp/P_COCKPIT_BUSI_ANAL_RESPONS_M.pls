create or replace procedure cf_busimg.P_COCKPIT_BUSI_ANAL_RESPONS_M(i_month_id    in varchar2,
                                                                    o_return_code out integer,
                                                                    o_return_msg  out varchar2) is
  --========================================================================================================================
  --系统名称:生成驾驶舱数据
  --模块名称:
  --模块编号:
  --模块描述:经营分析-分管部门-按月落地
  --开发人员:zhongying.zhang
  --目前版本:
  --创建时间:20230606
  --版    权:
  --修改历史:

  --========================================================================================================================
  v_op_object  VARCHAR2(50) DEFAULT 'P_COCKPIT_BUSI_ANA_LINE_M_DATA'; -- '操作对象';
  v_error_msg  VARCHAR2(200); --返回信息
  v_error_code INTEGER;
  v_userException EXCEPTION;

  v_begin_date varchar2(8); --开始日期
  v_end_date   varchar2(8); --结束日期

  v_new_begin_date varchar2(8); --新增客户开始日期
  v_new_end_date   varchar2(8); --新增客户结束日期

  v_busi_trade_days   number; --当前交易日天数
  v_total_done_amount number; --市场总成交金量
  v_total_done_money  number; --市场总成交金额
  v_total_index_value number; --市场总手续费收入
  v_total_rights      number; --市场总权益

  v_now_begin_date varchar2(8); --当月开始日期
  v_now_end_date   varchar2(8); --当月结束日期
  v_now_trade_days number; --当月交易日天数

  v_yoy_begin_date varchar2(8); --同比开始日期(单月)
  v_yoy_end_date   varchar2(8); --同比结束日期 (单月)
  v_yoy_month_id   varchar(6); --同比月份 (单月)
  v_yoy_trade_days number; --同比交易日天数 (单月)

  v_yoy_all_begin_date varchar2(8); --同比开始日期(从去年1号开始)
  v_yoy_all_end_date   varchar2(8); --同比结束日期(从去年同比日期结束)

  v_begin_month varchar2(6); --开始月份，当年1月

begin
  v_begin_date  := substr(i_month_id, 1, 4) || '0101';
  v_begin_month := substr(i_month_id, 1, 4) || '01';

  select max(t.busi_date), min(t.busi_date), count(1)
    into v_end_date, v_now_begin_date, v_now_trade_days
    from cf_sett.t_pub_date t
   where substr(t.busi_date, 1, 6) = i_month_id
     and t.busi_date <= to_char(sysdate, 'yyyymmdd')
     and t.market_no = '1'
     and t.trade_flag = '1';

  v_new_begin_date := substr(i_month_id, 1, 4) || '0101'; --新增客户开始日期  20240626
  v_new_end_date   := v_end_date;
  v_now_end_date   := v_end_date;

  --同比月份 20240626
  v_yoy_month_id := to_char(add_months(to_date(i_month_id, 'yyyymm'), -12),
                            'yyyymm');

  --同比日期 20240626
  select min(t.busi_date), max(t.busi_date), count(1)
    into v_yoy_begin_date, v_yoy_end_date, v_yoy_trade_days
    from cf_sett.t_pub_date t
   where substr(t.busi_date, 1, 6) = v_yoy_month_id
     and t.trade_flag = '1'
     and t.trade_flag = '1';

  --同比开始日期(从去年1号开始)
  v_yoy_all_begin_date := substr(v_yoy_month_id, 1, 4) || '0101';
  v_yoy_all_end_date   := to_char(add_months(to_date(v_end_date, 'yyyymmdd'),
                                             -12),
                                  'yyyymmdd');

  --当前交易日天数 202406026
  --从当年1号到当月最后一个交易日
  select count(1)
    into v_busi_trade_days
    from cf_sett.t_pub_date t
   where t.busi_date between v_begin_date and v_end_date
     and t.trade_flag = '1'
     and t.trade_flag = '1';

  DELETE FROM CF_BUSIMG.T_COCKPIT_BUSI_ANAL_RESPONS_M T
   WHERE T.BUSI_MONTH = i_month_id;
  COMMIT;

  --初始化数据
  insert into CF_BUSIMG.T_COCKPIT_BUSI_ANAL_RESPONS_M
    (BUSI_MONTH, RESPONS_LINE_ID)
    select i_month_id as BUSI_MONTH, t.RESPONS_LINE_ID
      from CF_BUSIMG.t_Respons_Line t
     where t.if_use = '1';
  commit;

  -----------------------------协同业务begin--------------------------------------------------------------------------------
  --协同业务-产品销售规模-保有量
  --协同业务-产品销售规模-新增量
  /*
  月度销售人员保有奖励分配情况—客户保有份额 选择当前月份，显示历史到当月的数据
  月度销售人员保有奖励分配情况—新增量  选择当前月份，显示当年1月到当月数据
  过程：CF_BUSIMG.P_COCKPIT_00099
  */
  merge into CF_BUSIMG.T_COCKPIT_BUSI_ANAL_RESPONS_M a
  using (
    with tmp as
     (
      --保有量
      select d.RESPONS_LINE_ID,
              sum(t.confirm_share) as XT_PRODUCT_SALES_STOCK
        from CF_BUSIMG.T_COCKPIT_00096 t
       inner join cf_sett.t_client b --必须是在客户表中的客户
          on t.client_name = b.client_name
       inner join cf_busimg.t_ctp_branch_oa_rela c
          on b.branch_id = c.ctp_branch_id
       inner join CF_BUSIMG.T_OA_BRANCH d
          on c.oa_branch_id = d.departmentid
       where t.busi_date <= v_end_date
         and D.RESPONS_LINE_ID is not null
       group by d.RESPONS_LINE_ID),
    tmp_new as
     (
      --新增量
      select d.RESPONS_LINE_ID,
              sum(case
                    when t.wh_trade_type in ('0', '1') then
                     t.confirm_share
                    else
                     0
                  end) as XT_PRODUCT_SALES_NEW
        from CF_BUSIMG.T_COCKPIT_00096 t
       inner join cf_sett.t_client b --必须是在客户表中的客户
          on t.client_name = b.client_name
       inner join cf_busimg.t_ctp_branch_oa_rela c
          on b.branch_id = c.ctp_branch_id
       inner join CF_BUSIMG.T_OA_BRANCH d
          on c.oa_branch_id = d.departmentid
       where t.busi_date between v_begin_date and v_end_date
         and D.RESPONS_LINE_ID is not null
       group by d.RESPONS_LINE_ID)
    select t.RESPONS_LINE_ID,
           nvl(b.XT_PRODUCT_SALES_STOCK, 0) / 10000 as XT_PRODUCT_SALES_STOCK,
           nvl(c.XT_PRODUCT_SALES_NEW, 0) / 10000 as XT_PRODUCT_SALES_NEW
      from CF_BUSIMG.T_COCKPIT_BUSI_ANAL_RESPONS_M t
      left join tmp b
        on t.RESPONS_LINE_ID = b.RESPONS_LINE_ID
      left join tmp_new c
        on t.RESPONS_LINE_ID = c.RESPONS_LINE_ID
     where t.busi_month = i_month_id

     ) y on (a.RESPONS_LINE_ID = y.RESPONS_LINE_ID and a.busi_month = i_month_id) when matched then
      update
         set a.XT_PRODUCT_SALES_STOCK = y.XT_PRODUCT_SALES_STOCK,
             a.XT_PRODUCT_SALES_NEW   = y.XT_PRODUCT_SALES_NEW;

  commit;

  --协同业务-交易咨询-交易咨询收入 当年到当前月份数据
  --协同业务-交易咨询-销售收入   当年到当前月份数据
  --过程：CF_BUSIMG.P_COCKPIT_00123
  merge into CF_BUSIMG.T_COCKPIT_BUSI_ANAL_RESPONS_M a
  using (
    with tmp as
     (select d.RESPONS_LINE_ID,
             a.ALLOCA_OA_BRANCH_TYPE, --分配部门类型  0:承做部门,1:销售部门
             sum(t.INVEST_TOTAL_SERVICE_FEE * t.kernel_total_rate *
                 a.alloca_kernel_rate) as alloca_income --部门收入 =投资咨询服务费总额(元)*内核总分配比例*部门内核分配比例
        from CF_BUSIMG.T_COCKPIT_00122 t
        left join CF_BUSIMG.T_COCKPIT_00122_1 a
          on t.busi_month = a.busi_month
         and t.client_id = a.client_id
         and t.product_name = a.product_name
       inner join cf_busimg.t_OA_BRANCH d
          on a.ALLOCA_OA_BRANCH_ID = d.departmentid
       where t.busi_month between v_begin_month and i_month_id
         and D.RESPONS_LINE_ID is not null
       group by d.RESPONS_LINE_ID, a.ALLOCA_OA_BRANCH_TYPE)
    select t.RESPONS_LINE_ID,
           sum(case
                 when t.ALLOCA_OA_BRANCH_TYPE in ('0', '1') then
                  t.alloca_income
                 else
                  0
               end) / 10000 as XT_TRADE_ZX_INCOME,
           sum(case
                 when t.ALLOCA_OA_BRANCH_TYPE = '1' then
                  t.alloca_income
                 else
                  0
               end) / 10000 as XT_TRADE_ZX_XC_INCOME
      from tmp t
     group by t.RESPONS_LINE_ID

    ) y
        on (a.RESPONS_LINE_ID = y.RESPONS_LINE_ID and
           a.busi_month = i_month_id) when matched then
      update
         set a.XT_TRADE_ZX_INCOME    = y.XT_TRADE_ZX_INCOME,
             a.XT_TRADE_ZX_XC_INCOME = y.XT_TRADE_ZX_XC_INCOME;

  commit;

  --协同业务-IB协同/驻点业务-日均权益  选择当前月份，显示当前月份
  /*
  日均权益 “IB协同统计汇总表——日均权益”与“驻点人员营销统计数据表——日均权益”之和

  CF_BUSIMG.P_COCKPIT_00109  IB协同统计汇总表
  CF_BUSIMG.P_COCKPIT_00110  驻点人员营销统计数据表

  CF_BUSIMG.P_COCKPIT_00178_DATA IB协同统计汇总表 落地数据
  CF_BUSIMG.P_COCKPIT_00179_DATA 驻点人员营销统计数据表 落地数据
  */

  --协同业务-IB协同/驻点业务-日均权益
  merge into CF_BUSIMG.T_COCKPIT_BUSI_ANAL_RESPONS_M a
  using (
    with tmp as
     (
      --
      select d.RESPONS_LINE_ID, sum(t.AVG_RIGHTS) as AVG_RIGHTS
        from CF_BUSIMG.T_COCKPIT_00178 t
       inner join cf_busimg.t_ctp_branch_oa_rela b
          on t.branch_id = b.ctp_branch_id
       inner join CF_BUSIMG.T_OA_BRANCH d
          on b.oa_branch_id = d.departmentid
       where t.month_id = i_month_id
         and D.RESPONS_LINE_ID is not null
       group by d.RESPONS_LINE_ID

      ),
    tmp1 as
     (select d.RESPONS_LINE_ID, sum(t.AVG_RIGHTS) as AVG_RIGHTS
        from CF_BUSIMG.T_COCKPIT_00179 t
       inner join CF_BUSIMG.T_OA_BRANCH d
          on t.oa_branch_id = d.departmentid
       where t.month_id = i_month_id
         and D.RESPONS_LINE_ID is not null
       group by d.RESPONS_LINE_ID)
    select t.RESPONS_LINE_ID,
           (nvl(b.AVG_RIGHTS, 0) + nvl(c.AVG_RIGHTS, 0)) / 10000 as XT_COLLA_AVG_RIGHTS
      from CF_BUSIMG.T_COCKPIT_BUSI_ANAL_RESPONS_M t
      left join tmp b
        on t.RESPONS_LINE_ID = b.RESPONS_LINE_ID
      left join tmp1 c
        on t.RESPONS_LINE_ID = c.RESPONS_LINE_ID
     where t.busi_month = i_month_id

     ) y on (a.RESPONS_LINE_ID = y.RESPONS_LINE_ID and a.busi_month = i_month_id) when matched then
      update set a.XT_COLLA_AVG_RIGHTS = y.XT_COLLA_AVG_RIGHTS;

  commit;

  --协同业务-IB协同/驻点业务-协同收入
  /*
  “IB协同收入调整表——收入合计”与“IB驻点收入调整表—— 收入合计”之和

  CF_BUSIMG.P_COCKPIT_00125  IB协同收入调整表
  CF_BUSIMG.P_COCKPIT_00142  IB驻点收入调整表

  需要扣税/1.06；
  协同是“ IB协同利息收入、减免收入、协同收入”之和，驻点里是“IB驻点利息收入、IB驻点减免收入、IB驻点留存收入”之和，两项加总

  CF_BUSIMG.P_COCKPIT_00175_DATA  IB协同收入调整表 落地数据
  CF_BUSIMG.P_COCKPIT_00176_DATA  IB驻点收入调整表 落地数据
  */

  merge into CF_BUSIMG.T_COCKPIT_BUSI_ANAL_RESPONS_M a
  using (
    with tmp as
     (select d.RESPONS_LINE_ID, sum(t.xt_income) as XT_COLLA_INCOME
        from CF_BUSIMG.T_COCKPIT_00175 t
       inner join cf_busimg.t_ctp_branch_oa_rela c
          on t.branch_id = c.ctp_branch_id
       inner join CF_BUSIMG.T_OA_BRANCH d
          on c.oa_branch_id = d.departmentid
       where t.month_id = i_month_id
         and D.RESPONS_LINE_ID is not null
       GROUP BY d.RESPONS_LINE_ID),
    tmp1 as
     (select d.RESPONS_LINE_ID, sum(t.ZD_INCOME) as XT_COLLA_INCOME
        from CF_BUSIMG.T_COCKPIT_00176 t
       inner join cf_busimg.t_ctp_branch_oa_rela c
          on t.branch_id = c.ctp_branch_id
       inner join CF_BUSIMG.T_OA_BRANCH d
          on c.oa_branch_id = d.departmentid
       where t.month_id = i_month_id
         and D.RESPONS_LINE_ID is not null
       GROUP BY d.RESPONS_LINE_ID)
    select t.RESPONS_LINE_ID,
           (nvl(b.XT_COLLA_INCOME, 0) + nvl(c.XT_COLLA_INCOME, 0)) / 1.06 /
           10000 as XT_COLLA_INCOME
      from CF_BUSIMG.T_COCKPIT_BUSI_ANAL_RESPONS_M t
      left join tmp b
        on t.RESPONS_LINE_ID = b.RESPONS_LINE_ID
      left join tmp1 c
        on t.RESPONS_LINE_ID = c.RESPONS_LINE_ID
     where t.busi_month = i_month_id

     ) y on (a.RESPONS_LINE_ID = y.RESPONS_LINE_ID and a.busi_month = i_month_id) when matched then
      update set a.XT_COLLA_INCOME = y.XT_COLLA_INCOME;

  commit;

  --协同业务-场外期权-名义本金
  --协同业务-场外期权-权利金
  --协同业务-场外期权-协同收入
  /*
  场外协同清算台账——名义本金
  场外协同清算台账——权利金绝对值总额
  场外协同清算台账——销售收入+协同定价收入
  当年到当前月份数据
  CF_BUSIMG.P_COCKPIT_00126
  */
  merge into CF_BUSIMG.T_COCKPIT_BUSI_ANAL_RESPONS_M a
  using (select d.RESPONS_LINE_ID,
                sum(nvl(t.NOTIONAL_PRINCIPAL, 0)) / 10000 as XT_OFF_SITE_PRINCI,
                sum(nvl(t.TOTAL_ABSOLUTE_ROYALTY, 0)) / 10000 as XT_OFF_SITE_OPTINO,
                sum(nvl(t.SALES_REVENUE, 0) + nvl(t.COLLABORATIVE_PRICE, 0)) /
                10000 as XT_OFF_SITE_INCOME
           from CF_BUSIMG.T_COCKPIT_00126 t
          inner join CF_BUSIMG.T_OA_BRANCH d
             on t.oa_branch_id = d.departmentid
          where substr(t.done_date, 1, 6) between v_begin_month and
                i_month_id
            and D.RESPONS_LINE_ID is not null
          group by d.RESPONS_LINE_ID) y
  on (a.RESPONS_LINE_ID = y.RESPONS_LINE_ID and a.busi_month = i_month_id)
  when matched then
    update
       set a.XT_OFF_SITE_PRINCI = y.XT_OFF_SITE_PRINCI,
           a.XT_OFF_SITE_OPTINO = y.XT_OFF_SITE_OPTINO,
           a.XT_OFF_SITE_INCOME = y.XT_OFF_SITE_INCOME;
  commit;

  --协同业务-自有资金参与项目-项目数量
  --协同业务-自有资金参与项目-期末权益
  --协同业务-自有资金参与项目-自有资金扣减额
  --协同业务-自有资金参与项目-协同收入
  /*
  自有资金投资项目综合收益情况跟踪表，可以筛选出：项目数量
  自有资金投资项目综合收益情况跟踪表——项目期末权益
  自有资金权益分配表——部门扣减日均权益
  自有资金投资项目综合收益情况跟踪表——本月协同收入
  选择当前月份，显示当年1月到当月数据
  CF_BUSIMG.P_COCKPIT_00172    自有资金投资项目综合收益情况跟踪表
  CF_BUSIMG.P_COCKPIT_00168_EDIT_2_Q  自有资金权益分配表

  CF_BUSIMG.P_COCKPIT_00177_DATA 落地数据 自有资金投资项目综合收益情况跟踪表
  */
  merge into CF_BUSIMG.T_COCKPIT_BUSI_ANAL_RESPONS_M a
  using (
    with tmp as
     (select d.RESPONS_LINE_ID,
             count(distinct t.PROJECT_REFERRED) as XT_OWN_FUNDS_NUMS,
             sum(nvl(t.ENDING_INTEREST, 0)) as XT_OWN_FUNDS_END_RIGHTS,
             sum(nvl(t.SYNERGISTIC_REVENUE, 0)) as XT_OWN_FUNDS_INCOME
        from CF_BUSIMG.T_COCKPIT_00177 t
       inner join cf_busimg.t_oa_branch d
          on t.recommend_department = d.departmentid
       where t.busi_month between v_begin_month and i_month_id
         and D.RESPONS_LINE_ID is not null
       group by d.RESPONS_LINE_ID),
    tmp1 as
     (select d.RESPONS_LINE_ID,
             sum(t.DEPART_REAL_ALLOCATION_RIGHT) as XT_OWN_FUNDS_DISCOUNTS
        from CF_BUSIMG.T_COCKPIT_00168_EDIT_2 t
       inner join cf_busimg.t_oa_branch d
          on t.ALLOCATION_DEPART = d.departmentid
       where t.busi_month between v_begin_month and i_month_id
         and D.RESPONS_LINE_ID is not null
       group by d.RESPONS_LINE_ID)
    select t.RESPONS_LINE_ID,
           nvl(b.XT_OWN_FUNDS_NUMS, 0) as XT_OWN_FUNDS_NUMS,
           nvl(b.XT_OWN_FUNDS_END_RIGHTS, 0) / 10000 as XT_OWN_FUNDS_END_RIGHTS,
           nvl(c.XT_OWN_FUNDS_DISCOUNTS, 0) / 10000 as XT_OWN_FUNDS_DISCOUNTS,
           nvl(b.XT_OWN_FUNDS_INCOME, 0) / 10000 as XT_OWN_FUNDS_INCOME
      from CF_BUSIMG.T_COCKPIT_BUSI_ANAL_RESPONS_M t
      left join tmp b
        on t.RESPONS_LINE_ID = b.RESPONS_LINE_ID
      left join tmp1 c
        on t.RESPONS_LINE_ID = c.RESPONS_LINE_ID
     where t.busi_month = i_month_id) y on (a.RESPONS_LINE_ID = y.RESPONS_LINE_ID and a.busi_month = i_month_id) when matched then
      update
         set a.XT_OWN_FUNDS_NUMS       = y.XT_OWN_FUNDS_NUMS,
             a.XT_OWN_FUNDS_END_RIGHTS = y.XT_OWN_FUNDS_END_RIGHTS,
             a.XT_OWN_FUNDS_DISCOUNTS  = y.XT_OWN_FUNDS_DISCOUNTS,
             a.XT_OWN_FUNDS_INCOME     = y.XT_OWN_FUNDS_INCOME;
  commit;
  -------------------------------------------------------------------------------------------------------------

  ----------------------------财务指标begin-------------------------------------------------------------------
  --财务指标-考核收入
  --财务指标-考核利润-考核利润
  merge into CF_BUSIMG.T_COCKPIT_BUSI_ANAL_RESPONS_M a
  using (
    with tmp as
     (
      --当月数据
      select D.RESPONS_LINE_ID,
              sum(t.f5) as ASSESS_INCOME, --财务指标-考核收入
              sum(t.f21) as ASSESS_PROFIT --财务指标-考核利润-考核利润
        from cf_busimg.t_cockpit_00172 t
       inner join CF_BUSIMG.T_YY_BRANCH_OA_RELA b
          on t.book_id = b.yy_book_id
       inner join CF_BUSIMG.T_OA_BRANCH d
          on b.oa_branch_id = d.departmentid
       where t.month_id = i_month_id
         and D.RESPONS_LINE_ID is not null
       group by D.RESPONS_LINE_ID),
    tmp_last as
     (
      --同比月份数据
      select D.RESPONS_LINE_ID,
              sum(t.f5) as ASSESS_INCOME, --财务指标-考核收入
              sum(t.f21) as ASSESS_PROFIT --财务指标-考核利润-考核利润
        from cf_busimg.t_cockpit_00172 t
       inner join CF_BUSIMG.T_YY_BRANCH_OA_RELA b
          on t.book_id = b.yy_book_id
       inner join CF_BUSIMG.T_OA_BRANCH d
          on b.oa_branch_id = d.departmentid
       where t.month_id = v_yoy_month_id
         and D.RESPONS_LINE_ID is not null
       group by D.RESPONS_LINE_ID)
    select t.RESPONS_LINE_ID,
           t.ASSESS_INCOME / 10000 as ASSESS_INCOME,
           (case
             when t1.ASSESS_INCOME <> 0 then
              t.ASSESS_INCOME / t1.ASSESS_INCOME - 1
             else
              0
           end) * 100 as ASSESS_INCOME_YOY,
           t.ASSESS_PROFIT / 10000 as ASSESS_PROFIT,
           (case
             when t1.ASSESS_PROFIT <> 0 then
              t.ASSESS_PROFIT / t1.ASSESS_PROFIT - 1
             else
              0
           end) * 100 as ASSESS_PROFIT_YOY
      from tmp t
      left join tmp_last t1
        on t.RESPONS_LINE_ID = t1.RESPONS_LINE_ID) y on (a.RESPONS_LINE_ID = y.RESPONS_LINE_ID and a.busi_month = i_month_id) when matched then
      update
         set a.ASSESS_INCOME     = y.ASSESS_INCOME,
             a.ASSESS_INCOME_YOY = y.ASSESS_INCOME_YOY,
             a.ASSESS_PROFIT     = y.ASSESS_PROFIT,
             a.ASSESS_PROFIT_YOY = y.ASSESS_PROFIT_YOY;

  commit;

  --财务指标-考核收入-交易所手续费减免净收入
  --财务指标-考核收入-留存手续费收入
  --财务指标-考核收入-利息净收入
  merge into CF_BUSIMG.T_COCKPIT_BUSI_ANAL_RESPONS_M a
  using (
    with tmp as
     (select d.RESPONS_LINE_ID,
             sum(t.remain_transfee) as remain_transfee,
             sum(t.interest_income) as interest_income,
             sum(t.market_reduct) as market_reduct,
             sum(t.feature_income_total) as feature_income_total
        from cf_busimg.t_cockpit_00174 t
       inner join cf_busimg.t_ctp_branch_oa_rela c
          on t.branch_id = c.ctp_branch_id
       inner join CF_BUSIMG.T_OA_BRANCH d
          on c.oa_branch_id = d.departmentid
       where t.busi_month = i_month_id
         and D.RESPONS_LINE_ID is not null
       group by d.RESPONS_LINE_ID

      )
    select t.RESPONS_LINE_ID,
           t.market_reduct / 10000 as EXCH_NET_INCOME,
           (case
             when t.feature_income_total <> 0 then
              t.market_reduct / t.feature_income_total
             else
              0
           end) * 100 as EXCH_NET_INCOME_PROP,
           t.remain_transfee / 10000 as REMAIN_TRANSFEE_INCOME,
           (case
             when t.feature_income_total <> 0 then
              t.remain_transfee / t.feature_income_total
             else
              0
           end) * 100 as REMAIN_TRANSFEE_INCOME_PROP,
           t.interest_income / 10000 as ASSESS_INTEREST_INCOME,
           (case
             when t.feature_income_total <> 0 then
              t.interest_income / t.feature_income_total
             else
              0
           end) * 100 as ASSESS_INTEREST_INCOME_PROP
      from tmp t) y
        on (a.RESPONS_LINE_ID = y.RESPONS_LINE_ID and
           a.busi_month = i_month_id) when matched then
      update
         set a.EXCH_NET_INCOME             = y.EXCH_NET_INCOME,
             a.EXCH_NET_INCOME_PROP        = y.EXCH_NET_INCOME_PROP,
             a.REMAIN_TRANSFEE_INCOME      = y.REMAIN_TRANSFEE_INCOME,
             a.REMAIN_TRANSFEE_INCOME_PROP = y.REMAIN_TRANSFEE_INCOME_PROP,
             a.ASSESS_INTEREST_INCOME      = y.ASSESS_INTEREST_INCOME,
             a.ASSESS_INTEREST_INCOME_PROP = y.ASSESS_INTEREST_INCOME_PROP;

  commit;

  ----------------------------财务指标end-------------------------------------------------------------------

  ----------------------------收入结构begin-----------------------------------------------------------------
  --收入结构-分析指标-经纪业务收入
  merge into CF_BUSIMG.T_COCKPIT_BUSI_ANAL_RESPONS_M a
  using (
    with tmp as
     (select t.fund_account_id,
             d.RESPONS_LINE_ID,
             (CASE
               WHEN B.OPEN_DATE BETWEEN v_begin_date AND v_end_date THEN
                '1'
               ELSE
                '0'
             END) AS IS_NEW_FLAG,
             sum(t.remain_transfee) as remain_transfee,
             sum(t.INTEREST_INCOME) as INTEREST_INCOME,
             sum(t.MARKET_REDUCT) as MARKET_REDUCT,
             sum(t.FEATURE_INCOME_TOTAL) as FEATURE_INCOME_TOTAL
        from cf_busimg.t_cockpit_00174 t
        left join cf_sett.t_fund_account b
          on t.fund_account_id = b.fund_account_id
       inner join cf_busimg.t_ctp_branch_oa_rela c
          on b.branch_id = c.ctp_branch_id
       inner join CF_BUSIMG.T_OA_BRANCH d
          on c.oa_branch_id = d.departmentid
       where t.busi_month between v_begin_month and i_month_id
         and D.RESPONS_LINE_ID is not null
       group by t.fund_account_id,
                d.RESPONS_LINE_ID,
                (CASE
                  WHEN B.OPEN_DATE BETWEEN v_begin_date AND v_end_date THEN
                   '1'
                  ELSE
                   '0'
                END)),
    tmp_result as
     (select t.RESPONS_LINE_ID,
             sum(t.remain_transfee) as CR_REMAIN_TRANSFEE_INCOME, --收入结构-分析指标-经纪业务收入-留存手续费收入
             sum(t.MARKET_REDUCT) as MARKET_TRANSFEE_INCOME, --收入结构-分析指标-经纪业务收入-交易所手续费减免净收入
             sum(t.INTEREST_INCOME) as INTEREST_INCOME, --收入结构-分析指标-经纪业务收入-利息净收入
             sum(t.FEATURE_INCOME_TOTAL) as FUTURE_INCOME, --收入结构-分析指标-经纪业务收入
             sum(case
                   when t.IS_NEW_FLAG = '0' then
                    t.remain_transfee
                   else
                    0
                 end) as remain_transfee_stock,
             sum(case
                   when t.IS_NEW_FLAG = '1' then
                    t.remain_transfee
                   else
                    0
                 end) as remain_transfee_new,
             sum(case
                   when t.IS_NEW_FLAG = '0' then
                    t.MARKET_REDUCT
                   else
                    0
                 end) as MARKET_REDUCT_stock,
             sum(case
                   when t.IS_NEW_FLAG = '1' then
                    t.MARKET_REDUCT
                   else
                    0
                 end) as MARKET_REDUCT_new,
             sum(case
                   when t.IS_NEW_FLAG = '0' then
                    t.INTEREST_INCOME
                   else
                    0
                 end) as INTEREST_INCOME_stock,
             sum(case
                   when t.IS_NEW_FLAG = '1' then
                    t.INTEREST_INCOME
                   else
                    0
                 end) as INTEREST_INCOME_new
        from tmp t
       group by t.RESPONS_LINE_ID)
    select t.RESPONS_LINE_ID,
           t.FUTURE_INCOME / 10000 as FUTURE_INCOME,
           t.CR_REMAIN_TRANSFEE_INCOME / 10000 as CR_REMAIN_TRANSFEE_INCOME,
           (case
             when t.FUTURE_INCOME > 0 then
              t.CR_REMAIN_TRANSFEE_INCOME / FUTURE_INCOME
             else
              0
           end) * 100 as CR_REMAIN_TRANSFEE_INCOME_PROP,
           (case
             when t.CR_REMAIN_TRANSFEE_INCOME > 0 then
              t.remain_transfee_stock / CR_REMAIN_TRANSFEE_INCOME
             else
              0
           end) * 100 as REMAIN_TRANSFEE_STOCK_PROP,
           (case
             when t.CR_REMAIN_TRANSFEE_INCOME > 0 then
              t.remain_transfee_new / CR_REMAIN_TRANSFEE_INCOME
             else
              0
           end) * 100 as REMAIN_TRANSFEE_NEW_PROP,
           t.MARKET_TRANSFEE_INCOME / 10000 as MARKET_TRANSFEE_INCOME,
           (case
             when t.FUTURE_INCOME > 0 then
              t.MARKET_TRANSFEE_INCOME / FUTURE_INCOME
             else
              0
           end) * 100 as MARKET_TRANSFEE_PROP,
           (case
             when t.MARKET_TRANSFEE_INCOME > 0 then
              t.MARKET_REDUCT_stock / MARKET_TRANSFEE_INCOME
             else
              0
           end) * 100 as MARKET_TRANSFEE_STOCK_PROP,
           (case
             when t.MARKET_TRANSFEE_INCOME > 0 then
              t.MARKET_REDUCT_new / MARKET_TRANSFEE_INCOME
             else
              0
           end) * 100 as MARKET_TRANSFEE_NEW_PROP,
           t.INTEREST_INCOME / 10000 as INTEREST_INCOME,
           (case
             when t.FUTURE_INCOME > 0 then
              t.INTEREST_INCOME / FUTURE_INCOME
             else
              0
           end) * 100 as INTEREST_INCOME_PROP,
           (case
             when t.INTEREST_INCOME > 0 then
              t.INTEREST_INCOME_stock / INTEREST_INCOME
             else
              0
           end) * 100 as INTEREST_INCOME_STOCK_PROP,
           (case
             when t.INTEREST_INCOME > 0 then
              t.INTEREST_INCOME_new / INTEREST_INCOME
             else
              0
           end) * 100 as INTEREST_INCOME_NEW_PROP
      from tmp_result t) y
        on (a.RESPONS_LINE_ID = y.RESPONS_LINE_ID and
           a.busi_month = i_month_id) when matched then
      update
         set a.FUTURE_INCOME                  = y.FUTURE_INCOME,
             a.CR_REMAIN_TRANSFEE_INCOME      = y.CR_REMAIN_TRANSFEE_INCOME,
             a.CR_REMAIN_TRANSFEE_INCOME_PROP = y.CR_REMAIN_TRANSFEE_INCOME_PROP,
             a.REMAIN_TRANSFEE_STOCK_PROP     = y.REMAIN_TRANSFEE_STOCK_PROP,
             a.REMAIN_TRANSFEE_NEW_PROP       = y.REMAIN_TRANSFEE_NEW_PROP,
             a.MARKET_TRANSFEE_INCOME         = y.MARKET_TRANSFEE_INCOME,
             a.MARKET_TRANSFEE_PROP           = y.MARKET_TRANSFEE_PROP,
             a.MARKET_TRANSFEE_STOCK_PROP     = y.MARKET_TRANSFEE_STOCK_PROP,
             a.MARKET_TRANSFEE_NEW_PROP       = y.MARKET_TRANSFEE_NEW_PROP,
             a.INTEREST_INCOME                = y.INTEREST_INCOME,
             a.INTEREST_INCOME_PROP           = y.INTEREST_INCOME_PROP,
             a.INTEREST_INCOME_STOCK_PROP     = y.INTEREST_INCOME_STOCK_PROP,
             a.INTEREST_INCOME_NEW_PROP       = y.INTEREST_INCOME_NEW_PROP;

  commit;
  ----------------------------收入结构end--------------------------------------------------------

  --业务指标-日均权益 20240626
  --业务指标-日均权益同比 20240626
  --业务指标-考核日均权益 20240626  逻辑与日均权益一致
  --业务指标-考核日均权益同比  20240626 逻辑与日均权益一致
  merge into CF_BUSIMG.T_COCKPIT_BUSI_ANAL_RESPONS_M a
  using (
    with tmp as
     (
      --当月日均权益
      select d.RESPONS_LINE_ID,
              sum(case
                    when v_now_trade_days > 0 then
                     t.rights / v_now_trade_days
                    else
                     0
                  end) as avg_rights
        from cf_sett.t_client_sett t
        left join cf_sett.t_fund_account b
          on t.fund_account_id = b.fund_account_id
       inner join cf_busimg.t_ctp_branch_oa_rela c
          on b.branch_id = c.ctp_branch_id
       inner join CF_BUSIMG.T_OA_BRANCH d
          on c.oa_branch_id = d.departmentid
       where t.busi_date between v_now_begin_date and v_now_end_date
         and D.RESPONS_LINE_ID is not null
       group by d.RESPONS_LINE_ID),
    tmp1 as
     (
      --同比日均权益
      select d.RESPONS_LINE_ID,
              sum(case
                    when v_yoy_trade_days > 0 then
                     t.rights / v_yoy_trade_days
                    else
                     0
                  end) as avg_rights
        from cf_sett.t_client_sett t
        left join cf_sett.t_fund_account b
          on t.fund_account_id = b.fund_account_id
       inner join cf_busimg.t_ctp_branch_oa_rela c
          on b.branch_id = c.ctp_branch_id
       inner join CF_BUSIMG.T_OA_BRANCH d
          on c.oa_branch_id = d.departmentid
       where t.busi_date between v_yoy_begin_date and v_yoy_end_date
         and D.RESPONS_LINE_ID is not null
       group by d.RESPONS_LINE_ID),
    tmp2 as
     (
      --当月  自有资金项目权益分配情况
      select d.RESPONS_LINE_ID,
              sum(t.depart_real_allocation_right) as depart_real_allocation_right
        from CF_BUSIMG.T_COCKPIT_00168_EDIT_2 t
       inner join CF_BUSIMG.T_OA_BRANCH d
          on t.ALLOCATION_DEPART = d.departmentid
       where t.BUSI_MONTH = i_month_id
         and D.RESPONS_LINE_ID is not null
       group by d.RESPONS_LINE_ID),
    tmp3 as
     (
      --同比月份  自有资金项目权益分配情况
      select d.RESPONS_LINE_ID,
              sum(t.depart_real_allocation_right) as depart_real_allocation_right
        from CF_BUSIMG.T_COCKPIT_00168_EDIT_2 t
       inner join CF_BUSIMG.T_OA_BRANCH d
          on t.ALLOCATION_DEPART = d.departmentid
       where t.BUSI_MONTH = v_yoy_month_id
         and D.RESPONS_LINE_ID is not null
       group by d.RESPONS_LINE_ID)
    select t.RESPONS_LINE_ID,
           t.avg_rights / 10000 as avg_rights,
           (case
             when t1.avg_rights > 0 then
              t.avg_rights / t1.avg_rights - 1
             else
              0
           end) * 100 as AVG_RIGHTS_YOY,
           (t.avg_rights - nvl(t2.depart_real_allocation_right, 0)) / 10000 as ASSESS_AVG_RIGHTS,
           (case
             when (t1.avg_rights - nvl(t3.depart_real_allocation_right, 0)) > 0 then
              (t.avg_rights - nvl(t2.depart_real_allocation_right, 0)) /
              (t1.avg_rights - nvl(t3.depart_real_allocation_right, 0)) - 1
             else
              0
           end) * 100 as ASSESS_AVG_RIGHTS_YOY
      from tmp t
      left join tmp1 t1
        on t.RESPONS_LINE_ID = t1.RESPONS_LINE_ID
      left join tmp2 t2
        on t.RESPONS_LINE_ID = t2.RESPONS_LINE_ID
      left join tmp3 t3
        on t.RESPONS_LINE_ID = t3.RESPONS_LINE_ID) y on (a.RESPONS_LINE_ID = y.RESPONS_LINE_ID and a.busi_month = i_month_id) when matched then
      update
         set a.AVG_RIGHTS            = y.AVG_RIGHTS,
             a.AVG_RIGHTS_YOY        = y.AVG_RIGHTS_YOY,
             a.ASSESS_AVG_RIGHTS     = y.AVG_RIGHTS,
             a.ASSESS_AVG_RIGHTS_YOY = y.AVG_RIGHTS_YOY;

  commit;

  --业务指标-成交量 20240626
  --业务指标-成交量同比 20240626
  --业务指标-成交额 20240626
  --业务指标-成交额同比 20240626
  merge into CF_BUSIMG.T_COCKPIT_BUSI_ANAL_RESPONS_M a
  using (
    with tmp as
     (
      --当月数据
      select d.RESPONS_LINE_ID,
              sum(t.done_amt) as DOEN_AMOUNT,
              sum(t.done_sum) as done_money
        from cf_sett.t_hold_balance t
        left join cf_sett.t_fund_account b
          on t.fund_account_id = b.fund_account_id
       inner join cf_busimg.t_ctp_branch_oa_rela c
          on b.branch_id = c.ctp_branch_id
       inner join CF_BUSIMG.T_OA_BRANCH d
          on c.oa_branch_id = d.departmentid
       where t.busi_date between v_now_begin_date and v_now_end_date
         and D.RESPONS_LINE_ID is not null
       group by d.RESPONS_LINE_ID),
    tmp1 as
     (
      --同比数据
      select d.RESPONS_LINE_ID,
              sum(t.done_amt) as DOEN_AMOUNT,
              sum(t.done_sum) as done_money
        from cf_sett.t_hold_balance t
        left join cf_sett.t_fund_account b
          on t.fund_account_id = b.fund_account_id
       inner join cf_busimg.t_ctp_branch_oa_rela c
          on b.branch_id = c.ctp_branch_id
       inner join CF_BUSIMG.T_OA_BRANCH d
          on c.oa_branch_id = d.departmentid
       where t.busi_date between v_yoy_begin_date and v_yoy_end_date
         and D.RESPONS_LINE_ID is not null
       group by d.RESPONS_LINE_ID)
    select t.RESPONS_LINE_ID,
           t.DOEN_AMOUNT / 10000 as DOEN_AMOUNT,
           (case
             when t1.DOEN_AMOUNT > 0 then
              t.DOEN_AMOUNT / t1.DOEN_AMOUNT - 1
             else
              0
           end) * 100 as DOEN_AMOUNT_YOY,
           t.DONE_MONEY / 100000000 as DONE_MONEY,
           (case
             when t1.DONE_MONEY > 0 then
              t.DONE_MONEY / t1.DONE_MONEY - 1
             else
              0
           end) * 100 as DONE_MONEY_YOY
      from tmp t
      left join tmp1 t1
        on t.RESPONS_LINE_ID = t1.RESPONS_LINE_ID

     ) y on (a.RESPONS_LINE_ID = y.RESPONS_LINE_ID and a.busi_month = i_month_id) when matched then
      update
         set a.DOEN_AMOUNT     = y.DOEN_AMOUNT,
             a.DOEN_AMOUNT_YOY = y.DOEN_AMOUNT_YOY,
             a.DONE_MONEY      = y.DONE_MONEY,
             a.DONE_MONEY_YOY  = y.DONE_MONEY_YOY

      ;

  commit;

  -- 业务指标-总客户数,  20240626
  -- 业务指标-总客户数同比,  20240626
  -- 业务指标-新增开户数, 20240626
  -- 业务指标-新增开户数同比 20240626
  merge into CF_BUSIMG.T_COCKPIT_BUSI_ANAL_RESPONS_M a
  using (
    with tmp as
     (select t.client_id, d.RESPONS_LINE_ID, t.open_date
        from cf_sett.t_client t
       inner join cf_busimg.t_ctp_branch_oa_rela c
          on t.branch_id = c.ctp_branch_id
       inner join CF_BUSIMG.T_OA_BRANCH d
          on c.oa_branch_id = d.departmentid
       where t.isactive <> '3'
         and D.RESPONS_LINE_ID is not null),
    tmp1 as
     (
      --当月数据
      select t.RESPONS_LINE_ID,
              sum(case
                    when t.open_date <= v_end_date then
                     1
                    else
                     0
                  end) as TOTAL_CLIENT_NUM,
              sum(case
                    when t.open_date between v_now_begin_date and v_now_end_date then
                     1
                    else
                     0
                  end) as NEW_CLIENT_NUM
        from tmp t
       group by t.RESPONS_LINE_ID),
    tmp2 as
     (
      --同比数据
      select t.RESPONS_LINE_ID,
              sum(case
                    when t.open_date <= v_yoy_all_end_date then
                     1
                    else
                     0
                  end) as TOTAL_CLIENT_NUM,
              sum(case
                    when t.open_date between v_yoy_begin_date and v_yoy_end_date then
                     1
                    else
                     0
                  end) as NEW_CLIENT_NUM
        from tmp t
       group by t.RESPONS_LINE_ID)
    select t.RESPONS_LINE_ID,
           t.TOTAL_CLIENT_NUM,
           (case
             when t1.TOTAL_CLIENT_NUM > 0 then
              t.TOTAL_CLIENT_NUM / t1.TOTAL_CLIENT_NUM - 1
             else
              0
           end) * 100 as TOTAL_CLIENT_NUM_YOY,
           t.NEW_CLIENT_NUM,
           (case
             when t1.NEW_CLIENT_NUM > 0 then
              t.NEW_CLIENT_NUM / t1.NEW_CLIENT_NUM - 1
             else
              0
           end) * 100 as NEW_CLIENT_NUM_YOY
      from tmp1 t
      left join tmp2 t1
        on t.RESPONS_LINE_ID = t1.RESPONS_LINE_ID

     ) y on (a.RESPONS_LINE_ID = y.RESPONS_LINE_ID and a.busi_month = i_month_id) when matched then
      update
         set a.TOTAL_CLIENT_NUM     = y.TOTAL_CLIENT_NUM,
             a.TOTAL_CLIENT_NUM_YOY = y.TOTAL_CLIENT_NUM_YOY,
             a.NEW_CLIENT_NUM       = y.NEW_CLIENT_NUM,
             a.NEW_CLIENT_NUM_YOY   = y.NEW_CLIENT_NUM

      ;

  commit;

  -- 业务指标-总有效客户数,  20240626
  -- 业务指标-总有效客户数同比,  20240626
  merge into CF_BUSIMG.T_COCKPIT_BUSI_ANAL_RESPONS_M a
  using (
    with tmp as
    --当月有效户
     (select t.client_id, d.RESPONS_LINE_ID
        from cf_sett.t_hold_balance t
        left join cf_busimg.t_ctp_branch_oa_rela c
          on t.branch_id = c.ctp_branch_id
       inner join CF_BUSIMG.T_OA_BRANCH d
          on c.oa_branch_id = d.departmentid
       where t.busi_date between v_begin_date and v_end_date
         and D.RESPONS_LINE_ID is not null
       group by t.client_id, d.RESPONS_LINE_ID),
    tmp1 as
     (
      --同比日期有效户
      select t.client_id, d.RESPONS_LINE_ID
        from cf_sett.t_hold_balance t
        left join cf_busimg.t_ctp_branch_oa_rela c
          on t.branch_id = c.ctp_branch_id
       inner join CF_BUSIMG.T_OA_BRANCH d
          on c.oa_branch_id = d.departmentid
       where t.busi_date between v_yoy_all_begin_date and v_yoy_all_end_date
         and D.RESPONS_LINE_ID is not null
       group by t.client_id, d.RESPONS_LINE_ID),
    tmp2 as
     (select t.RESPONS_LINE_ID, count(1) as TOTAL_ACTIVE_NUM
        from tmp t
       group by t.RESPONS_LINE_ID),
    tmp3 as
     (select t.RESPONS_LINE_ID, count(1) as TOTAL_ACTIVE_NUM
        from tmp1 t
       group by t.RESPONS_LINE_ID)
    select t.RESPONS_LINE_ID,
           t.TOTAL_ACTIVE_NUM,
           (case
             when t1.TOTAL_ACTIVE_NUM > 0 then
              t.TOTAL_ACTIVE_NUM / t1.TOTAL_ACTIVE_NUM - 1
             else
              0
           end) * 100 as TOTAL_ACTIVE_NUM_YOY
      from tmp2 t
      left join tmp3 t1
        on t.RESPONS_LINE_ID = t1.RESPONS_LINE_ID) y on (a.RESPONS_LINE_ID = y.RESPONS_LINE_ID and a.busi_month = i_month_id) when matched then
      update
         set a.TOTAL_ACTIVE_NUM     = y.TOTAL_ACTIVE_NUM,
             a.TOTAL_ACTIVE_NUM_YOY = y.TOTAL_ACTIVE_NUM_YOY;
  commit;

  --业务结构-日均权益-存量客户 202406
  --业务结构-日均权益-新增客户 202406
  merge into CF_BUSIMG.T_COCKPIT_BUSI_ANAL_RESPONS_M a
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
              D.RESPONS_LINE_ID,
              sum(t.rights) as sum_rights
        from cf_sett.t_client_sett t
        left join cf_sett.t_fund_account b
          on t.fund_account_id = b.fund_account_id
       inner join cf_busimg.t_ctp_branch_oa_rela c
          on b.branch_id = c.ctp_branch_id
       inner join CF_BUSIMG.T_OA_BRANCH d
          on c.oa_branch_id = d.departmentid
       where t.busi_date between v_begin_date and v_end_date
         and D.RESPONS_LINE_ID is not null
       group by t.fund_account_id,
                 (case
                   when b.open_date between v_new_begin_date and v_new_end_date then
                    '1'
                   else
                    '0'
                 end),
                 D.RESPONS_LINE_ID),
    tmp_result as
     (select t.RESPONS_LINE_ID,
             sum(case
                   when t.is_new_flag = '1' and v_busi_trade_days <> 0 then
                    t.sum_rights / v_busi_trade_days
                   else
                    0
                 end) as AVG_RIGHTS_NEW,
             sum(case
                   when t.is_new_flag = '0' and v_busi_trade_days <> 0 then
                    t.sum_rights / v_busi_trade_days
                   else
                    0
                 end) as AVG_RIGHTS_STOCK,
             sum(case
                   when v_busi_trade_days <> 0 then
                    t.sum_rights / v_busi_trade_days
                   else
                    0
                 end) as sum_avg_rights
        from tmp_new t
       group by t.RESPONS_LINE_ID)


    select t.RESPONS_LINE_ID,
           t.AVG_RIGHTS_STOCK / 10000 as AVG_RIGHTS_STOCK,
           (case
             when t.sum_avg_rights <> 0 then
              t.AVG_RIGHTS_STOCK / t.sum_avg_rights
             else
              0
           end) * 100 as AVG_RIGHTS_STOCK_PROP,
           t.AVG_RIGHTS_NEW / 10000 as AVG_RIGHTS_NEW,
           (case
             when t.sum_avg_rights <> 0 then
              t.AVG_RIGHTS_NEW / t.sum_avg_rights
             else
              0
           end) * 100 as AVG_RIGHTS_NEW_PROP
      from tmp_result t) y
        on (a.RESPONS_LINE_ID = y.RESPONS_LINE_ID and
           a.busi_month = i_month_id) when matched then
      update
         set a.AVG_RIGHTS_STOCK      = y.AVG_RIGHTS_STOCK,
             a.AVG_RIGHTS_STOCK_PROP = y.AVG_RIGHTS_STOCK_PROP,
             a.AVG_RIGHTS_NEW        = y.AVG_RIGHTS_NEW,
             a.AVG_RIGHTS_NEW_PROP   = y.AVG_RIGHTS_NEW_PROP

      ;

  commit;

  --业务结构-成交量-存量客户 20240626
  --业务结构-成交量-新增客户 20240626
  --业务结构-成交额-存量客户 20240626
  --业务结构-成交额-新增客户 20240626
  merge into CF_BUSIMG.T_COCKPIT_BUSI_ANAL_RESPONS_M a
  using (
    with tmp_new as
     (
      --新增客户
      SELECT T.FUND_ACCOUNT_ID,
              (CASE
                WHEN B.OPEN_DATE BETWEEN V_NEW_BEGIN_DATE AND V_NEW_END_DATE THEN
                 '1'
                ELSE
                 '0'
              END) AS IS_NEW_FLAG,
              D.RESPONS_LINE_ID,
              SUM(T.DONE_AMT) AS DONE_AMOUNT,
              SUM(T.DONE_SUM) AS DONE_MONEY
        FROM CF_SETT.T_HOLD_BALANCE T
        LEFT JOIN CF_SETT.T_FUND_ACCOUNT B
          ON T.FUND_ACCOUNT_ID = B.FUND_ACCOUNT_ID
       INNER JOIN CF_BUSIMG.T_CTP_BRANCH_OA_RELA C
          ON B.BRANCH_ID = C.CTP_BRANCH_ID
       inner join CF_BUSIMG.T_OA_BRANCH d
          on c.oa_branch_id = d.departmentid
       WHERE T.BUSI_DATE BETWEEN V_BEGIN_DATE AND V_END_DATE
         AND D.RESPONS_LINE_ID IS NOT NULL
       GROUP BY T.FUND_ACCOUNT_ID,
                 (CASE
                   WHEN B.OPEN_DATE BETWEEN V_NEW_BEGIN_DATE AND V_NEW_END_DATE THEN
                    '1'
                   ELSE
                    '0'
                 END),
                 D.RESPONS_LINE_ID),
    TMP_RESULT AS
     (SELECT T.RESPONS_LINE_ID,
             sum(CASE
                   WHEN T.IS_NEW_FLAG = '1' AND V_BUSI_TRADE_DAYS <> 0 THEN
                    T.DONE_AMOUNT
                   ELSE
                    0
                 END) AS DONE_AMOUNT_NEW,
             sum(CASE
                   WHEN T.IS_NEW_FLAG = '0' AND V_BUSI_TRADE_DAYS <> 0 THEN
                    T.DONE_AMOUNT
                   ELSE
                    0
                 END) AS DONE_AMOUNT_STOCK,
             sum(CASE
                   WHEN T.IS_NEW_FLAG = '1' AND V_BUSI_TRADE_DAYS <> 0 THEN
                    T.DONE_MONEY
                   ELSE
                    0
                 END) AS DONE_MONEY_NEW,
             sum(CASE
                   WHEN T.IS_NEW_FLAG = '0' AND V_BUSI_TRADE_DAYS <> 0 THEN
                    T.DONE_MONEY
                   ELSE
                    0
                 END) AS DONE_MONEY_STOCK,

             sum(T.DONE_AMOUNT) AS SUM_DONE_AMOUNT,
             sum(T.DONE_MONEY) AS SUM_DONE_MONEY
        FROM TMP_NEW T
       group by t.RESPONS_LINE_ID

      )
    SELECT T.RESPONS_LINE_ID,
           T.DONE_AMOUNT_STOCK / 10000 as DONE_AMOUNT_STOCK,
           (CASE
             WHEN T.SUM_DONE_AMOUNT <> 0 THEN
              T.DONE_AMOUNT_STOCK / T.SUM_DONE_AMOUNT
             ELSE
              0
           END) * 100 AS DONE_AMOUNT_STOCK_PROP,
           T.DONE_AMOUNT_NEW / 10000 as DONE_AMOUNT_NEW,
           (CASE
             WHEN T.SUM_DONE_AMOUNT <> 0 THEN
              T.DONE_AMOUNT_NEW / T.SUM_DONE_AMOUNT
             ELSE
              0
           END) * 100 AS DONE_AMOUNT_NEW_PROP,
           T.DONE_MONEY_STOCK / 10000 as DONE_MONEY_STOCK,
           (CASE
             WHEN T.SUM_DONE_MONEY <> 0 THEN
              T.DONE_MONEY_STOCK / T.SUM_DONE_MONEY
             ELSE
              0
           END) * 100 AS DONE_MONEY_STOCK_PROP,
           T.DONE_MONEY_NEW / 10000 as DONE_MONEY_NEW,
           (CASE
             WHEN T.SUM_DONE_MONEY <> 0 THEN
              T.DONE_MONEY_NEW / T.SUM_DONE_MONEY
             ELSE
              0
           END) * 100 AS DONE_MONEY_NEW_PROP
      FROM TMP_RESULT T

    ) Y
        ON (A.RESPONS_LINE_ID = Y.RESPONS_LINE_ID AND
           A.BUSI_MONTH = I_MONTH_ID) WHEN MATCHED THEN
      UPDATE
         SET A.DONE_AMOUNT_STOCK      = Y.DONE_AMOUNT_STOCK,
             A.DONE_AMOUNT_STOCK_PROP = Y.DONE_AMOUNT_STOCK_PROP,
             A.DONE_AMOUNT_NEW        = Y.DONE_AMOUNT_NEW,
             A.DONE_AMOUNT_NEW_PROP   = Y.DONE_AMOUNT_NEW_PROP,
             A.DONE_MONEY_STOCK       = Y.DONE_MONEY_STOCK,
             A.DONE_MONEY_STOCK_PROP  = Y.DONE_MONEY_STOCK_PROP,
             A.DONE_MONEY_NEW         = Y.DONE_MONEY_NEW,
             A.DONE_MONEY_NEW_PROP    = Y.DONE_MONEY_NEW_PROP

      ;
  COMMIT;

  --市场成交量，成交额
  select nvl(sum(t.trad_num), 0) * 2,
         nvl(sum(t.trad_amt), 0) * 2 * 100000000
    into v_total_done_amount, v_total_done_money
    from CF_BUSIMG.T_COCKPIT_INDUSTRY_TRAD t --取中期协月度交易数据
   where t.etl_month = i_month_id;

  --市场客户权益-日均
  with tmp as
   (select nvl(sum(t.index_value), 0) * 100000000 as rights
      from CF_BUSIMG.T_COCKPIT_INDUSTRY_MANAGE t
     where t.etl_month = i_month_id
       and t.index_name = '客户权益')
  select (case
           when v_now_trade_days <> 0 then
            t.rights / v_now_trade_days
           else
            0
         end)
    into v_total_rights
    from tmp t;

  --市场手续费收入
  select nvl(sum(t.index_value), 0) * 100000000
    into v_total_index_value
    from CF_BUSIMG.T_COCKPIT_INDUSTRY_MANAGE t
   where t.etl_month = i_month_id
     and t.index_name = '手续费收入';

  --市场地位-日均权益市占率  20240626
  --市场地位-经纪业务手续费收入市占率 20240626
  merge into CF_BUSIMG.T_COCKPIT_BUSI_ANAL_RESPONS_M a
  using (
    with tmp as
     (SELECT T.FUND_ACCOUNT_ID,
             D.RESPONS_LINE_ID,
             SUM(T.RIGHTS) AS RIGHTS,
             SUM(NVL(T.TRANSFEE, 0) + NVL(T.DELIVERY_TRANSFEE, 0) +
                 NVL(T.STRIKEFEE, 0)) AS TRANSFEE
        FROM CF_SETT.T_CLIENT_SETT T
        LEFT JOIN CF_SETT.T_FUND_ACCOUNT B
          ON T.FUND_ACCOUNT_ID = B.FUND_ACCOUNT_ID
       INNER JOIN CF_BUSIMG.T_CTP_BRANCH_OA_RELA C
          ON B.BRANCH_ID = C.CTP_BRANCH_ID
       inner join CF_BUSIMG.T_OA_BRANCH d
          on c.oa_branch_id = d.departmentid
       WHERE T.BUSI_DATE BETWEEN v_now_begin_date AND v_now_end_date
         AND D.RESPONS_LINE_ID IS NOT NULL
       GROUP BY T.FUND_ACCOUNT_ID, D.RESPONS_LINE_ID),
    TMP1 AS
     (SELECT T.RESPONS_LINE_ID,
             SUM(CASE
                   WHEN v_now_trade_days <> 0 THEN
                    T.RIGHTS / v_now_trade_days
                   ELSE
                    0
                 END) AS AVG_RIGHTS,
             SUM(T.TRANSFEE) AS TRANSFEE
        FROM TMP T
       GROUP BY T.RESPONS_LINE_ID)
    SELECT T.RESPONS_LINE_ID,
           (CASE
             WHEN v_total_rights <> 0 THEN
              T.AVG_RIGHTS / v_total_rights
             ELSE
              0
           END) * 10000 AS AVG_RIGHTS_MARKET_RATE,
           (CASE
             WHEN v_total_index_value <> 0 THEN
              T.TRANSFEE / v_total_index_value
             ELSE
              0
           END) * 10000 AS FUTU_TRANS_INCOME_MARKET_RATE
      FROM TMP1 T

    ) Y
        ON (A.RESPONS_LINE_ID = Y.RESPONS_LINE_ID AND
           A.BUSI_MONTH = I_MONTH_ID) WHEN MATCHED THEN
      UPDATE
         SET A.AVG_RIGHTS_MARKET_RATE        = Y.AVG_RIGHTS_MARKET_RATE,
             A.FUTU_TRANS_INCOME_MARKET_RATE = Y.FUTU_TRANS_INCOME_MARKET_RATE;
  COMMIT;

  --市场地位-成交额市场份额占比（市占率） 20240626
  --市场地位-成交量市场份额占比（市占率） 20240626
  merge into CF_BUSIMG.T_COCKPIT_BUSI_ANAL_RESPONS_M a
  using (
    with tmp as
     (SELECT T.FUND_ACCOUNT_ID,
             D.RESPONS_LINE_ID,
             SUM(T.Done_Amt) AS Done_amount,
             SUM(t.done_sum) AS done_money
        FROM CF_SETT.t_Hold_Balance T
        LEFT JOIN CF_SETT.T_FUND_ACCOUNT B
          ON T.FUND_ACCOUNT_ID = B.FUND_ACCOUNT_ID
       INNER JOIN CF_BUSIMG.T_CTP_BRANCH_OA_RELA C
          ON B.BRANCH_ID = C.CTP_BRANCH_ID
       inner join CF_BUSIMG.T_OA_BRANCH d
          on c.oa_branch_id = d.departmentid
       WHERE T.BUSI_DATE BETWEEN v_now_begin_date AND v_now_end_date
         AND D.RESPONS_LINE_ID IS NOT NULL
       GROUP BY T.FUND_ACCOUNT_ID, D.RESPONS_LINE_ID),
    TMP1 AS
     (SELECT T.RESPONS_LINE_ID,
             SUM(t.Done_amount) AS Done_amount,
             SUM(T.done_money) AS done_money
        FROM TMP T
       GROUP BY T.RESPONS_LINE_ID)
    SELECT T.RESPONS_LINE_ID,
           (CASE
             WHEN v_total_done_amount <> 0 THEN
              T.Done_amount / v_total_done_amount
             ELSE
              0
           END) * 10000 AS DONE_AMOUNT_MARKET_RATE,
           (CASE
             WHEN v_total_done_money <> 0 THEN
              T.done_money / v_total_done_money
             ELSE
              0
           END) * 10000 AS DONE_MONEY_MAREKT_RATE
      FROM TMP1 T

    ) Y
        ON (A.RESPONS_LINE_ID = Y.RESPONS_LINE_ID AND
           A.BUSI_MONTH = I_MONTH_ID) WHEN MATCHED THEN
      UPDATE
         SET A.DONE_MONEY_MAREKT_RATE  = Y.DONE_MONEY_MAREKT_RATE,
             A.DONE_AMOUNT_MARKET_RATE = Y.DONE_AMOUNT_MARKET_RATE

      ;
  COMMIT;

  ---------------------------------收入结构其他收入数据更新begin----------------------------------
  --收入结构-主指标-营业收入
  --营业收入=经纪业务收入+其他收入
  --其他收入=交易咨询收入+产品销售收入+场外期权收入+自有资金参与项目收入
  /*
   产品销售收入 不统计产品费用分配表
   一、产品销售收入指标为：以下三项之和
  1、资管部“产品费用分配表“字段 综合收益
  2、金融产品中心”收入分配表-FOF产品“字段 经纪业务总收入     CF_BUSIMG.P_COCKPIT_00119_DATA
  3、金融产品中心”收入分配表-普通产品“字段 经纪业务总收入    CF_BUSIMG.P_COCKPIT_00121_DATA

  场外期权收入：场外期权-场外协同清算台账-合计收入/营业收入
   */
  merge into CF_BUSIMG.T_COCKPIT_BUSI_ANAL_RESPONS_M a
  using (
    with tmp as
     (
      --场外期权收入
      select d.RESPONS_LINE_ID,

              sum(nvl(t.TOTAL_INCOME, 0)) / 10000 as OFF_SITE_INCOME
        from CF_BUSIMG.T_COCKPIT_00126 t
         inner join CF_BUSIMG.T_OA_BRANCH d
          on t.oa_branch_id = d.departmentid
       where substr(t.done_date, 1, 6) between v_begin_month and i_month_id
          and D.RESPONS_LINE_ID is not null
       group by d.RESPONS_LINE_ID),
    TMP_PRODUCT_fof AS
     (
      --产品销售收入 FOF产品
      select d.RESPONS_LINE_ID,
              sum(t.TOTAL_FUTU_INCOME * a.alloca_reate) as PRODUCT_SELL_INCOME
        from CF_BUSIMG.T_COCKPIT_00118 t
       inner join CF_BUSIMG.T_COCKPIT_00117 a
          on t.busi_month = a.busi_month
         and t.filing_code = a.filing_code
         inner join CF_BUSIMG.T_OA_BRANCH d
          on a.ALLOCA_OA_BRANCH_ID = d.departmentid
       where t.busi_month between v_begin_month and i_month_id
        and D.RESPONS_LINE_ID is not null
       group by d.RESPONS_LINE_ID),
    TMP_PRODUCT_pt AS
     (
      --产品销售收入 普通产品
      select d.RESPONS_LINE_ID,
              sum(t.TOTAL_FUTU_INCOME * a.alloca_reate) as PRODUCT_SELL_INCOME
        from CF_BUSIMG.T_COCKPIT_00120 t
       inner join CF_BUSIMG.T_COCKPIT_00117 a
          on t.busi_month = a.busi_month
         and t.filing_code = a.filing_code
        inner join CF_BUSIMG.T_OA_BRANCH d
          on a.ALLOCA_OA_BRANCH_ID = d.departmentid
       where t.busi_month between v_begin_month and i_month_id
       group by d.RESPONS_LINE_ID),
    TMP1 AS
     (SELECT T.RESPONS_LINE_ID,
             (T.FUTURE_INCOME + T.XT_TRADE_ZX_INCOME + T.XT_OWN_FUNDS_INCOME +
             nvl(c.PRODUCT_SELL_INCOME, 0) + nvl(d.PRODUCT_SELL_INCOME, 0) +
             NVL(B.OFF_SITE_INCOME, 0)) AS OPERAT_INCOME,
             T.FUTURE_INCOME, --收入结构-分析指标-经纪业务收入
             T.XT_TRADE_ZX_INCOME, --协同业务-交易咨询-交易咨询收入
             (nvl(c.PRODUCT_SELL_INCOME, 0) + nvl(d.PRODUCT_SELL_INCOME, 0)) AS PRODUCT_SELL_INCOME, --产品销售收入
             NVL(B.OFF_SITE_INCOME, 0) AS OFF_SITE_INCOME, --场外期权收入
             T.XT_OWN_FUNDS_INCOME --协同业务-自有资金参与项目-协同收入
        FROM CF_BUSIMG.T_COCKPIT_BUSI_ANAL_RESPONS_M T
        LEFT JOIN TMP B
          ON T.RESPONS_LINE_ID = B.RESPONS_LINE_ID
        left join TMP_PRODUCT_fof c
          on t.RESPONS_LINE_ID = c.RESPONS_LINE_ID
        left join TMP_PRODUCT_pt d
          on t.RESPONS_LINE_ID = d.RESPONS_LINE_ID
       WHERE T.BUSI_MONTH = I_MONTH_ID)
    SELECT T.RESPONS_LINE_ID,
           t.OPERAT_INCOME, --收入结构-主指标-营业收入
           (CASE
             WHEN t.OPERAT_INCOME <> 0 then
              t.FUTURE_INCOME / t.OPERAT_INCOME
             else
              0
           end) * 100 AS FUTURE_INCOME_PROP, --收入结构-分析指标-经纪业务收入-经纪业务收入占比
           (CASE
             WHEN t.OPERAT_INCOME <> 0 then
              t.XT_TRADE_ZX_INCOME / t.OPERAT_INCOME
             else
              0
           end) * 100 as TRADE_ZX_INCOME_PROP, --收入结构-分析指标-其他收入-交易咨询收入占比
           (CASE
             WHEN t.OPERAT_INCOME <> 0 then
              t.PRODUCT_SELL_INCOME / t.OPERAT_INCOME
             else
              0
           end)* 100 as PRODUCT_XC_INCOME_PORP, --收入结构-分析指标-其他收入-产品销售收入占比
           (CASE
             WHEN t.OPERAT_INCOME <> 0 then
              t.OFF_SITE_INCOME / t.OPERAT_INCOME
             else
              0
           end) * 100 as OVER_OPTION_INCOME_PROP, --收入结构-分析指标-其他收入-场外期权收入占比
           (CASE
             WHEN t.OPERAT_INCOME <> 0 then
              t.XT_OWN_FUNDS_INCOME / t.OPERAT_INCOME
             else
              0
           end) * 100 as OWN_FUNDS_INCOME_PROP --收入结构-分析指标-其他收入-自有资金参与项目收入占比
      FROM TMP1 T

    ) Y
        ON (A.RESPONS_LINE_ID = Y.RESPONS_LINE_ID AND A.BUSI_MONTH = I_MONTH_ID) WHEN
     MATCHED THEN
      UPDATE
         SET A.OPERAT_INCOME           = Y.OPERAT_INCOME,
             a.FUTURE_INCOME_PROP      = y.FUTURE_INCOME_PROP,
             a.TRADE_ZX_INCOME_PROP    = y.TRADE_ZX_INCOME_PROP,
             a.PRODUCT_XC_INCOME_PORP  = y.PRODUCT_XC_INCOME_PORP,
             a.OVER_OPTION_INCOME_PROP = y.OVER_OPTION_INCOME_PROP,
             a.OWN_FUNDS_INCOME_PROP   = y.OWN_FUNDS_INCOME_PROP

      ;
  COMMIT;
  ---------------------------------收入结构其他收入数据更新end-----------------------------------

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
