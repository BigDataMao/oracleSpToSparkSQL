create procedure P_COCKPIT_00092_DATA(i_busi_date in varchar2,
                                      o_return_code out integer,
                                      o_return_msg out varchar2) is
    --========================================================================================================================
    --系统名称:生成千万工程落地数据
    --模块名称:
    --模块编号:
    --模块描述：  宏源-千万工程”指标落地数据
    --开发人员:zhongying.zhang
    --目前版本:
    --创建时间:20230411
    --版    权:
    --修改历史:

    --========================================================================================================================
    v_op_object  VARCHAR2(50) DEFAULT 'P_COCKPIT_00092_DATA'; -- '操作对象';
    v_error_msg  VARCHAR2(200); --返回信息
    v_error_code INTEGER;
    v_userException EXCEPTION;

    V_BUSI_YEAR        VARCHAR2(4); --业务年份
    v_begin_date       varchar2(8); --开始时间
    v_end_date         varchar2(8); --结束时间
    v_trade_days       number;
    v_begin_month      varchar2(6); --开始月份
    v_end_month        varchar2(6); --结束月份
    v_last_begin_month varchar2(6); --去年开始月份
    v_last_end_month   varchar2(6); --去年结束月份
    v_last_begin_date  varchar2(8); --去年开始日期
    v_last_end_date    varchar2(8); --去年结束日期
begin

    V_BUSI_YEAR := SUBSTR(i_busi_date, 1, 4);
    v_begin_date := V_BUSI_YEAR || '0101';
    v_end_date := i_busi_date;

    v_begin_month := V_BUSI_YEAR || '01';
    v_end_month := substr(i_busi_date, 1, 6);

    v_last_begin_month := substr(to_char(add_months(to_date(I_BUSI_DATE,
                                                            'yyyymmdd'),
                                                    -12),
                                         'yyyymmdd'),
                                 1,
                                 6);
    v_last_end_month := substr(v_last_begin_month, 1, 4) || '12';

    v_last_begin_date := v_last_begin_month || '01';
    v_last_end_date := v_last_end_month || '31';

    --计算交易日天数
    select count(1)
    into v_trade_days
    from cf_sett.t_pub_date t
    where t.busi_date between v_begin_date and v_end_date
      and t.trade_flag = '1'
      and t.market_no = '1';

    --本年度千万工程客户
    --千万工程客户
    execute immediate 'truncate table CF_BUSIMG.T_BRP_00092_QW';
    INSERT INTO CF_BUSIMG.T_BRP_00092_QW
        (FUND_ACCOUNT_ID, OA_BRANCH_ID, CLIENT_TYPE, OPEN_DATE)
    select b.fund_account_id, c.oa_branch_id, b.client_type, B.OPEN_DATE
    from CF_CRMMG.T_HIS_LABEL_CLIENT t
             inner join cf_crmmg.t_label a
                        on t.label_id = a.label_id
                            and a.label_id = 'BQ4909' --千万工程
             left join cf_sett.t_fund_account b
                       on t.client_id = b.fund_account_id
             inner join cf_busimg.t_ctp_branch_oa_rela c
                        on b.branch_id = c.ctp_branch_id

    where c.oa_branch_id is not null
      and t.months between v_begin_month and v_end_month
    group by b.fund_account_id, c.oa_branch_id, b.client_type, B.OPEN_DATE;
    COMMIT;

    --去年千万工程客户
    execute immediate 'truncate table CF_BUSIMG.T_BRP_00092_QW_LAST';
    INSERT INTO CF_BUSIMG.T_BRP_00092_QW_LAST
        (FUND_ACCOUNT_ID, OA_BRANCH_ID, CLIENT_TYPE, OPEN_DATE)
    select b.fund_account_id, c.oa_branch_id, b.client_type, B.OPEN_DATE
    from CF_CRMMG.T_HIS_LABEL_CLIENT t
             inner join cf_crmmg.t_label a
                        on t.label_id = a.label_id
                            and a.label_id = 'BQ4909' --千万工程
             left join cf_sett.t_fund_account b
                       on t.client_id = b.fund_account_id
             inner join cf_busimg.t_ctp_branch_oa_rela c
                        on b.branch_id = c.ctp_branch_id

    where c.oa_branch_id is not null
      and t.months between v_last_begin_month and v_last_end_month
    group by b.fund_account_id, c.oa_branch_id, b.client_type, B.OPEN_DATE;
    COMMIT;

    --计算客户本年的净贡献
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
                 2)      as jgx
    from CTP63.T_DS_ADM_INVESTOR_VALUE a
             left JOIN CTP63.T_DS_ADM_BROKERDATA_DETAIL a2
                       on a.date_dt = a2.tx_dt
                           and a.investor_id = a2.investor_id
                           and a2.rec_freq = 'M'
    where replace(a.date_dt, '-', '') between v_begin_date and v_end_date

    group by a.investor_id;
    commit;

    --计算客户上一年的净贡献
    execute immediate 'truncate table CF_BUSIMG.TMP_COCKPIT_CLIENT_JGX_LAST';
    INSERT INTO CF_BUSIMG.TMP_COCKPIT_CLIENT_JGX_LAST
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
                 2)      as jgx
    from CTP63.T_DS_ADM_INVESTOR_VALUE a
             left JOIN CTP63.T_DS_ADM_BROKERDATA_DETAIL a2
                       on a.date_dt = a2.tx_dt
                           and a.investor_id = a2.investor_id
                           and a2.rec_freq = 'M'
    where replace(a.date_dt, '-', '') between v_last_begin_date and
              v_last_end_date

    group by a.investor_id;
    commit;

    delete from CF_BUSIMG.T_BRP_00092 t where t.Busi_Year = V_BUSI_YEAR;
    commit;

    insert into CF_BUSIMG.T_BRP_00092
        (BUSI_YEAR, OA_BRANCH_ID, OA_BRANCH_NAME)
    select V_BUSI_YEAR    as busi_year,
           b.departmentid as OA_BRANCH_ID,
           b.shortname    as OA_BRANCH_NAME
    from cf_busimg.T_OA_BRANCH b

    where b.canceled is null;
    commit;

    --日均权益
    merge into cf_busimg.t_brp_00092 a
    using (WITH tmp1 as
                    (select t.fund_account_id,
                            a.oa_branch_id,
                            a.client_type,
                            sum(t.rights) as rights
                     from cf_sett.t_client_sett t
                              inner join CF_BUSIMG.T_BRP_00092_QW a
                                         on t.fund_account_id = a.fund_account_id
                     where t.busi_date between v_begin_date and v_end_date
                     group by t.fund_account_id, a.oa_branch_id, a.client_type),
                tmp2 as
                    (select t.oa_branch_id,
                            sum(case
                                    when t.client_type in ('3', '4') then
                                        t.rights
                                    else
                                        0
                                end) / v_trade_days      as avg_rights_3,
                            sum(case
                                    when t.client_type = '1' then
                                        t.rights
                                    else
                                        0
                                end) / v_trade_days      as avg_rights_1,
                            sum(case
                                    when t.client_type = '0' then
                                        t.rights
                                    else
                                        0
                                end) / v_trade_days      as avg_rights_0,
                            sum(t.rights) / v_trade_days as avg_rights_all
                     from tmp1 t
                     group by t.oa_branch_id)
           select V_BUSI_YEAR as busi_year,
                  t.oa_branch_id,
                  t.avg_rights_3,
                  (case
                       when t.avg_rights_all <> 0 then
                           t.avg_rights_3 / t.avg_rights_all
                       else
                           0
                      end)    as avg_rights_3_rate,
                  t.avg_rights_1,
                  (case
                       when t.avg_rights_all <> 0 then
                           t.avg_rights_1 / t.avg_rights_all
                       else
                           0
                      end)    as avg_rights_1_rate,
                  t.avg_rights_0,
                  (case
                       when t.avg_rights_all <> 0 then
                           t.avg_rights_0 / t.avg_rights_all
                       else
                           0
                      end)    as avg_rights_0_rate

           from tmp2 t) y
    on (a.BUSI_YEAR = y.busi_year and a.oa_branch_id = y.oa_branch_id)
    when
        matched then
        update
        set a.avg_rights_3      = y.avg_rights_3,
            a.avg_rights_3_rate = y.avg_rights_3_rate,
            a.avg_rights_1      = y.avg_rights_1,
            a.avg_rights_1_rate = y.avg_rights_1_rate,
            a.avg_rights_0      = y.avg_rights_0,
            a.avg_rights_0_rate = y.avg_rights_0_rate;
    commit;

    --手续费
    merge into cf_busimg.t_brp_00092 a
    using (WITH tmp1 as
                    (select t.fund_account_id,
                            a.oa_branch_id,
                            a.client_type,
                            sum(t.transfee + t.delivery_transfee + t.strikefee) as transfee
                     from cf_sett.t_client_sett t
                              inner join CF_BUSIMG.T_BRP_00092_QW a
                                         on t.fund_account_id = a.fund_account_id
                     where t.busi_date between v_begin_date and v_end_date
                     group by t.fund_account_id, a.oa_branch_id, a.client_type),
                tmp2 as
                    (select t.oa_branch_id,
                            sum(case
                                    when t.client_type in ('3', '4') then
                                        t.transfee
                                    else
                                        0
                                end)        as transfee_3,
                            sum(case
                                    when t.client_type = '1' then
                                        t.transfee
                                    else
                                        0
                                end)        as transfee_1,
                            sum(case
                                    when t.client_type = '0' then
                                        t.transfee
                                    else
                                        0
                                end)        as transfee_0,
                            sum(t.transfee) as transfee_all
                     from tmp1 t
                     group by t.oa_branch_id)
           select V_BUSI_YEAR as busi_year,
                  t.oa_branch_id,
                  t.transfee_3,
                  (case
                       when t.transfee_all <> 0 then
                           t.transfee_3 / t.transfee_all
                       else
                           0
                      end)    as TRANSFEE_3_RATE,
                  t.transfee_1,
                  (case
                       when t.transfee_all <> 0 then
                           t.transfee_1 / t.transfee_all
                       else
                           0
                      end)    as TRANSFEE_1_RATE,
                  t.transfee_0,
                  (case
                       when t.transfee_all <> 0 then
                           t.transfee_0 / t.transfee_all
                       else
                           0
                      end)    as TRANSFEE_0_RATE

           from tmp2 t) y
    on (a.BUSI_YEAR = y.busi_year and a.oa_branch_id = y.oa_branch_id)
    when
        matched then
        update
        set a.TRANSFEE_3      = y.TRANSFEE_3,
            a.TRANSFEE_3_rate = y.TRANSFEE_3_rate,
            a.TRANSFEE_1      = y.TRANSFEE_1,
            a.TRANSFEE_1_rate = y.TRANSFEE_1_rate,
            a.TRANSFEE_0      = y.TRANSFEE_0,
            a.TRANSFEE_0_rate = y.TRANSFEE_0_rate;
    commit;

    --客户数
    merge into cf_busimg.t_brp_00092 a
    using (with tmp1 as
                    (select t.fund_account_id, a.oa_branch_id, a.client_type
                     from cf_sett.t_fund_account t
                              inner join CF_BUSIMG.T_BRP_00092_QW a
                                         on t.fund_account_id = a.fund_account_id

                     group by t.fund_account_id, a.oa_branch_id, a.client_type),
                tmp2 as
                    (select t.oa_branch_id,
                            sum(case
                                    when t.client_type in ('3', '4') then
                                        1
                                    else
                                        0
                                end) as client_num_3,
                            sum(case
                                    when t.client_type = '1' then
                                        1
                                    else
                                        0
                                end) as client_num_1,
                            sum(case
                                    when t.client_type = '0' then
                                        1
                                    else
                                        0
                                end) as client_num_0

                     from tmp1 t
                     group by t.oa_branch_id)
           select V_BUSI_YEAR as busi_year,
                  t.oa_branch_id,
                  t.CLIENT_NUM_3,
                  t.CLIENT_NUM_1,
                  t.CLIENT_NUM_0

           from tmp2 t) y
    on (a.BUSI_YEAR = y.busi_year and a.oa_branch_id = y.oa_branch_id)
    when
        matched then
        update
        set a.CLIENT_NUM_3 = y.CLIENT_NUM_3,
            a.CLIENT_NUM_1 = y.CLIENT_NUM_1,
            a.CLIENT_NUM_0 = y.CLIENT_NUM_0;
    commit;

    --净贡献总和（万）
    merge into cf_busimg.t_brp_00092 a
    using (with tmp1 as
                    (select t.client_id as fund_account_id,
                            a.oa_branch_id,
                            a.client_type,
                            t.jgx
                     from CF_BUSIMG.tmp_COCKPIT_CLIENT_jgx t
                              inner join CF_BUSIMG.T_BRP_00092_QW a
                                         on t.client_id = a.fund_account_id),
                tmp2 as
                    (select t.oa_branch_id,
                            sum(case
                                    when t.client_type in ('3', '4') then
                                        t.jgx
                                    else
                                        0
                                end) as CLEAR_TRANSFEE_3,
                            sum(case
                                    when t.client_type = '1' then
                                        t.jgx
                                    else
                                        0
                                end) as CLEAR_TRANSFEE_1,
                            sum(case
                                    when t.client_type = '0' then
                                        t.jgx
                                    else
                                        0
                                end) as CLEAR_TRANSFEE_0
                     from tmp1 t
                     group by t.oa_branch_id)
           select V_BUSI_YEAR                as busi_year,
                  t.oa_branch_id,
                  t.clear_transfee_3 / 10000 as clear_transfee_3,
                  t.clear_transfee_1 / 10000 as clear_transfee_1,
                  t.clear_transfee_0 / 10000 as clear_transfee_0
           from tmp2 t) y
    on (a.BUSI_YEAR = y.busi_year and a.oa_branch_id = y.oa_branch_id)
    when
        matched then
        update
        set a.clear_transfee_3 = y.clear_transfee_3,
            a.clear_transfee_1 = y.clear_transfee_1,
            a.clear_transfee_0 = y.clear_transfee_0;
    commit;

    --单客户平均贡献值
    update cf_busimg.t_brp_00092 a
    set a.avg_clear_transfee_3 = (case
                                      when a.client_num_3 <> 0 then
                                                  a.clear_transfee_3 * 10000 /
                                                  a.client_num_3
                                      else
                                          0
        end),
        a.avg_clear_transfee_1 = (case
                                      when a.client_num_1 <> 0 then
                                                  a.clear_transfee_1 * 10000 /
                                                  a.client_num_1
                                      else
                                          0
            end),
        a.avg_clear_transfee_0 = (case
                                      when a.client_num_0 <> 0 then
                                                  a.clear_transfee_0 * 10000 /
                                                  a.client_num_0
                                      else
                                          0
            end)
    where a.busi_year = V_BUSI_YEAR;

    commit;

    --两年千万工程客户数
    merge into cf_busimg.t_brp_00092 a
    using (with tmp as
                    (
                        --本年度符合标准千万工程
                        select t.oa_branch_id, count(1) as QW_CLIENT_NUM_ALL
                        from CF_BUSIMG.T_BRP_00092_QW t
                        group by t.oa_branch_id),
                tmp1 as
                    (
                        --连续两年符合标准存量客户数
                        select T.OA_BRANCH_ID, COUNT(1) AS QW_CLIENT_NUM_1
                        from CF_BUSIMG.T_BRP_00092_QW t
                                 inner join CF_BUSIMG.T_BRP_00092_QW_LAST A
                                            ON T.FUND_ACCOUNT_ID = A.FUND_ACCOUNT_ID
                        GROUP BY T.OA_BRANCH_ID),
                TMP2 AS
                    (
                        --连续两年符合标准新开户客户数
                        select T.OA_BRANCH_ID, COUNT(1) AS QW_CLIENT_NUM_2
                        from CF_BUSIMG.T_BRP_00092_QW t
                                 inner join CF_BUSIMG.T_BRP_00092_QW_LAST A
                                            ON T.FUND_ACCOUNT_ID = A.FUND_ACCOUNT_ID
                        where t.open_date between v_begin_date and v_end_date
                        GROUP BY T.OA_BRANCH_ID),
                tmp3 as
                    (
                        --新增符合标准存量客户数
                        select t.oa_branch_id, count(1) as QW_CLIENT_NUM_3
                        from CF_BUSIMG.T_BRP_00092_QW t
                        where not exists (select 1
                                          from CF_BUSIMG.T_BRP_00092_QW a
                                                   inner join CF_BUSIMG.T_BRP_00092_QW_LAST b
                                                              on a.fund_account_id = b.fund_account_id
                                          where t.fund_account_id = a.fund_account_id)
                          and t.open_date < v_begin_date
                        group by t.oa_branch_id),
                tmp4 as
                    (
                        --两年千万工程客户数-新增符合标准新开户客户数
                        select t.oa_branch_id, count(1) as QW_CLIENT_NUM_4
                        from CF_BUSIMG.T_BRP_00092_QW t
                        where not exists (select 1
                                          from CF_BUSIMG.T_BRP_00092_QW a
                                                   inner join CF_BUSIMG.T_BRP_00092_QW_LAST b
                                                              on a.fund_account_id = b.fund_account_id
                                          where t.fund_account_id = a.fund_account_id)
                          and t.open_date >= v_begin_date
                        group by t.oa_branch_id)
           select t.oa_branch_id,
                  nvl(a.QW_CLIENT_NUM_1, 0)   as QW_CLIENT_NUM_1,
                  (case
                       when nvl(e.QW_CLIENT_NUM_ALL, 0) <> 0 then
                           nvl(a.QW_CLIENT_NUM_1, 0) / nvl(e.QW_CLIENT_NUM_ALL, 0)
                       else
                           0
                      end)                    as QW_CLIENT_NUM_1_RATE,
                  nvl(b.QW_CLIENT_NUM_2, 0)   as QW_CLIENT_NUM_2,
                  (case
                       when nvl(e.QW_CLIENT_NUM_ALL, 0) <> 0 then
                           nvl(b.QW_CLIENT_NUM_2, 0) / nvl(e.QW_CLIENT_NUM_ALL, 0)
                       else
                           0
                      end)                    as QW_CLIENT_NUM_2_RATE,
                  nvl(c.QW_CLIENT_NUM_3, 0)   as QW_CLIENT_NUM_3,
                  (case
                       when nvl(e.QW_CLIENT_NUM_ALL, 0) <> 0 then
                           nvl(c.QW_CLIENT_NUM_3, 0) / nvl(e.QW_CLIENT_NUM_ALL, 0)
                       else
                           0
                      end)                    as QW_CLIENT_NUM_3_RATE,
                  nvl(d.QW_CLIENT_NUM_4, 0)   as QW_CLIENT_NUM_4,
                  (case
                       when nvl(e.QW_CLIENT_NUM_ALL, 0) <> 0 then
                           nvl(d.QW_CLIENT_NUM_4, 0) / nvl(e.QW_CLIENT_NUM_ALL, 0)
                       else
                           0
                      end)                    as QW_CLIENT_NUM_4_RATE,
                  nvl(e.QW_CLIENT_NUM_ALL, 0) as QW_CLIENT_NUM_ALL
           from CF_BUSIMG.T_BRP_00092 t
                    left join tmp1 a
                              on t.oa_branch_id = a.oa_branch_id
                    left join tmp2 b
                              on t.oa_branch_id = b.oa_branch_id
                    left join tmp3 c
                              on t.oa_branch_id = c.oa_branch_id
                    left join tmp4 d
                              on t.oa_branch_id = d.oa_branch_id
                    left join tmp e
                              on t.oa_branch_id = e.oa_branch_id
           where t.busi_year = V_BUSI_YEAR) y
    on (a.BUSI_YEAR = v_busi_year and a.oa_branch_id = y.oa_branch_id)
    when matched then
        update
        set a.QW_CLIENT_NUM_1      = y.QW_CLIENT_NUM_1,
            a.QW_CLIENT_NUM_1_RATE = y.QW_CLIENT_NUM_1_RATE,
            a.QW_CLIENT_NUM_2      = y.QW_CLIENT_NUM_2,
            a.QW_CLIENT_NUM_2_RATE = y.QW_CLIENT_NUM_2_RATE,
            a.QW_CLIENT_NUM_3      = y.QW_CLIENT_NUM_3,
            a.QW_CLIENT_NUM_3_RATE = y.QW_CLIENT_NUM_3_RATE,
            a.QW_CLIENT_NUM_4      = y.QW_CLIENT_NUM_4,
            a.QW_CLIENT_NUM_4_RATE = y.QW_CLIENT_NUM_4_RATE,
            a.QW_CLIENT_NUM_ALL    = y.QW_CLIENT_NUM_ALL;
    commit;

    --两年千万工程客户净贡献
    merge into cf_busimg.t_brp_00092 a
    using (with tmp as
                    (
                        --本年度符合标准千万工程
                        select t.oa_branch_id, sum(a.jgx) as QW_CLEAR_TRANSFEE_ALL
                        from CF_BUSIMG.T_BRP_00092_QW t
                                 inner join CF_BUSIMG.tmp_COCKPIT_CLIENT_jgx a
                                            on t.fund_account_id = a.client_id

                        group by t.oa_branch_id),
                tmp1 as
                    (
                        --连续两年符合标准存量客户净贡献
                        select T.OA_BRANCH_ID, sum(b.jgx) AS QW_CLEAR_TRANSFEE_1
                        from CF_BUSIMG.T_BRP_00092_QW t
                                 inner join CF_BUSIMG.T_BRP_00092_QW_LAST A
                                            ON T.FUND_ACCOUNT_ID = A.FUND_ACCOUNT_ID
                                 inner join CF_BUSIMG.tmp_COCKPIT_CLIENT_jgx b
                                            on t.fund_account_id = b.client_id
                        GROUP BY T.OA_BRANCH_ID),
                TMP2 AS
                    (
                        --连续两年符合标准新开户客户数
                        select T.OA_BRANCH_ID, sum(b.jgx) AS QW_CLEAR_TRANSFEE_2
                        from CF_BUSIMG.T_BRP_00092_QW t
                                 inner join CF_BUSIMG.T_BRP_00092_QW_LAST A
                                            ON T.FUND_ACCOUNT_ID = A.FUND_ACCOUNT_ID
                                 inner join CF_BUSIMG.tmp_COCKPIT_CLIENT_jgx b
                                            on t.fund_account_id = b.client_id
                        where t.open_date between v_begin_date and v_end_date
                        GROUP BY T.OA_BRANCH_ID),
                tmp3 as
                    (
                        --新增符合标准存量客户数
                        select t.oa_branch_id, sum(b.jgx) as QW_CLEAR_TRANSFEE_3
                        from CF_BUSIMG.T_BRP_00092_QW t
                                 inner join CF_BUSIMG.tmp_COCKPIT_CLIENT_jgx b
                                            on t.fund_account_id = b.client_id
                        where not exists (select 1
                                          from CF_BUSIMG.T_BRP_00092_QW a
                                                   inner join CF_BUSIMG.T_BRP_00092_QW_LAST b
                                                              on a.fund_account_id = b.fund_account_id
                                          where t.fund_account_id = a.fund_account_id)
                          and t.open_date < v_begin_date
                        group by t.oa_branch_id),
                tmp4 as
                    (
                        --两年千万工程客户数-新增符合标准新开户客户数
                        select t.oa_branch_id, sum(b.jgx) as QW_CLEAR_TRANSFEE_4
                        from CF_BUSIMG.T_BRP_00092_QW t
                                 inner join CF_BUSIMG.tmp_COCKPIT_CLIENT_jgx b
                                            on t.fund_account_id = b.client_id
                        where not exists (select 1
                                          from CF_BUSIMG.T_BRP_00092_QW a
                                                   inner join CF_BUSIMG.T_BRP_00092_QW_LAST b
                                                              on a.fund_account_id = b.fund_account_id
                                          where t.fund_account_id = a.fund_account_id)
                          and t.open_date >= v_begin_date
                        group by t.oa_branch_id)
           select t.oa_branch_id,
                  nvl(a.QW_CLEAR_TRANSFEE_1, 0)   as QW_CLEAR_TRANSFEE_1,
                  (case
                       when nvl(e.QW_CLEAR_TRANSFEE_ALL, 0) <> 0 then
                               nvl(a.QW_CLEAR_TRANSFEE_1, 0) /
                               nvl(e.QW_CLEAR_TRANSFEE_ALL, 0)
                       else
                           0
                      end)                        as QW_CLEAR_TRANSFEE_1_RATE,
                  nvl(b.QW_CLEAR_TRANSFEE_2, 0)   as QW_CLEAR_TRANSFEE_2,
                  (case
                       when nvl(e.QW_CLEAR_TRANSFEE_ALL, 0) <> 0 then
                               nvl(b.QW_CLEAR_TRANSFEE_2, 0) /
                               nvl(e.QW_CLEAR_TRANSFEE_ALL, 0)
                       else
                           0
                      end)                        as QW_CLEAR_TRANSFEE_2_RATE,
                  nvl(c.QW_CLEAR_TRANSFEE_3, 0)   as QW_CLEAR_TRANSFEE_3,
                  (case
                       when nvl(e.QW_CLEAR_TRANSFEE_ALL, 0) <> 0 then
                               nvl(c.QW_CLEAR_TRANSFEE_3, 0) /
                               nvl(e.QW_CLEAR_TRANSFEE_ALL, 0)
                       else
                           0
                      end)                        as QW_CLEAR_TRANSFEE_3_RATE,
                  nvl(d.QW_CLEAR_TRANSFEE_4, 0)   as QW_CLEAR_TRANSFEE_4,
                  (case
                       when nvl(e.QW_CLEAR_TRANSFEE_ALL, 0) <> 0 then
                               nvl(d.QW_CLEAR_TRANSFEE_4, 0) /
                               nvl(e.QW_CLEAR_TRANSFEE_ALL, 0)
                       else
                           0
                      end)                        as QW_CLEAR_TRANSFEE_4_RATE,
                  nvl(e.QW_CLEAR_TRANSFEE_ALL, 0) as QW_CLEAR_TRANSFEE_ALL
           from CF_BUSIMG.T_BRP_00092 t
                    left join tmp1 a
                              on t.oa_branch_id = a.oa_branch_id
                    left join tmp2 b
                              on t.oa_branch_id = b.oa_branch_id
                    left join tmp3 c
                              on t.oa_branch_id = c.oa_branch_id
                    left join tmp4 d
                              on t.oa_branch_id = d.oa_branch_id
                    left join tmp e
                              on t.oa_branch_id = e.oa_branch_id
           where t.busi_year = V_BUSI_YEAR) y
    on (a.BUSI_YEAR = v_busi_year and a.oa_branch_id = y.oa_branch_id)
    when matched then
        update
        set a.QW_CLEAR_TRANSFEE_1      = y.QW_CLEAR_TRANSFEE_1,
            a.QW_CLEAR_TRANSFEE_1_RATE = y.QW_CLEAR_TRANSFEE_1_RATE,
            a.QW_CLEAR_TRANSFEE_2      = y.QW_CLEAR_TRANSFEE_2,
            a.QW_CLEAR_TRANSFEE_2_RATE = y.QW_CLEAR_TRANSFEE_2_RATE,
            a.QW_CLEAR_TRANSFEE_3      = y.QW_CLEAR_TRANSFEE_3,
            a.QW_CLEAR_TRANSFEE_3_RATE = y.QW_CLEAR_TRANSFEE_3_RATE,
            a.QW_CLEAR_TRANSFEE_4      = y.QW_CLEAR_TRANSFEE_4,
            a.QW_CLEAR_TRANSFEE_4_RATE = y.QW_CLEAR_TRANSFEE_4_RATE,
            a.QW_CLEAR_TRANSFEE_ALL    = y.QW_CLEAR_TRANSFEE_ALL;
    commit;

    --户均贡献值（万）
    update cf_busimg.t_brp_00092 a
    set a.QW_AVG_CLEAR_TRANSFEE1 = (case
                                        when a.QW_CLIENT_NUM_ALL <> 0 then
                                                a.QW_CLEAR_TRANSFEE_ALL /
                                                a.QW_CLIENT_NUM_ALL
                                        else
                                            0
        end) / 10000,
        a.QW_AVG_CLEAR_TRANSFEE2 = (case
                                        when a.QW_CLIENT_NUM_2 <> 0 then
                                                a.QW_CLEAR_TRANSFEE_2 /
                                                a.QW_CLIENT_NUM_2
                                        else
                                            0
            end) / 10000,
        a.QW_AVG_CLEAR_TRANSFEE3 = (case
                                        when a.QW_CLIENT_NUM_3 <> 0 then
                                                a.QW_CLEAR_TRANSFEE_3 /
                                                a.QW_CLIENT_NUM_3
                                        else
                                            0
            end) / 10000,
        a.QW_AVG_CLEAR_TRANSFEE4 = (case
                                        when a.QW_CLIENT_NUM_4 <> 0 then
                                                a.QW_CLEAR_TRANSFEE_4 /
                                                a.QW_CLIENT_NUM_4
                                        else
                                            0
            end) / 10000
    where a.busi_year = V_BUSI_YEAR;

    commit;

    --流失客户数
    merge into cf_busimg.t_brp_00092 a
    using (with tmp as
                    (
                        --上年度符合标准的客户数
                        select t.oa_branch_id, count(1) as LOSE_CLIENT_NUM1
                        from CF_BUSIMG.T_BRP_00092_QW_LAST t
                        group by t.oa_branch_id),
                tmp1 as
                    (
                        --连续两年符合标准的客户总数
                        select t.oa_branch_id, count(1) as LOSE_CLIENT_NUM2
                        from CF_BUSIMG.T_BRP_00092_QW t
                                 inner join CF_BUSIMG.T_BRP_00092_QW_LAST A
                                            ON T.FUND_ACCOUNT_ID = A.FUND_ACCOUNT_ID
                        group by t.oa_branch_id),
                tmp2 as
                    (
                        --上年度符合，本年度不符合总数
                        select t.oa_branch_id, count(1) as LOSE_CLIENT_NUM4
                        from CF_BUSIMG.T_BRP_00092_QW_LAST t
                        where not exists (select 1
                                          from CF_BUSIMG.T_BRP_00092_QW a
                                                   inner join CF_BUSIMG.T_BRP_00092_QW_LAST b
                                                              on a.FUND_ACCOUNT_ID = b.FUND_ACCOUNT_ID
                                          where t.FUND_ACCOUNT_ID = a.FUND_ACCOUNT_ID)
                        group by t.oa_branch_id)

           select t.oa_branch_id,
                  nvl(a.LOSE_CLIENT_NUM1, 0) as LOSE_CLIENT_NUM1,
                  nvl(b.LOSE_CLIENT_NUM2, 0) as LOSE_CLIENT_NUM2,
                  (case
                       when nvl(a.LOSE_CLIENT_NUM1, 0) <> 0 then
                           nvl(b.LOSE_CLIENT_NUM2, 0) / nvl(a.LOSE_CLIENT_NUM1, 0)
                       else
                           0
                      end)                   as LOSE_CLIENT_NUM3,
                  nvl(c.LOSE_CLIENT_NUM4, 0) as LOSE_CLIENT_NUM4,
                  (case
                       when nvl(a.LOSE_CLIENT_NUM1, 0) <> 0 then
                           nvl(c.LOSE_CLIENT_NUM4, 0) / nvl(a.LOSE_CLIENT_NUM1, 0)
                       else
                           0
                      end)                   as LOSE_CLIENT_NUM5

           from CF_BUSIMG.T_BRP_00092 t
                    left join tmp a
                              on t.oa_branch_id = a.oa_branch_id
                    left join tmp1 b
                              on t.oa_branch_id = b.oa_branch_id
                    left join tmp2 c
                              on t.oa_branch_id = c.oa_branch_id

           where t.busi_year = V_BUSI_YEAR) y
    on (a.BUSI_YEAR = v_busi_year and a.oa_branch_id = y.oa_branch_id)
    when matched then
        update
        set a.LOSE_CLIENT_NUM1 = y.LOSE_CLIENT_NUM1,
            a.LOSE_CLIENT_NUM2 = y.LOSE_CLIENT_NUM2,
            a.LOSE_CLIENT_NUM3 = y.LOSE_CLIENT_NUM3,
            a.LOSE_CLIENT_NUM4 = y.LOSE_CLIENT_NUM4,
            a.LOSE_CLIENT_NUM5 = y.LOSE_CLIENT_NUM5;
    commit;

    --流失客户数-净贡献
    merge into cf_busimg.t_brp_00092 a
    using (with tmp as
                    (
                        --流失客户数-上年度符合客户净贡献
                        select t.oa_branch_id, sum(a.jgx) as LOSE_CLIENT_NUM6
                        from CF_BUSIMG.T_BRP_00092_QW_LAST t
                                 inner join CF_BUSIMG.TMP_COCKPIT_CLIENT_JGX_LAST a
                                            on t.fund_account_id = a.client_id
                        group by t.oa_branch_id),
                tmp1 as
                    (
                        --连续两年符合标准的客户净贡献
                        select t.oa_branch_id, sum(b.jgx) as LOSE_CLIENT_NUM7
                        from CF_BUSIMG.T_BRP_00092_QW t
                                 inner join CF_BUSIMG.T_BRP_00092_QW_LAST A
                                            ON T.FUND_ACCOUNT_ID = A.FUND_ACCOUNT_ID
                                 inner join CF_BUSIMG.TMP_COCKPIT_CLIENT_JGX b
                                            on t.fund_account_id = b.client_id
                        group by t.oa_branch_id),
                tmp2 as
                    (
                        --上年度符合，本年度不符合总数
                        select t.oa_branch_id, sum(a.jgx) as LOSE_CLIENT_NUM9
                        from CF_BUSIMG.T_BRP_00092_QW_LAST t
                                 inner join CF_BUSIMG.TMP_COCKPIT_CLIENT_JGX_LAST a
                                            on t.fund_account_id = a.client_id
                        where not exists (select 1
                                          from CF_BUSIMG.T_BRP_00092_QW a
                                                   inner join CF_BUSIMG.T_BRP_00092_QW_LAST b
                                                              on a.FUND_ACCOUNT_ID = b.FUND_ACCOUNT_ID
                                          where t.FUND_ACCOUNT_ID = a.FUND_ACCOUNT_ID)
                        group by t.oa_branch_id)

           select t.oa_branch_id,
                  nvl(a.LOSE_CLIENT_NUM6, 0) as LOSE_CLIENT_NUM6,
                  nvl(b.LOSE_CLIENT_NUM7, 0) as LOSE_CLIENT_NUM7,
                  (case
                       when nvl(a.LOSE_CLIENT_NUM6, 0) <> 0 then
                           nvl(b.LOSE_CLIENT_NUM7, 0) / nvl(a.LOSE_CLIENT_NUM6, 0)
                       else
                           0
                      end)                   as LOSE_CLIENT_NUM8,
                  nvl(c.LOSE_CLIENT_NUM9, 0) as LOSE_CLIENT_NUM9,
                  (case
                       when nvl(a.LOSE_CLIENT_NUM6, 0) <> 0 then
                           nvl(c.LOSE_CLIENT_NUM9, 0) / nvl(a.LOSE_CLIENT_NUM6, 0)
                       else
                           0
                      end)                   as LOSE_CLIENT_NUM10
           from CF_BUSIMG.T_BRP_00092 t
                    left join tmp a
                              on t.oa_branch_id = a.oa_branch_id
                    left join tmp1 b
                              on t.oa_branch_id = b.oa_branch_id
                    left join tmp2 c
                              on t.oa_branch_id = c.oa_branch_id
           where t.busi_year = V_BUSI_YEAR) y
    on (a.BUSI_YEAR = v_busi_year and a.oa_branch_id = y.oa_branch_id)
    when matched then
        update
        set a.LOSE_CLIENT_NUM6  = y.LOSE_CLIENT_NUM6,
            a.LOSE_CLIENT_NUM7  = y.LOSE_CLIENT_NUM7,
            a.LOSE_CLIENT_NUM8  = y.LOSE_CLIENT_NUM8,
            a.LOSE_CLIENT_NUM9  = y.LOSE_CLIENT_NUM9,
            a.LOSE_CLIENT_NUM10 = y.LOSE_CLIENT_NUM10;
    commit;

    --流失客户数-本年全客户平均净贡献增长率
    /*
      "200个客户在2022年的净贡献，除以，200；假设为A
    180个客户在2021年的净贡献，除以，180；假设为B
    结果为：A/B—1"

      */
    merge into cf_busimg.t_brp_00092 a
    using (with tmp as
                    (
                        --本年客户数
                        select t.oa_branch_id, count(1) as client_num
                        from CF_BUSIMG.T_BRP_00092_QW t
                        group by t.oa_branch_id),
                tmp1 as
                    (
                        --本年净贡献
                        select t.oa_branch_id, sum(a.jgx) as jgx_sum
                        from CF_BUSIMG.T_BRP_00092_QW t
                                 inner join CF_BUSIMG.TMP_COCKPIT_CLIENT_JGX a
                                            on t.fund_account_id = a.client_id
                        group by t.oa_branch_id),
                tmp2 as
                    (
                        --本年全客户平均净贡献
                        select t.oa_branch_id,
                               (case
                                    when t.client_num <> 0 then
                                        nvl(a.jgx_sum, 0) / t.client_num
                                    else
                                        0
                                   end) as jgx_rate
                        from tmp t
                                 left join tmp1 a
                                           on t.oa_branch_id = a.oa_branch_id),
                tmp3 as
                    (
                        --上一年客户数
                        select t.oa_branch_id, count(1) as client_num
                        from CF_BUSIMG.T_BRP_00092_QW_last t
                        group by t.oa_branch_id),
                tmp4 as
                    (
                        --上一年净贡献
                        select t.oa_branch_id, sum(a.jgx) as jgx_sum
                        from CF_BUSIMG.T_BRP_00092_QW_LAST t
                                 inner join CF_BUSIMG.TMP_COCKPIT_CLIENT_JGX_LAST a
                                            on t.fund_account_id = a.client_id
                        group by t.oa_branch_id),
                tmp5 as
                    (
                        --上一年全客户平均净贡献
                        select t.oa_branch_id,
                               (case
                                    when t.client_num <> 0 then
                                        nvl(a.jgx_sum, 0) / t.client_num
                                    else
                                        0
                                   end) as jgx_rate
                        from tmp3 t
                                 left join tmp4 a
                                           on t.oa_branch_id = a.oa_branch_id)
           select t.oa_branch_id,
                  (case
                       when nvl(b.jgx_rate, 0) <> 0 then
                           nvl(a.jgx_rate, 0) / nvl(b.jgx_rate, 0) - 1
                       else
                           0
                      end) as LOSE_CLIENT_NUM11
           from CF_BUSIMG.T_BRP_00092 t
                    left join tmp2 a
                              on t.oa_branch_id = a.oa_branch_id
                    left join tmp5 b
                              on t.oa_branch_id = b.oa_branch_id

           where t.busi_year = V_BUSI_YEAR) y
    on (a.BUSI_YEAR = v_busi_year and a.oa_branch_id = y.oa_branch_id)
    when matched then
        update set a.LOSE_CLIENT_NUM11 = y.LOSE_CLIENT_NUM11;
    commit;

    --流失客户数-本年未流失客户平均净贡献增长率
    /*
      "150个客户在2022年的净贡献，除以，150；假设为C
    150个客户在2021年的净贡献，除以，150；假设为D
    结果为：C/D—1"

      */
    merge into cf_busimg.t_brp_00092 a
    using (with tmp as
                    (
                        --本年和上年同时千万工程
                        select t.fund_account_id, t.oa_branch_id
                        from CF_BUSIMG.T_BRP_00092_QW t
                                 inner join CF_BUSIMG.T_BRP_00092_QW_last a
                                            on t.fund_account_id = a.fund_account_id
                        group by t.fund_account_id, t.oa_branch_id),
                tmp1 as
                    (
                        --客户数
                        select t.oa_branch_id, count(1) as client_num
                        from tmp t
                        group by t.oa_branch_id),
                tmp2 as
                    (
                        --当年净贡献
                        select t.oa_branch_id, sum(a.jgx) as jgx_sum
                        from tmp t
                                 inner join CF_BUSIMG.TMP_COCKPIT_CLIENT_JGX a
                                            on t.fund_account_id = a.client_id
                        group by t.oa_branch_id),
                tmp3 as
                    (
                        --上一年净贡献
                        select t.oa_branch_id, sum(a.jgx) as jgx_sum
                        from tmp t
                                 inner join CF_BUSIMG.TMP_COCKPIT_CLIENT_JGX_LAST a
                                            on t.fund_account_id = a.client_id
                        group by t.oa_branch_id),
                tmp4 as
                    (
                        --本年平均净贡献
                        select t.oa_branch_id,
                               (case
                                    when t.client_num <> 0 then
                                        nvl(a.jgx_sum, 0) / t.client_num
                                    else
                                        0
                                   end) as jgx_rate
                        from tmp1 t
                                 left join tmp2 a
                                           on t.oa_branch_id = a.oa_branch_id),
                tmp5 as
                    (
                        --上一年平均净贡献
                        select t.oa_branch_id,
                               (case
                                    when t.client_num <> 0 then
                                        nvl(a.jgx_sum, 0) / t.client_num
                                    else
                                        0
                                   end) as jgx_rate
                        from tmp1 t
                                 left join tmp3 a
                                           on t.oa_branch_id = a.oa_branch_id)

           select t.oa_branch_id,
                  (case
                       when nvl(b.jgx_rate, 0) <> 0 then
                           nvl(a.jgx_rate, 0) / nvl(b.jgx_rate, 0) - 1
                       else
                           0
                      end) as LOSE_CLIENT_NUM12
           from CF_BUSIMG.T_BRP_00092 t
                    left join tmp4 a
                              on t.oa_branch_id = a.oa_branch_id
                    left join tmp5 b
                              on t.oa_branch_id = b.oa_branch_id
           where t.busi_year = V_BUSI_YEAR) y
    on (a.BUSI_YEAR = v_busi_year and a.oa_branch_id = y.oa_branch_id)
    when matched then
        update set a.LOSE_CLIENT_NUM12 = y.LOSE_CLIENT_NUM12;
    commit;

    --流失客户数-增创净贡献
    /*
    150个客户在2022年的净贡献，减去，150个客户在2021年的净贡献

    */
    merge into cf_busimg.t_brp_00092 a
    using (with tmp as
                    (
                        --本年和上年同时千万工程
                        select t.fund_account_id, t.oa_branch_id
                        from CF_BUSIMG.T_BRP_00092_QW t
                                 inner join CF_BUSIMG.T_BRP_00092_QW_last a
                                            on t.fund_account_id = a.fund_account_id
                        group by t.fund_account_id, t.oa_branch_id),
                tmp2 as
                    (
                        --当年净贡献
                        select t.oa_branch_id, sum(a.jgx) as jgx_sum
                        from tmp t
                                 inner join CF_BUSIMG.TMP_COCKPIT_CLIENT_JGX a
                                            on t.fund_account_id = a.client_id
                        group by t.oa_branch_id),
                tmp3 as
                    (
                        --上一年净贡献
                        select t.oa_branch_id, sum(a.jgx) as jgx_sum
                        from tmp t
                                 inner join CF_BUSIMG.TMP_COCKPIT_CLIENT_JGX_LAST a
                                            on t.fund_account_id = a.client_id
                        group by t.oa_branch_id)
           select t.oa_branch_id,
                  nvl(a.jgx_sum, 0) - nvl(b.jgx_sum, 0) as LOSE_CLIENT_NUM13
           from CF_BUSIMG.T_BRP_00092 t
                    left join tmp2 a
                              on t.oa_branch_id = a.oa_branch_id
                    left join tmp3 b
                              on t.oa_branch_id = b.oa_branch_id
           where t.busi_year = V_BUSI_YEAR) y
    on (a.BUSI_YEAR = v_busi_year and a.oa_branch_id = y.oa_branch_id)
    when matched then
        update set a.LOSE_CLIENT_NUM13 = y.LOSE_CLIENT_NUM13;
    commit;
    O_RETURN_CODE := 0;
    O_RETURN_MSG := '执行成功';
EXCEPTION
    when v_userException then
        ROLLBACK;
        v_error_msg := o_return_msg;
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
        O_RETURN_MSG := O_RETURN_MSG || SQLERRM;
        V_ERROR_CODE := SQLCODE;
        V_ERROR_MSG := SQLERRM;
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

/

