# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, when, sum

from config import Config
from utils.StructTypes.ddw_t_business_line import schema as ddw_t_business_line_schema
from utils.StructTypes.ddw_t_busi_anal_target_type import schema as ddw_t_busi_anal_target_type_schema
from utils.StructTypes.ddw_t_cockpit_00138 import schema as ddw_t_cockpit_00138_schema
from utils.StructTypes.ddw_t_oa_branch import schema as ddw_t_oa_branch_schema
from utils.io_utils.common_uitls import to_color_str
from utils.task_env import return_to_hive, update_dataframe, log

logger = Config().get_logger()


@log
def p_coc_bu_anal_targ_line_y_data(spark, i_month_id):
    """
    经营分析-业务条线-经营目标完成情况-按年落地
    """

    """
    v_busi_year := substr(I_MONTH_ID, 1, 4);

  delete from CF_BUSIMG.T_COCKPIT_BU_ANAL_TARG_LINE_Y t
   where t.busi_year = v_busi_year;
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
  INSERT INTO CF_BUSIMG.T_COCKPIT_BU_ANAL_TARG_LINE_Y
    (BUSI_YEAR,
     Business_Line_Id,
     BUSI_TYPE,
     BUSI_TYPE_NAME
   )
    select v_busi_year as BUSI_YEAR, t.business_line_id,a.busi_type,a.busi_type_name
      from CF_BUSIMG.T_business_line t 
      inner join CF_BUSIMG.T_BUSI_ANAL_TARGET_TYPE a
      on 1=1
     where t.if_use='1';
  commit;


 --更新各个指标的数据
  merge into CF_BUSIMG.T_COCKPIT_BU_ANAL_TARG_LINE_Y x
  using (
    with tmp as
     (select a.BUSINESS_LINE_ID,
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
        on t.oa_branch_id=a.departmentid
       where t.year_id = v_busi_year
         and a.BUSINESS_LINE_ID is not null
       group by a.BUSINESS_LINE_ID,
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
    select t.BUSINESS_LINE_ID,
           t.busi_type,
           t.complete_value,
           (case
             when t.year_target_value <> 0 then
              t.complete_value / t.year_target_value
             else
              0
           end) as complete_value_rate
      from tmp t) y
        on (x.busi_year = v_busi_year and
           x.business_line_id = y.business_line_id and x.busi_type = y.busi_type) when
     matched then
      update
         set x.complete_value      = y.complete_value,
             x.complete_value_rate = y.complete_value_rate;
    """

    v_busi_year = i_month_id[:4]

    logger.info(to_color_str('初始化数据', 'blue'))

    df_business_line = spark.table("ddw.t_business_line").where("if_use='1'")
    df_business_line = spark.createDataFrame(
        data=df_business_line.rdd,
        schema=ddw_t_business_line_schema
    )

    df_target_type = spark.table("ddw.t_busi_anal_target_type")
    df_target_type = spark.createDataFrame(
        data=df_target_type.rdd,
        schema=ddw_t_busi_anal_target_type_schema
    )

    df_result_y = df_business_line.crossJoin(df_target_type).select(
        lit(v_busi_year).alias("busi_year"),
        df_business_line["business_line_id"],
        df_target_type["busi_type"],
        df_target_type["busi_type_name"]
    )

    return_to_hive(
        spark=spark,
        df_result=df_result_y,
        target_table='ddw.t_cockpit_bu_anal_targ_line_y',
        insert_mode='overwrite'
    )

    df_result_y = spark.table('ddw.t_cockpit_bu_anal_targ_line_y').where(col('busi_year') == lit(v_busi_year))

    logger.info(to_color_str('更新各个指标的数据', 'blue'))

    df_138_yq = spark.table('ddw.t_cockpit_00138').where(col('year_id') == lit(v_busi_year))
    df_138_yq = spark.createDataFrame(
        data=df_138_yq.rdd,
        schema=ddw_t_cockpit_00138_schema
    )

    df_oa_branch = spark.table('ddw.t_oa_branch').where(col('business_line_id').isNotNull())
    df_oa_branch = spark.createDataFrame(
        data=df_oa_branch.rdd,
        schema=ddw_t_oa_branch_schema
    )

    tmp = df_138_yq.join(
        other=df_oa_branch,
        on=(df_138_yq['oa_branch_id'] == df_oa_branch['departmentid']),
        how='inner'
    ).groupBy(
        df_oa_branch['business_line_id'],
        when(
            (df_138_yq['index_type'] == '0') & (df_138_yq['index_asses_benchmark'] == '1'),
            '001'
        ).when(
            (df_138_yq['index_type'] == '1') & (df_138_yq['index_asses_benchmark'] == '1'),
            '002'
        ).when(
            (df_138_yq['index_type'] == '0') & (df_138_yq['index_asses_benchmark'] == '2'),
            '003'
        ).when(
            (df_138_yq['index_type'] == '1') & (df_138_yq['index_asses_benchmark'] == '2'),
            '004'
        ).when(
            (df_138_yq['index_type'] == '0') & (df_138_yq['index_asses_benchmark'] == '3'),
            '005'
        ).when(
            (df_138_yq['index_type'] == '0') & (df_138_yq['index_asses_benchmark'] == '4'),
            '006'
        ).when(
            (df_138_yq['index_type'] == '1') & (df_138_yq['index_asses_benchmark'] == '4'),
            '007'
        ).when(
            (df_138_yq['index_type'] == '0') & (df_138_yq['index_asses_benchmark'] == '5'),
            '008'
        ).when(
            (df_138_yq['index_type'] == '1') & (df_138_yq['index_asses_benchmark'] == '5'),
            '009'
        ).when(
            (df_138_yq['index_type'] == '0') & (df_138_yq['index_asses_benchmark'] == '6') &
            (df_138_yq['index_name'].like('%新增直接开发有效客户数量%')),
            '010'
        ).when(
            (df_138_yq['index_type'] == '0') & (df_138_yq['index_asses_benchmark'] == '6') &
            (df_138_yq['index_name'].like('%新增有效客户数量%')),
            '011'
        ).when(
            (df_138_yq['index_type'] == '0') & (df_138_yq['index_asses_benchmark'] == '7'),
            '012'
        ).otherwise('').alias('busi_type'),
    ).agg(
        sum(df_138_yq['year_target_value']).alias('year_target_value'),
        sum(df_138_yq['complet_value']).alias('complete_value')
    ).select(
        df_oa_branch['business_line_id'],
        'busi_type',
        'year_target_value',
        'complete_value',
    )

    df_y = tmp.select(
        tmp['business_line_id'],
        tmp['busi_type'],
        tmp['complete_value'],
        when(
            tmp['year_target_value'] != 0,
            tmp['complete_value'] / tmp['year_target_value']
        ).otherwise(0).alias('complete_value_rate')
    )

    df_result_y = update_dataframe(
        df_to_update=df_result_y,
        df_use_me=df_y,
        join_columns=['business_line_id', 'busi_type'],
        update_columns=['complete_value', 'complete_value_rate']
    )

    return_to_hive(
        spark=spark,
        df_result=df_result_y,
        target_table='ddw.t_cockpit_bu_anal_targ_line_y',
        insert_mode='overwrite'
    )
