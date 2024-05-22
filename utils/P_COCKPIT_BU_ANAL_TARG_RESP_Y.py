# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, lit, col, sum, when
from pyspark.sql.types import StringType

from config import Config
from utils.hy_utils.type_map import get_busi_type
from utils.io_utils.common_uitls import to_color_str
from utils.StructTypes.ddw_t_respons_line import schema as ddw_t_respons_line_schema
from utils.StructTypes.ddw_t_busi_anal_target_type import schema as ddw_t_busi_anal_target_type_schema
from utils.StructTypes.ddw_t_cockpit_00138 import schema as ddw_t_cockpit_00138_schema
from utils.StructTypes.ddw_t_oa_branch import schema as ddw_t_oa_branch_schema
from utils.task_env import return_to_hive, update_dataframe, log

logger = Config().get_logger()


@log
def p_cockpit_bu_anal_targ_resp_y(spark, i_month_id):
    v_busi_year = i_month_id[:4]
    # 注册udf
    get_busi_type_udf = udf(get_busi_type, StringType())

    logger.info(to_color_str('初始化本年数据', 'blue'))

    """
      v_busi_year := substr(I_MONTH_ID, 1, 4);

  delete from CF_BUSIMG.T_COCKPIT_BU_ANAL_TARG_RESP_Y t
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
  INSERT INTO CF_BUSIMG.T_COCKPIT_BU_ANAL_TARG_RESP_Y
    (BUSI_YEAR,
     RESPONS_LINE_ID,
     BUSI_TYPE,
     BUSI_TYPE_NAME
   )
    select v_busi_year as BUSI_YEAR, t.RESPONS_LINE_ID,a.busi_type,a.busi_type_name
      from CF_BUSIMG.t_Respons_Line t
      inner join CF_BUSIMG.T_BUSI_ANAL_TARGET_TYPE a
      on 1=1
     where t.if_use='1';
  commit;


  --更新各个指标的数据
  merge into CF_BUSIMG.T_COCKPIT_BU_ANAL_TARG_RESP_Y x
  using (
    with tmp as
     (select a.respons_line_id,
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
         and a.respons_line_id is not null
       group by a.respons_line_id,
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
           x.respons_line_id = y.respons_line_id and x.busi_type = y.busi_type) when
     matched then
      update
         set x.complete_value      = y.complete_value,
             x.complete_value_rate = y.complete_value_rate;
    """

    df_respons_line = spark.table("ddw.t_respons_line").where("if_use = '1'")
    df_respons_line = spark.createDataFrame(
        data=df_respons_line.rdd,
        schema=ddw_t_respons_line_schema
    )

    df_target_type = spark.table("ddw.t_busi_anal_target_type")
    df_target_type = spark.createDataFrame(
        data=df_target_type.rdd,
        schema=ddw_t_busi_anal_target_type_schema
    )

    df_result = df_respons_line.crossJoin(
        other=df_target_type
    ).select(
        lit(v_busi_year).alias("busi_year"),
        df_respons_line["respons_line_id"],
        df_target_type["busi_type"],
        df_target_type["busi_type_name"]
    )

    return_to_hive(
        spark=spark,
        df_result=df_result,
        target_table="ddw.t_cockpit_bu_anal_targ_resp_y",
        insert_mode="overwrite"
    )

    df_result = spark.table('ddw.t_cockpit_bu_anal_targ_resp_y').where(col('busi_year') == v_busi_year)

    logger.info(to_color_str('更新各个指标的数据', 'green'))

    def update_data(df_in):
        df_138_y = spark.table('ddw.t_cockpit_00138').where(col('year_id') == lit(v_busi_year))
        df_138_y = spark.createDataFrame(
            data=df_138_y.rdd,
            schema=ddw_t_cockpit_00138_schema
        )

        df_oa_branch = spark.table('ddw.t_oa_branch').where(col('respons_line_id').isNotNull())
        a = spark.createDataFrame(
            data=df_oa_branch.rdd,
            schema=ddw_t_oa_branch_schema
        )

        t = df_138_y.withColumn(
            'busi_type',
            get_busi_type_udf(
                df_138_y['index_type'],
                df_138_y['index_asses_benchmark'],
                df_138_y['index_name']
            )
        )

        tmp = t.join(
            other=a,
            on=(t['oa_branch_id'] == a['departmentid'])
        ).groupBy(
            a['respons_line_id'],
            t['busi_type']
        ).agg(
            sum(t['year_target_value']).alias('year_target_value'),
            sum(t['complet_value']).alias('complete_value')
        ).select(
            a['respons_line_id'],
            t['busi_type'],
            col('year_target_value'),
            col('complete_value'),
        )

        y = tmp.select(
            tmp['respons_line_id'],
            tmp['busi_type'],
            tmp['complete_value'],
            when(tmp['year_target_value'] != 0, tmp['complete_value'] / tmp['year_target_value']).otherwise(0).alias('complete_value_rate')
        )

        y = update_dataframe(
            df_to_update=df_in,
            df_use_me=y,
            join_columns=['respons_line_id', 'busi_type'],
            update_columns=['complete_value', 'complete_value_rate']
        )

        return y

    df_result = update_data(df_result)

    return_to_hive(
        spark=spark,
        df_result=df_result,
        target_table="ddw.t_cockpit_bu_anal_targ_resp_y",
        insert_mode="overwrite"
    )

