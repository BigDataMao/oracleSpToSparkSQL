# -*- coding: utf-8 -*-
from utils.task_env import create_env, return_to_hive

if __name__ == '__main__':
    spark = create_env()
    data = [("a001", 10000, "D1")]
    df_result = spark.createDataFrame(data, ["id", "salary", "dept_no"])
    return_to_hive(spark, df_result, "test.t_emp", "overwrite")
