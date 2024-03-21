# 这是一个示例 Python 脚本。
from utils.task_env import create_env, return_to_hive

# 按 Shift+F10 执行或将其替换为您的代码。
# 按 双击 Shift 在所有地方搜索类、文件、工具窗口、操作和设置。


# 按装订区域中的绿色按钮以运行脚本。
if __name__ == '__main__':
    spark = create_env()
    data = [("a001", 10000, "D1")]
    df_result = spark.createDataFrame(data, ["id", "salary", "dept_no"])
    return_to_hive(spark, df_result, "test.t_emp", "overwrite")
# 访问 https://www.jetbrains.com/help/pycharm/ 获取 PyCharm 帮助
