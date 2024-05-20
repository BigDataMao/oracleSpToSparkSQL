from config import Config
from utils.io_utils.path_utils import get_project_path

instance1 = Config(get_project_path() + "/config.json")
instance2 = Config(get_project_path() + "/config.json")

print(instance1 == instance2)
print(instance1 is instance2)
print(instance1.get_logger() == instance2.get_logger())
print(instance1.get_logger() is instance2.get_logger())

# 打印结果
# True
# True
# True
# True
