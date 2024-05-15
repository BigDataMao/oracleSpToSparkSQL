# -*- coding: utf-8 -*-
import os


# 获取当前文件的路径
def get_current_path():
    return os.path.dirname(os.path.realpath(__file__))


# 获取项目路径
def get_project_path():
    father_path = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
    project_path = os.path.abspath(os.path.dirname(father_path))
    return project_path
