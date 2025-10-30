#!/bin/bash

# 设置项目基础目录
pyproj_basedir="/Users/carlostevez/PycharmProjects/dataxsl"

# 导出为环境变量
export pyproj_basedir

# 运行Python脚本
python "${pyproj_basedir}/main.py" -job "${pyproj_basedir}/tests/data_json/tpa/WTTZBB_TPA_ZY_HJ.json" -p "bizDate=20240731"