#!/bin/bash

# 设置项目基础目录
pyproj_basedir="/Users/carlostevez/PycharmProjects/dataxsl"

# 导出为环境变量
export pyproj_basedir

# 获取上一月月底日期
get_previous_month_end() {
    local first_day_current_month=$(date -v1d +%Y%m%d)
    date -j -v-1d -f "%Y%m%d" "$first_day_current_month" +%Y%m%d
}

# 默认使用系统计算的日期
BIZDATE=$(get_previous_month_end)

# 支持手动指定日期（如果需要覆盖）
while getopts "d:" opt; do
    case $opt in
        d) BIZDATE="$OPTARG" ;;
        *) ;;
    esac
done

echo "使用的业务日期: $BIZDATE"

# 运行Python脚本
python "${pyproj_basedir}/main.py" \
    -job "${pyproj_basedir}/tests/data_json/of/WTTZBB_ZO_HJ.json" \
    -p "bizDate=$BIZDATE"