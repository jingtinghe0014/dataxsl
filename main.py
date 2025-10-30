import argparse
from pathlib import Path
import sys
import time
from multiprocessing import freeze_support


# # 获取当前脚本（main.py）的绝对路径，然后找到其父目录（项目根目录）
# current_file = Path(__file__).resolve()  # __file__ 表示当前文件（main.py）的路径
# project_root = current_file.parent     # .parent 获取父目录，即项目根目录
#
# # 构建 src 目录的路径
# src_dir = project_root / "src"
#
# print(src_dir)
# # 将 src 目录添加到 Python 路径
# if str(src_dir) not in sys.path:
#     sys.path.insert(0, str(src_dir))

from dataxsl.core import DataXslContext
from dataxsl.logging_config import LoggingManager

logger = LoggingManager.get_logger(__name__)

DATA_XSL_VERSION = '1.0'


def print_copyright():
    print('''
data_xsl (%s), From TaiPingFinTech !
Copyright (C) 2024-2025, Carlos Tevez. All Rights Reserved.
    ''' % DATA_XSL_VERSION)
    sys.stdout.flush()

# 按装订区域中的绿色按钮以运行脚本。
if __name__ == '__main__':
    freeze_support()

    print_copyright()
    usage = "%(prog)s [options] job-url-or-path"
    parser = argparse.ArgumentParser(usage=usage, allow_abbrev=False)
    parser.add_argument('-job', help='job-json must be needed prompt.')
    parser.add_argument('-p', '--param', action='append', nargs='?', help='param could prompt.')
    args = parser.parse_args()

    if args.job is None:
        parser.print_help()
        print(f"错误：job参数缺失", file=sys.stderr)
        sys.exit(-1)

    start_time = time.time()
    context = DataXslContext()
    # 解析输入参数，进行数据替换，替换job中的变量，并将参数存入context
    try:
        res, exception = context.parse(args)
    except Exception as e:
        logger.error(f'数据处理异常：{e}')
        print(f'数据处理异常：{e}', file=sys.stderr)
        sys.exit(-1)

    logger.info("程序结束")
    end_time = time.time()

    logger.info(f'共用时{end_time - start_time}')