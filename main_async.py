import asyncio
import argparse
from aiodataxsl.core import DataXslContext
from aiodataxsl.logging_config import LoggingManager

logger = LoggingManager.get_logger(__name__)

DATA_XSL_VERSION = '1.0'

def print_copyright():
    print('''
data_xsl (%s), Carlos Tevez. All Rights Reserved.
    ''' % DATA_XSL_VERSION)
    sys.stdout.flush()

async def main():
    parser = argparse.ArgumentParser(description='异步数据交换工具')
    parser.add_argument('--job', required=True, help='任务配置文件')
    parser.add_argument('--param', nargs='*', help='参数替换')

    args = parser.parse_args()

    context = DataXslContext()
    result = context.parse(args)

    print(f"处理结果: {result}")


if __name__ == '__main__':
    asyncio.run(main())