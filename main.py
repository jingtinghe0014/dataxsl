import argparse
from multiprocessing import freeze_support
from pathlib import Path
import sys

# Prefer this checkout when running the repository entry point.
sys.path.insert(0, str(Path(__file__).resolve().parent / 'src'))

from dataxsl import __version__
from dataxsl.core import DataXslContext, logger
from dataxsl.logging_config import LoggingManager
from dataxsl.utils import redact

DATA_XSL_VERSION = __version__


def print_copyright():
    print(f'data_xsl ({DATA_XSL_VERSION}), Carlos Tevez. All Rights Reserved.', flush=True)


def main(argv=None):

    parser = argparse.ArgumentParser(description='Single-pipeline offline data conversion', allow_abbrev=False)
    parser.add_argument('-job', '--job', required=True, help='Local job JSON file')
    parser.add_argument('-p', '--param', action='append', help='Template parameter: key=value')
    args = parser.parse_args(argv)
    LoggingManager.setup_logging()
    logger = LoggingManager.get_logger(__name__)
    print_copyright()
    context = DataXslContext()
    try:
        context.parse(args)
    except KeyboardInterrupt:
        logger.error('Job interrupted; workers have been stopped')
        return 130
    except Exception as exc:
        secrets = [arg.split('=', 1)[1] for arg in (args.param or []) if '=' in arg]
        db_config = context.writer_config.get('db_config')
        if isinstance(db_config, dict):
            secrets += [db_config.get('password'), db_config.get('passwd')]
        secrets.append(context.reader_config.get('exl_password'))
        message = redact(str(exc), secrets)
        logger.error('Job failed: %s: %s', type(exc).__name__, message)
        print(f'数据处理异常：{message}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    freeze_support()
    sys.exit(main())
