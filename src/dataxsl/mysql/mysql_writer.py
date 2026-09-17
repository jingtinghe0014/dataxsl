import re
from multiprocessing import current_process, Queue
import pandas as pd
import pymysql

from dataxsl.register import PluginRegistry
from dataxsl.utils import decrypt
from dataxsl.writer import Writer
from dataxsl import LoggingManager
from dataxsl.error import MissingParameterError
from dataxsl.core import key

logger = LoggingManager.get_logger(__name__)

# 7. 定义具体子类 MySQLWriter
@PluginRegistry.register_writer('mysql_writer')
class MySQLWriter(Writer):

    def __init__(self, **mysql_config):

        self.sql_config = mysql_config
        self.db_config = mysql_config.get('db_config')
        self.table = mysql_config.get('table')
        self.column = mysql_config.get('column')
        self.pre_sql = mysql_config.get('pre_sql')
        self.post_sql = mysql_config.get('post_sql')
        self.additive_attr = mysql_config.get('additive_attr')

        # 参数非空要求
        if self.sql_config is None:
            raise MissingParameterError("sql_config")
        if self.db_config is None:
            raise MissingParameterError("db_config")
        if self.table is None:
            raise MissingParameterError("table")
        if self.column is None:
            raise MissingParameterError("column")

        self.schema = {

        }

        # 插入数据到 MySQL
        column = ''
        param = ''
        for x in self.column:
            column += x + ','
            param += '%s' + ','

        self.sql_insert = (
                'insert into ' + self.table + ' ('
                + column[:-1] + ') values (' + param[:-1] + ')')

        logger.debug(f'sql_insert = {self.sql_insert}')

        if self.db_config['password']:
            match_str = re.search(r'ENC\((.*?)\)', self.db_config['password'])
            if match_str:
                self.db_config['password'] = decrypt(bytes.fromhex(match_str.group(1)), key)


    def validate(self):
        pass

    def pre_deal(self):

        if not self.pre_sql or (isinstance(self.pre_sql, list) and len(self.pre_sql) == 0):
            logger.warning("pre_sql为空，跳过执行")
            return

        # 如果pre_sql不为空
        logger.debug(f"预处理SQL列表: {self.pre_sql}")

        try:
            with pymysql.connect(**self.db_config) as connection:
                cursor = connection.cursor()
                sql_list = [self.pre_sql] if isinstance(self.pre_sql, str) else self.pre_sql
                for sql in sql_list:
                    logger.debug(f"Executing pre-SQL: {sql}")
                    cursor.execute(sql)
                connection.commit()
        except pymysql.Error as e:
            logger.error(f"Pre-SQL error: {e}", exc_info = True)
            connection.rollback()
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)


    def post_deal(self):

        if not self.post_sql or (isinstance(self.post_sql, list) and len(self.post_sql) == 0):
            logger.warning("post_sql为空，跳过执行")
            return

        # 如果post_sql不为空
        logger.debug(f"批后处理SQL列表: {self.post_sql}")

        try:
            with pymysql.connect(**self.db_config) as connection:
                cursor = connection.cursor()
                sql_list = [self.post_sql] if isinstance(self.post_sql, str) else self.post_sql
                for sql in sql_list:
                    logger.debug(f"Executing post-SQL: {sql}")
                    cursor.execute(sql)
                connection.commit()
        except pymysql.Error as e:
            logger.error(f"Post-SQL error: {e}", exc_info=True)
            connection.rollback()
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)


    def write_parallel(self,queue:Queue):

        conn = pymysql.connect(**self.db_config)
        try:
            logger.info(f"进程{current_process().name},开始连接 MySQL 数据库...")
            # 连接 MySQL 数据库
            cursor = conn.cursor()
            logger.info(f"进程{current_process().name}, MySQL 数据库连接成功")

            # count = 0
            while True:
                # 从队列中获取数据
                rows: pd.DataFrame = queue.get()
                # logger.debug(f'DataFrame = {row}')
                if rows is None:  # 结束标志
                    break

                if self.additive_attr is not None:
                    for k, v in self.additive_attr.items():
                        rows.loc[:, k] = [v for _ in range(len(rows.index))]

                data_to_insert = []
                for i in rows.index:
                    line: pd.Series = rows.loc[i]
                    # logger.debug(f'当前进程为{current_process().name}，数据为line =  {line.to_dict()}')

                    line_data_tuple = tuple(line.values.tolist())
                    data_to_insert.append(line_data_tuple)
                logger.debug(f'{current_process().name}写入{len(data_to_insert)}行数据')

                cursor.executemany(self.sql_insert, data_to_insert)

                conn.commit()
            cursor.close()
            logger.info(f'数据写入完成。')
        except Exception as e:
            logger.debug(f"{current_process().name}写入 MySQL 数据库时出错: {e}")
            conn.rollback()
            return 1, e
        finally:
            conn.close()

        return 0, None
