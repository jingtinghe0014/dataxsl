import os
import re
import sys

import time
from multiprocessing import Queue
from pathlib import Path
from typing import Union
import msoffcrypto
import numpy as np
import pandas as pd
from python_calamine import CalamineWorkbook

import dataxsl.core
from dataxsl import LoggingManager
from dataxsl.register import PluginRegistry
from dataxsl.reader import Reader
from dataxsl.error import MissingParameterError, ParameterTypeError
from dataxsl.utils import decrypt

logger = LoggingManager.get_logger(__name__)
_CellValue = Union[int, float, str, bool]


# 2. 定义具体子类 ExcelReader
@PluginRegistry.register_reader('excel_reader')
class ExcelReader(Reader):
    json_schema = {}

    def __init__(self, path=None, sheet_name=None, exl_password=None, chunk_size=1000, parallel=1, header=0,
                 skip_rows=None, use_cols=None, ins_row_num = False, encode='utf-8'):
        """
        :type path: excel文件路径，必填
        :type sheet_name: excel文件读取的sheet_name，必填
        :type exl_password: 如果excel存在密码，需要指定密码
        :type chunk_size: 分片读取数据，数据片大小
        :type parallel: 并行度，指定读取线程对应多少个并行写入线程
        :type header: 数据头对应的列，数字从0 开始
        :type skip_rows: 忽略部分行，如果存在多重表头需要忽略相关列
        :type use_cols: 如果只需要导入部分列则需要指定该参数
        :type encode: 如果存在特殊编码需要特殊指定，目前暂时无用
        :type ins_row_num: 是否将行号插入dataframe，默认不加载行号

        """
        self.file_path = path
        self.sheet_name = sheet_name
        self.exl_password = exl_password
        self.chunk_size = chunk_size
        self.parallel = parallel
        self.header = header
        self.skip_rows = skip_rows
        self.use_cols = use_cols
        self.encode = encode
        self.ins_row_num = ins_row_num

        if self.file_path is None:
            raise MissingParameterError("file_path")
        if self.sheet_name is None:
            raise MissingParameterError("sheet_name")


    def validate(self):
        pass

    def pre_deal(self):
        # 如果是加密文件，则在读取前需要进行解密
        if self.exl_password:
            match_str = re.search(r'ENC\((.*?)\)', self.exl_password)
            if match_str:
                self.exl_password = decrypt(bytes.fromhex(match_str.group(1)), dataxsl.core.key)
            self.file_path = self._decrypt_excel()

    def post_deal(self):
        # 如果是加密文件则需要删除
        if self.exl_password:
            if os.path.exists(self.file_path):
                os.remove(self.file_path)
                logger.debug(f"文件 {self.file_path} 删除成功！")
            else:
                logger.debug(f"文件 {self.file_path} 不存在。")

    @staticmethod
    def _convert_cell(value: _CellValue):
        if isinstance(value, float):
            val = int(value)
            if val == value:
                return val
            else:
                return value
        if value == '':
            return None
        return value

    def _decrypt_excel(self):

        logger.info("开始解密 Excel 文件...")
        p = Path(self.file_path)
        decrypted_path = p.with_name(p.stem + '_decrypted' + p.suffix)

        with open(self.file_path, "rb") as f:
            file = msoffcrypto.OfficeFile(f)
            file.load_key(password=self.exl_password)  # Use password
            with open(decrypted_path, 'wb') as w:
                file.decrypt(w)
        return decrypted_path

    def read_parallel(self, queue: Queue):

        logger.info("开始读取 Excel 文件...")
        logger.debug(f"文件路径：{self.file_path}")
        start_read_time = time.time()

        try:

            workbook = CalamineWorkbook.from_path(self.file_path)
            sheet = workbook.get_sheet_by_name(self.sheet_name)

            header_row = []
            count = 0
            chunk = []
            # 获取所有行
            for rows in sheet.iter_rows():
                # logger.debug(f'rows = {rows}')
                # 自增1
                count += 1
                if self.header >= count - 1:
                    if self.header == count - 1:
                        if self.ins_row_num:
                            header_row = ['IDX'] + rows
                            # logger.debug(f'header_row = {header_row}')
                        else:
                            header_row = rows  # 获取表头
                    continue
                # 需要skip的行
                if self.skip_rows:

                    if isinstance(self.skip_rows, list):
                        if count - 1 in self.skip_rows:
                            continue
                    elif isinstance(self.skip_rows, int):
                        if count - 1 == self.skip_rows:
                            continue
                    else:
                        raise ParameterTypeError('参数skip_rows类型错误。')
                # 数据加入List
                chunk.append(rows)
                if len(chunk) >= self.chunk_size:
                    # 将 chunk 转换为 DataFrame
                    if self.ins_row_num:
                        data = [[index+1] + [ExcelReader._convert_cell(cell) for cell in row] for index, row in enumerate(chunk)]
                    else:
                        data = [[ExcelReader._convert_cell(cell) for cell in row] for row in chunk]

                    df = pd.DataFrame(data, columns=header_row)
                    # 保留需要的列
                    if self.use_cols:
                        logger.debug(f'保留的列为:{self.use_cols}')
                        df = df.iloc[:, self.use_cols]

                    df.dropna(how='all')
                    df = df.replace(np.nan, None)

                    queue.put(df)  # 放入队列
                    chunk = []  # 清空 chunk

            # 处理剩余的行
            if chunk:

                if self.ins_row_num:
                    data = [[index + 1] + [ExcelReader._convert_cell(cell) for cell in row] for index, row in
                            enumerate(chunk)]
                else:
                    data = [[ExcelReader._convert_cell(cell) for cell in row] for row in chunk]

                df = pd.DataFrame(data, columns=header_row)

                # logger.debug(f'header_row = {header_row}')
                # logger.debug(f'data = {data}')
                # 保留需要的列
                if self.use_cols:
                    logger.debug(f'保留的列为:{self.use_cols}')
                    df = df.iloc[:, self.use_cols]
                # logger.debug(f'df[item] = {df}')
                df.dropna(how='all')
                df = df.replace(np.nan, None)
                queue.put(df)

            logger.info("Excel 文件读取完成")
            end_read_time = time.time()
            logger.info(f'读取用时{end_read_time - start_read_time}')

            return 0, None

        except Exception as e:
            logger.debug(f"读取 Excel 文件时出错: {e}")
            return 1, e

        finally:
            # 放入结束标志
            logger.debug(f'全局变量为parallel = {self.parallel}')
            for i in range(self.parallel):
                queue.put(None)
