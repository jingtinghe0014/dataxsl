import asyncio
import os
from pathlib import Path

import msoffcrypto
import numpy as np
import pandas as pd
import time
from typing import Union, Optional

from python_calamine import CalamineWorkbook

from aiodataxsl.error import MissingParameterError, ParameterTypeError
from aiodataxsl.logging_config import LoggingManager
from aiodataxsl.reader import AsyncReader

logger = LoggingManager.get_logger(__name__)

_CellValue = Union[int, float, str, bool]

class AsyncExcelReader(AsyncReader):
    """异步Excel读取器"""

    def __init__(self, path=None, sheet_name=None, exl_password=None,
                 chunk_size=1000, header=0, skip_rows=None, use_cols=None, encode='utf-8'):
        self.file_path = path
        self.sheet_name = sheet_name
        self.exl_password = exl_password
        self.chunk_size = chunk_size
        self.header = header
        self.skip_rows = skip_rows
        self.use_cols = use_cols
        self.encode = encode

        if self.file_path is None:
            raise MissingParameterError("file_path")
        if self.sheet_name is None:
            raise MissingParameterError("sheet_name")

    async def validate(self) -> tuple[int, Optional[Exception]]:
        """异步验证"""
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"文件不存在: {self.file_path}")
        return True, None


    async def pre_deal(self) -> None:
        # 异步执行文件解密（如果有密码）
        self.file_path = await self._async_decrypt_excel() if self.exl_password else self.file_path

    async def post_deal(self) -> None:
        """异步后处理"""
        logger.info("Excel读取后处理")
        if self.exl_password:
            if os.path.exists(self.file_path):
                os.remove(self.file_path)
                logger.debug(f"文件 {self.file_path} 删除成功！")
            else:
                logger.debug(f"文件 {self.file_path} 不存在。")

        await asyncio.sleep(0)

    async def read_async(self, queue: asyncio.Queue) -> tuple[int, Optional[Exception]]:
        """异步读取Excel数据"""
        try:
            logger.info("开始异步读取Excel文件")
            start_time = time.time()

            # 使用线程池执行阻塞IO操作
            #loop = asyncio.get_event_loop()

            # 异步读取Excel数据
            result = await self._async_read_excel_data(self.file_path, queue)

            end_time = time.time()
            logger.info(f"Excel异步读取完成，用时: {end_time - start_time:.2f}秒")

            return result

        except Exception as e:
            logger.error(f"异步读取Excel失败: {e}")
            return 1, e

    async def _async_decrypt_excel(self) -> Path:
        """异步解密Excel文件"""
        loop = asyncio.get_event_loop()

        def decrypt_sync():
            logger.info("开始解密Excel文件...")
            p = Path(self.file_path)
            decrypted_path = p.with_name(p.stem + '_decrypted' + p.suffix)

            with open(self.file_path, "rb") as f:
                file = msoffcrypto.OfficeFile(f)
                file.load_key(password=self.exl_password)
                with open(decrypted_path, 'wb') as w:
                    file.decrypt(w)
            return decrypted_path

        return await loop.run_in_executor(None, decrypt_sync)

    async def _async_read_excel_data(self, file_path: str, queue: asyncio.Queue) -> tuple[int, Optional[Exception]]:
        """异步读取Excel数据"""
        loop = asyncio.get_event_loop()

        def read_sync():
            workbook = CalamineWorkbook.from_path(file_path)
            sheet = workbook.get_sheet_by_name(self.sheet_name)

            header_row = []
            count = 0
            chunk = []

            for rows in sheet.iter_rows():
                count += 1
                if self.header >= count - 1:
                    if self.header == count - 1:
                        header_row = rows
                    continue

                if self.skip_rows:
                    if isinstance(self.skip_rows, list):
                        if count - 1 in self.skip_rows:
                            continue
                    elif isinstance(self.skip_rows, int):
                        if count - 1 == self.skip_rows:
                            continue
                    else:
                        raise ParameterTypeError('参数skip_rows类型错误。')

                chunk.append(rows)
                if len(chunk) >= self.chunk_size:
                    data = [[self._convert_cell(cell) for cell in row] for row in chunk]
                    df = pd.DataFrame(data, columns=header_row)

                    if self.use_cols:
                        df = df.iloc[:, self.use_cols]

                    df.dropna(how='all')
                    df = df.replace(np.nan, None)

                    # 将数据放入队列（这里还是同步的，会在外层处理）
                    yield df
                    chunk = []

            if chunk:
                data = [[self._convert_cell(cell) for cell in row] for row in chunk]
                df = pd.DataFrame(data, columns=header_row)

                if self.use_cols:
                    df = df.iloc[:, self.use_cols]

                df.dropna(how='all')
                df = df.replace(np.nan, None)
                yield df

            # 清理临时文件
            if self.exl_password and os.path.exists(file_path):
                os.remove(file_path)

            return count - 1  # 返回处理的行数

        # 异步执行读取操作
        row_count = 0
        try:
            # 使用生成器异步处理数据
            data_generator = await loop.run_in_executor(None, read_sync)

            # 如果是生成器，异步处理每个数据块
            if hasattr(data_generator, '__iter__'):
                for df in data_generator:
                    # 异步放入队列
                    await queue.put(df)
                    row_count += len(df)
                    # 让出控制权，避免阻塞事件循环
                    await asyncio.sleep(0)

            # 放入结束标志
            await queue.put(None)

            logger.info(f"成功读取 {row_count} 行数据")
            return 0, None

        except Exception as e:
            logger.error(f"读取Excel数据失败: {e}")
            return 1, e

    @staticmethod
    def _convert_cell(value: _CellValue):
        """转换单元格值（保持原有逻辑）"""
        if isinstance(value, float):
            val = int(value)
            if val == value:
                return val
            else:
                return value
        if value == '':
            return None
        return value