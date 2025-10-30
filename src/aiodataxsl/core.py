from aiodataxsl.error import MissingParameterError
from aiodataxsl.logging_config import LoggingManager
from aiodataxsl.reader import AsyncReader
from aiodataxsl.writer import AsyncWriter
import asyncio
from typing import Optional, Dict, Any
from string import Template
import os
import json


logger = LoggingManager.get_logger(__name__)

class AsyncDataProcessor:
    """异步数据处理器"""

    def __init__(self, reader: AsyncReader, writer: AsyncWriter):
        self.reader = reader
        self.writer = writer
        #self.max_concurrent = max_concurrent
        self.data_queue = asyncio.Queue(maxsize=1000)
        self._stop_event = asyncio.Event()

    async def start(self) -> dict:
        """启动异步处理流程"""
        try:
            # 1. 预处理
            await self.pre_deal()

            # 2. 启动读写任务
            results = await asyncio.gather(
                self._run_reader(),
                self._run_writer(),
                return_exceptions=True
            )

            # 3. 后处理
            await self.post_deal()

            return self._format_results(results)

        except Exception as e:
            logger.error(f"异步处理失败: {e}")
            return {"status": "error", "error": str(e)}

    async def _run_reader(self) -> tuple[int, Optional[Exception]]:
        """运行读取器"""
        try:
            return await self.reader.read_async(self.data_queue)
        except Exception as e:
            logger.error(f"读取器执行失败: {e}")
            return 1, e

    async def _run_writer(self) -> tuple[int, Optional[Exception]]:
        """运行写入器"""
        try:
            return await self.writer.write_async(self.data_queue)
        except Exception as e:
            logger.error(f"写入器执行失败: {e}")
            return 1, e

    async def pre_deal(self) -> None:
        """异步预处理"""
        await asyncio.gather(
            self.reader.pre_deal(),
            self.writer.pre_deal()
        )

    async def post_deal(self) -> None:
        """异步后处理"""
        await asyncio.gather(
            self.reader.post_deal(),
            self.writer.post_deal()
        )

    @staticmethod
    def _format_results(results: tuple) -> dict:
        """格式化结果"""
        reader_result, writer_result = results

        return {
            "status": "success",
            "reader": {"code": reader_result[0], "error": str(reader_result[1]) if reader_result[1] else None},
            "writer": {"code": writer_result[0], "error": str(writer_result[1]) if writer_result[1] else None}
        }

    async def stop(self) -> None:
        """停止处理"""
        self._stop_event.set()


class DataXslContext:
    """异步数据处理上下文"""

    def __init__(self):
        self.construct_config = {}
        self.reader_config = {}
        self.writer_config = {}
        self.process_config = {"max_concurrent": 3}

    async def parse(self, arguments) -> Dict[str, Any]:
        """异步解析配置并执行"""
        # 读取和解析配置（同步操作，但很快）
        sf_config = await self._load_config(arguments)

        # 构建处理器
        processor = await self._build_processor(sf_config)

        # 异步执行
        result = await processor.start()
        return result

    async def _load_config(self, arguments) -> Dict[str, Any]:
        """异步加载配置"""
        loop = asyncio.get_event_loop()

        def load_sync():
            if not os.path.exists(arguments.job):
                raise FileNotFoundError(f"配置文件不存在: {arguments.job}")

            with open(arguments.job, 'r', encoding="utf-8") as file:
                json_str = file.read()

            # 参数替换
            if arguments.param:
                config = dict(map(lambda x: x.split('='), arguments.param))
                t = Template(json_str)
                json_str = t.safe_substitute(config)

            return json.loads(json_str)

        return await loop.run_in_executor(None, load_sync)

    async def _build_processor(self, sf_config: Dict[str, Any]) -> AsyncDataProcessor:
        """构建异步处理器"""
        # 提取配置
        content = sf_config['job']['content'][0]
        self.construct_config = {
            'reader': content['reader']['name'],
            'writer': content['writer']['name']
        }

        self.reader_config = content['reader']['parameter']
        self.writer_config = content['writer']['parameter']

        # 清理reader配置
        if 'column' in self.reader_config:
            self.reader_config.pop('column')

        # 设置并发数
        if sf_config['job']['setting']['speed']['channel']:
            self.process_config['max_concurrent'] = sf_config['job']['setting']['speed']['channel']

        # 创建异步读写器
        reader = await self._create_async_reader()
        writer = await self._create_async_writer()

        return AsyncDataProcessor(reader, writer, **self.process_config)

    async def _create_async_reader(self) -> AsyncReader:
        """创建异步读取器"""
        reader_name = self.construct_config.get('reader')
        if not reader_name:
            raise MissingParameterError('未设置reader参数')

        # 这里需要将同步Reader适配为异步Reader
        # 实际项目中可以创建专门的异步Reader类
        reader_cls = PluginRegistry.get_reader(reader_name)
        return await self._adapt_sync_reader_to_async(reader_cls, self.reader_config)

    async def _create_async_writer(self) -> AsyncWriter:
        """创建异步写入器"""
        writer_name = self.construct_config.get('writer')
        if not writer_name:
            raise MissingParameterError('未设置writer参数')

        writer_cls = PluginRegistry.get_writer(writer_name)
        return await self._adapt_sync_writer_to_async(writer_cls, self.writer_config)

    async def _adapt_sync_reader_to_async(self, reader_cls, config):
        """将同步Reader适配为异步（临时方案）"""
        from dataxsl.async_adapter import AsyncAdapter
        sync_reader = reader_cls(**config)
        return AsyncAdapter.sync_reader_to_async(sync_reader)

    async def _adapt_sync_writer_to_async(self, writer_cls, config):
        """将同步Writer适配为异步（临时方案）"""
        from dataxsl.async_adapter import AsyncAdapter
        sync_writer = writer_cls(**config)
        return AsyncAdapter.sync_writer_to_async(sync_writer)