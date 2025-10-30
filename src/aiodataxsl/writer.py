from abc import ABC, abstractmethod
import asyncio
from typing import Any, Optional

class AsyncWriter(ABC):
    """异步写入器基类"""

    @abstractmethod
    async def write_async(self, queue: asyncio.Queue) -> tuple[int, Optional[Exception]]:
        pass

    @abstractmethod
    async def validate(self) -> tuple[int, Optional[Exception]]:
        pass

    @abstractmethod
    async def pre_deal(self) -> tuple[int, Optional[Exception]]:
        pass

    @abstractmethod
    async def post_deal(self) -> tuple[int, Optional[Exception]]:
        pass