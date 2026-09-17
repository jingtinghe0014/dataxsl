# dataxsl
personal ETL tool transfers data from one medium to another.

离线数据转换工具。`main.py` 使用简单多进程模型；后续 `main_async.py` 实现进程隔离、协程调度与线程读写结合的异步混合模型。

已确认的编码目标：

- `main.py`：一条流水线，1 个读进程 + N 个写进程，不按 channel 分片。
- `main_async.py`：`channel=C` 控制最多 C 个独立通道进程；每通道有本地事件循环、有界 `asyncio.Queue`、1 个读线程和 N 个写线程。协程操作队列，线程执行实际读写；通过队列背压和唤醒协调负载。
- `parallel=N` 只计算写入单元。第一版固定 N，队列不自动扩缩线程。
- 分片仅在异步模型中调度：文件暂按单通道，数据库按主键或辅助键分片；分片数量与通道进程数独立。

## 架构文档

- [多进程模型架构方案](docs/architecture-multiprocess.md)：单条流水线的执行流程、插件、配置与改进计划。
- [异步混合模型架构方案](docs/architecture-async.md)：通道进程、非阻塞调度、逐批线程读写、故障处理及编码验收清单。
