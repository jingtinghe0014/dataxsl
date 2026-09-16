# dataxsl
personal ETL tool transfers data from one medium to another.

离线数据转换工具。`main.py` 使用多进程模型；后续 `main_async.py` 实现协程模型。

设计上，`channel` 表示数据隔离的通道，每个通道对应一组逻辑 Reader / Writer。`parallel=N` 表示每通道的写并发数，不包含读取单元：`main.py` 对应 1 个读进程 + N 个写进程；`main_async.py` 对应 1 个读线程 + N 个写线程，由协程协调调度。文件暂按单通道读取，数据库后续按主键或辅助键分片到多个通道。

## 架构文档

- [多进程模型架构方案](docs/architecture-multiprocess.md)：`main.py` 的执行流程、插件、配置与改进计划。
- [异步模型架构方案](docs/architecture-async.md)：`main_async.py` 的协程调度、线程读写、隔离通道及实施计划。
