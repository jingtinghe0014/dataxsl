# DataXSL 架构文档导航

> 更新日期：2026-09-18。同步 PostgreSQL Writer、独立连接器的设计边界及两种执行模型的实现状态。

项目统一以 **Python 3.12+** 为运行基线；多进程版按此声明安装要求，异步模型后续实现沿用同一版本基线。

| 文档 | 入口与执行模型 | 内容重点 |
| --- | --- | --- |
| [多进程模型架构方案](architecture-multiprocess.md) | `main.py`：单条流水线，1 个读进程 + N 个写进程，不按 channel 分片 | 当前执行链、插件、配置样例及可靠性改进 |
| [异步混合模型架构方案](architecture-async.md) | `main_async.py`：每通道独立进程、本地协程调度、1 个读线程 + N 个写线程 | 有界异步队列、背压、分片调度、编码骨架、失败处理及验收清单 |

## 当前实现状态

| 能力 | 多进程模型 | 异步混合模型 |
| --- | --- | --- |
| Excel 读取 | 已实现 `excel_reader` | 草稿，完整链路待实现 |
| MySQL 写入 | 已实现 `mysql_writer` | 待实现线程适配与装配 |
| PostgreSQL 写入 | 已实现 `postgresql_writer`，使用 Psycopg 3 同步连接 | 待实现线程适配与装配 |
| 数据库读取 | 尚未实现可用 Reader，PostgreSQL 本阶段仅支持写入 | 按键分片及 Reader 均为后续目标 |
| 执行单元与队列 | 1 个读进程 + N 个写进程，共享有界阻塞队列 | 目标为每通道 1 个读线程 + N 个写线程，由协程操作通道独立的有界异步队列 |

多进程版的 MySQLWriter 与 PostgreSQLWriter 分别直接实现 Writer 接口，各自负责参数校验、字段处理、连接、批次写入、提交、异常清理和计时。保留独立实现，方便各连接器后续演进；每个写进程独立创建和使用资源。

后续 Iceberg、Hadoop、Greenplum 等连接器按各自目标存储的能力实现 Reader / Writer 接口。公共接口只约束生命周期、批次传递、结果与清理，不统一要求 SQL、游标或数据库事务；配置及提交语义由具体连接器定义。这些连接器尚未实现。

主进程保持 `validate → pre_deal → process_data → post_deal → invoke_hook` 顺序，失败即停止后续业务步骤，所有路径均清理资源。前后 SQL 和各数据批次分别提交，后续失败不会整体回滚已提交的数据。

运行示例：[Excel → MySQL](../examples/excel-to-mysql.json)、[Excel → PostgreSQL](../examples/excel-to-postgresql.json)。两份示例均使用 `main.py`；异步版尚无可用的对应执行链路。

## 两种模型的边界

`parallel=N` 只计算写入执行单元，不包含读取单元。异步模型中的 `channel` 控制并发通道进程上限；每通道的协程独占异步队列，线程只执行读写，队列空/满时由异步等待与唤醒协调负载。文件暂用单通道，数据库按键分片，分片数量与进程数量分离。简单多进程模型不承担分片与多通道调度。

本文件保留为导航，详细架构以对应模型文档为准。
