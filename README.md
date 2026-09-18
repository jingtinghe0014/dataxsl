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

## 多进程版运行与验证

运行环境要求 **Python 3.12 及以上版本**，安装包通过 `python_requires >=3.12` 声明最低版本。当前回归验证环境为 Python 3.12.7。

```bash
python -m pip install -e .
# 设置 DB_HOST / DB_USER / DB_PASSWORD / DB_NAME 后使用公开示例。
python main.py -job examples/excel-to-mysql.json -p input_path=/path/to/input.xlsx -p biz_date=2026-09-17
PYTHONPATH=src python -B -m unittest discover -s tests/unit -t . -v
```

当前可用链路为 Excel → MySQL / PostgreSQL。多进程版已加入严格校验、故障退出、资源清理和读写列数检查（字段按位置对应）；异步版仍处于规划阶段。数据库操作按批次提交，失败不会撤销此前已提交的批次。

### PostgreSQL 写入

安装命令会同时安装 Psycopg 3 驱动。配置示例见 [excel-to-postgresql.json](examples/excel-to-postgresql.json)，使用 `postgresql_writer`，仍为 1 个读进程 + `parallel` 个独立写进程。支持 MySQL Writer 相同的 `pre_sql`、`post_sql`、`session`、`additive_attr`、`column_types`，字段按位置对应。

```sql
-- 在测试数据库事先创建目标表。
CREATE TABLE public.etl_demo (
    account text,
    amount numeric,
    biz_date date
);
```

```bash
# 设置 PostgreSQL 的 DB_HOST / DB_USER / DB_PASSWORD / DB_NAME。
python main.py -job examples/excel-to-postgresql.json \
  -p input_path=/path/to/input.xlsx -p biz_date=2026-09-18
```

`table` 可写 `表名` 或 `schema.表名`，大小写按配置原样保留；`column` 填实际字段名，无需自行加引号。数据库名使用 `database` 或 `dbname`，不能同时配置。`connect_timeout` 默认 10 秒；SQL 执行超时通过 `session` 设置 `statement_timeout`（示例为 30 秒），不使用 MySQL 的 `read_timeout` / `write_timeout` / `charset`。目前仅支持 INSERT，尚未实现 PostgreSQL Reader。

`db_config.schema` 可选，例如 `"schema": "test"`。它为未限定的 `table` 补充 schema，并在每个连接执行 `session` 之前设置 `search_path`，因此 `pre_sql`、`post_sql` 中未限定的表也会在该 schema 中查找。示例使用 `table: "etl_demo"` 和 `schema: "public"`，目标仍为 public.etl_demo。已限定的 `table` 优先采用其自身 schema；未配置时保留连接原有的 search_path。schema 必须是非空名称，目标 schema 和表需事先存在。

`pre_sql`、`post_sql`、`session` 原样执行；显式设置 search_path 的 session SQL 可以覆盖初始配置。若真实表名和列名为大写，手写 SQL 也要加双引号，如 `DELETE FROM "test"."ETL_DEMO" WHERE "C_SOURCEDETAILS" = 'demo'`；若数据库实际名称为小写，则 table/column 配置使用小写。参见 [PostgreSQL 标识符规则](https://www.postgresql.org/docs/17/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS)。
