# Oracle Writer（多进程）

使用 `oracle_writer`；完整作业见 [Excel → Oracle](../examples/excel-to-oracle.json)。Python 3.12+，安装 `pip install -e .` 会引入 `oracledb>=3.4,<4`。默认 Thin 模式，无需 Oracle Client；面向 Oracle Database 12.1+，当前不提供 Thick 模式初始化（[驱动兼容要求](https://github.com/oracle/python-oracledb#oracle-database-connectivity)）。驱动连接说明见 [官方文档](https://python-oracledb.readthedocs.io/en/stable/user_guide/connection_handling.html)。

## 配置

```json
{
  "name": "oracle_writer",
  "parameter": {
    "writerMode": "insert",
    "table": "etl_demo",
    "column": ["account", "amount", "biz_date"],
    "column_types": {"amount": "decimal", "biz_date": "date"},
    "additive_attr": {"biz_date": "${biz_date}"},
    "pre_sql": [],
    "post_sql": [],
    "session": [],
    "db_config": {
      "host": "${DB_HOST}",
      "port": 1521,
      "service_name": "${DB_SERVICE_NAME}",
      "user": "${DB_USER}",
      "password": "${DB_PASSWORD}",
      "tcp_connect_timeout": 10,
      "call_timeout": 30000
    }
  }
}
```

- 地址选择：`host + service_name`、`host + sid` 或单独 `dsn`，不能混用。使用 dsn 时，端口写在 dsn 中；可用 `config_dir` 指定 TNS 配置目录。用户名密码单独传递，password 支持现有 ENC 解密。
- `tcp_connect_timeout`：正数，默认 10 秒；`call_timeout`：正整数，默认 30000 毫秒，限制单次数据库往返，不代表作业总超时。dsn 中的连接描述配置可能覆盖单独传入的连接参数。
- 可选 `db_config.schema`：为未限定表名补充 schema，并在每个连接的 session SQL 之前执行 `ALTER SESSION SET CURRENT_SCHEMA`。显式 schema.table 优先；schema 不改变用户权限。
- 拒绝未知配置及 `autocommit=true`。不使用 MySQL 的 database、charset、read_timeout、write_timeout 参数。
- 普通表名、schema、列名按 Oracle 规则转大写；例如 `etl_demo` 对应 `ETL_DEMO`。区分大小写的名称须显式加双引号，例如 JSON 中 `"\"MixedCase\""`。目标表需预先创建，工具不自动建表。
- `pre_sql`、`post_sql`、`session` 支持字符串或字符串列表，原样执行。普通 SQL 不附加 SQL*Plus 的分号或 `/`；PL/SQL 块保留所需的块内分号。

与示例对应的建表语句：

```sql
CREATE TABLE etl_demo (
    account VARCHAR2(200),
    amount NUMBER,
    biz_date DATE
);
```

## 写入、类型和事务

Excel 输入可在 `reader.parameter.column` 配置 `{"index": 0, "type": "string"}`，在入队前把同列中的整数和字符串统一为字符串；decimal 等类型也在读取端转换。index 对应插入 IDX、执行 use_cols 后的输出列位置，转换不改变字段顺序。示例在 Reader 转换 account/amount，Writer 仅转换附加常量 biz_date。已有 Excel column 配置现在生效，应按实际输出位置检查。

`DPY-3013` 表示客户端绑定变量与 Python 值类型不兼容，不能仅凭错误中的 DB_TYPE_VARCHAR 断定实际目标表字段类型。同一批次同一绑定列混有字符串和整数时，应先按配置统一类型；只重置 setinputsizes 无法替代值转换。参考 [驱动维护者对 DPY-3013 的说明](https://github.com/oracle/python-oracledb/discussions/418)。

1 个读进程 + `job.setting.parallel` 个写进程，共享有界阻塞队列。每个写进程独立创建、复用并关闭连接和游标，插件对象不携带活动连接跨进程传递；不实现连接池或 channel 分片。

DataFrame 列与 writer.column 按位置对应，不按名称重排。additive_attr 同名源列原位覆盖，新列按配置顺序追加；column_types 按目标配置名引用，每批检查列数。NULL/NaN/NaT 转为 None，非法类型转换抛错。boolean 转为 NUMBER 可接收的 0/1；date 绑定 DATE，datetime/timestamp 绑定 TIMESTAMP；time 转 ISO 文本，目标应为字符列。Oracle 空字符串具有 NULL 语义。

每批调用 `executemany`，使用 `:1, :2, ...` 绑定值，成功提交后计入 rows_written；不会把业务值拼接到 SQL。每批重设绑定，防止全 NULL 批次影响后续数字或日期绑定。参考 [官方批量写入说明](https://python-oracledb.readthedocs.io/en/v3.4.0/user_guide/batch_statement.html)。

预处理、各批次、后处理各自提交。失败回滚当前未提交事务并传播异常，停止后续阶段；以前已经提交的批次保留。不开启 batcherrors，不自动重试或跳过失败数据。Oracle DDL（如 TRUNCATE）可能隐式提交，不能依赖回滚撤销它或恢复该阶段全部操作。多个写进程不保证最终行顺序。

支持现有 Process timing / Job timing，以及 DEBUG 队列大小日志。单元测试使用模拟数据库连接，集成测试使用真实 spawn 子进程及文件事务替身；未连接真实 Oracle 数据库，驱动与目标表的兼容性及实际吞吐需在测试库验证。
