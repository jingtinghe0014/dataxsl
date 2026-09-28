# REST API Reader

> 更新日期：2026-09-20。适用于 Python 3.12+、`main.py` 多进程模型。插件名为 `restapi_reader`，直接实现 Reader 接口；暂未实现 REST Writer 或异步适配。

## 1. 请求与认证配置

完整示例：[REST API → MySQL](../examples/restapi-to-mysql.json)。Reader 参数如下：

| 参数 | 说明 / 默认值 |
| --- | --- |
| `url` | 必填，读取数据的 HTTP/HTTPS 地址 |
| `method` | GET 或 POST，默认 GET |
| `headers` | 请求头字典，默认空；认证请求也携带这些头，但移除数据请求的 Token 头；用户名密码通过请求参数传递 |
| `response_type` | 当前只支持 json |
| `requestParam` | 查询字符串或对象，默认空对象；如 `A2=20260101&A2_copy=20260131&pageSize=1000` |
| `json_path` | 数据数组的点分路径，默认 `data.data`；不支持通配符、数组索引或完整 JSONPath 语法 |
| `Authorization` | 可选的认证配置，缺省时直接使用 headers 发起请求 |
| `read_timeout` | 正整数，毫秒，默认 60000；传给 HTTP 客户端时除以 1000 |
| `encode` | 响应编码，默认 utf-8 |
| `batch_size` | DataFrame 最大行数，默认 1000；与接口页大小独立 |
| `max_retry` | 失败后的额外重试次数，默认 3；0 表示不进行普通失败重试 |
| `backoff_factor` | 指数退避系数，单位秒，默认 1；0 表示立即重试 |
| `pagination` | 响应中的分页元数据路径，缺省时仅请求一次 |
| `column` | 可选的源列索引与类型配置，见第 3 节 |

GET 将 requestParam 作为查询参数。POST 根据 Content-Type 发送 JSON 对象或表单；未设置 Content-Type 时发送 JSON。查询字符串按 URL 编码规则解析，保留空字符串值，同名参数保留最后一个值。日期等业务值不会自动转成数字，分页参数单独标准化为整数。

连接在 Reader 子进程中创建并复用，关闭后释放；禁用环境代理和 netrc 自动认证，使用作业显式配置的认证信息。HTTP 重定向不自动跟随，3xx 作为请求失败处理。

`read_timeout` 同时用于连接等待和读数据等待，不是整个请求下载或整个作业的绝对时间上限。参见 [Requests 超时说明](https://requests.readthedocs.io/en/latest/user/quickstart/#timeouts)。

### 1.1 可配置 Token 提取

对于返回结构：

```json
{
  "success": true,
  "code": 200,
  "data": {"Authorization": "Bearer example-token"}
}
```

使用下面的配置片段：

```json
{
  "auth_type": "basic",
  "auth_url": "https://api.example.com/token/{auth_username}/{auth_password}",
  "method": "GET",
  "auth_username": "${API_USERNAME}",
  "auth_password": "${API_PASSWORD}",
  "token_path": "data.Authorization",
  "token_header": "Authorization",
  "token_prefix": ""
}
```

- `auth_type` 目前保留 `basic` 配置值，表示用户名密码登录。`auth_username`、`auth_password` 提供登录凭据，不使用 HTTP Basic Auth。密码支持现有 `ENC(...)` 解析。
- `auth_url` 支持路径模板 `{auth_username}`、`{auth_password}`；使用模板时必须同时包含两者，且只能出现在路径中。实际请求将凭据分别进行 URL 编码后替换占位符，不发送凭据查询参数或请求体。例如 `/token/{auth_username}/{auth_password}` 将请求 `/token/<用户名>/<密码>`。
- 未使用路径模板时，保留参数登录方式。认证 `method` 支持 GET/POST，默认 GET：GET 通过查询参数传递用户名密码；POST 按 `headers` 的 Content-Type 传递，缺省或 JSON 类型使用 JSON 请求体，其他类型使用表单。认证请求只传递上述两个参数，不附加数据接口的 `requestParam`。
- `token_path` 指向返回 JSON 中的字符串节点。缺省时识别根 access_token/token、data.access_token/data.token、字符串 data，或响应本身为字符串。
- `token_header` 默认 Authorization；`token_prefix` 默认 `Bearer `。头值为 prefix 与返回字符串直接拼接。返回值已包含 Bearer 时应将 prefix 配置为空，避免重复前缀。
- headers 已提供 Token 头时先使用该值；没有 Token 时先认证。数据请求 HTTP 401 时重新获取 Token 并重发同一页，每页最多执行一次 401 刷新，第二次仍为 401 则失败。
- Token 响应的根或 data 节点含正数 expires_in 时，按秒缓存有效期，到期后在下一次数据请求前刷新；否则依赖 HTTP 401 判断过期。
- 认证失败、没有可用 Token、配置的 token_path 不可用均报错；不会将响应正文、Token、密码或完整请求参数写入错误信息。

HTTP 200 响应内的业务 `code` 不作为通用 Token 过期信号；不同接口的业务错误码含义需要单独约定。HTTP 403 按失败处理，不反复刷新 Token。

## 2. 分页与批次

```json
{
  "json_path": "data.data",
  "requestParam": "pageSize=1000",
  "pagination": {
    "total": "data.total",
    "pageSize": "data.pageSize",
    "pageNum": "data.pageNum"
  },
  "batch_size": 600
}
```

`data.data` 表示根对象的 data 下的 data 数组。分页的三个配置也是响应路径；pageNum/pageSize 路径的末级名称分别作为请求参数名，例如向数据接口发送 pageNum=1、pageSize=1000。

- 默认从第 1 页开始；requestParam 可指定起始页及页大小，未指定页大小时使用 batch_size。
- 首次响应的 pageSize 可小于请求值，后续请求沿用实际返回的页大小。后续响应的 total 和 pageSize 必须保持一致。
- 校验返回页号等于请求页号；本页记录数应为 `min(pageSize, max(0, total - (pageNum - 1) * pageSize))`。元数据缺失、负数、意外空页、页号重复或页行数不符均失败，避免静默漏读或死循环。
- 当前页覆盖 total 后停止，不额外请求一个空尾页。total=0 时不输出 DataFrame，由引擎正常发送结束标记。
- 一页可以拆成多个 DataFrame，一个 DataFrame 也可以包含相邻两页的数据。以上例子，1000 行的第一页先输出 600 行，剩余 400 行与下一页的 200 行组成下一批。
- 只缓存当前响应页和待输出批次；不累积所有页面。HTTP 响应正文与 JSON 解码仍会占用一页内存，batch_size 不限制接口单页响应大小。

接口应提供稳定的离线数据范围及排序；相同 total 不代表分页期间源数据没有变化，本实现不提供服务端快照或跨故障 exactly-once 保证。

## 3. 字段与类型

`column` 按源列位置选择和转换，不增加源名称到目标名称的字段映射：

```json
[
  {"index": "0", "type": "string"},
  {"index": "1", "type": "decimal"}
]
```

- index 从 0 开始，接受非负整数或数字字符串；按 column 配置顺序输出，禁止重复索引。
- type 可选，使用现有值转换工具；日期类型可配置 format。未指定 type 时保留原值并统一空值。
- 数据节点支持对象数组或二维数组。对象数组以首条记录的键顺序定义源列索引，后续记录按已确定的源列顺序读取；JSON 对象键序变化不会交换字段。接口应保证首条记录字段顺序符合列索引配置。
- 后续记录的键集合、数组宽度或记录类型变化时失败；嵌套对象/数组单元格不自动展开。
- 未配置 column 或为空数组时输出所有源列。空结果且没有列配置时无法推断源字段，输出列信息为 None。
- Writer 继续按位置写入。Excel 的历史 reader.column 仍作为元数据移除；REST 的 reader.column 保留并由 REST Reader 实际处理。

## 4. 重试和清理

重试连接错误、超时、HTTP 408/429/5xx；其余 HTTP 错误、无效 JSON、字段/分页契约错误立即失败。默认 max_retry=3 表示最多 1 次初始请求加 3 次重试。认证请求与每页数据请求各自计数；401 的一次刷新独立于普通重试预算。

第 n 次重试前等待 `min(60, backoff_factor * 2 ** (n - 1))` 秒：默认依次为 1、2、4 秒。单次等待最多 60 秒，不解析 Retry-After。重试的是当前页请求，成功后才处理数据和增加页号。POST 仅用于读取数据的查询接口，接口需允许同一查询重复调用。

正常读写期间，队列停止事件可中断退避等待及下一次请求；正在进行的 HTTP 调用受请求超时约束。Reader 初始化阶段和无法合作退出的情况由现有进程监督器按清理期限终止并回收。响应处理后关闭 Response，读完或失败时关闭 Session，cleanup 可重复调用。

主进程顺序保持 validate → pre_deal → process_data → post_deal → invoke_hook。REST validate 校验配置且不联网，pre_deal 仅解析凭据；prepare_read 在子进程中认证并预取第一页、确定字段，read_parallel 复用第一页继续读取。认证或数据校验失败发生在 Writer pre_sql 之后，不能撤销已提交的预处理。

仍仅输出既有进程总耗时和作业总耗时日志。详细结果中 init_timings.prepare_seconds 是初始化耗时，Reader stages 中 http_seconds/backoff_seconds 覆盖初始化和正式读取，dataframe_seconds/enqueue_seconds 为正式输出阶段；这些存在包含关系，不能重复相加。

## 5. 验证范围

单元测试覆盖参数校验、配置化 Token、已有 Token、过期刷新、HTTP 重试和退避、JSON 节点、分页一致性、跨页批次、列索引/类型及取消清理。回环 HTTP 服务配合真实 spawn 进程验证用户名密码路径认证、401 刷新、503 重试、两个写进程消费及失败后不执行后处理。

测试不访问业务配置的接口或数据库。生产接口的业务错误码、认证协议、字段顺序和稳定分页仍应按接口实际约定核对。
