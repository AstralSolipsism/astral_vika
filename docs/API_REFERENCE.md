# Astral Vika API Reference

`astral_vika` 是异步优先的 Vika 维格表 Fusion API Python SDK。当前版本为 `1.1.3`，支持 Python 3.8+。主包只提供 SDK 能力；`vika_mcp` 是可选集成，未安装时相关测试会跳过。

## 入口

```python
from astral_vika import Vika

async with Vika("your_api_token") as vika:
    datasheet = vika.datasheet("dstXXXXXXXXXXXXXX")
    records = await datasheet.records.all().limit(10).aall()
```

### `Vika`

构造参数：

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| `token` | `str` | Vika API Token |
| `api_base` | `Optional[str]` | API 基础地址，默认 `https://vika.cn` |
| `status_callback` | `Optional[Callable]` | 异步状态回调 |

常用方法：

| 方法 | 说明 |
| --- | --- |
| `space(space_id)` | 获取 `Space` 实例 |
| `datasheet(dst_id_or_url, space_id=None, field_key="name")` | 获取 `Datasheet` 实例 |
| `aget_spaces()` | 异步获取空间列表 |
| `aauth()` / `atest_connection()` | 验证 token 是否可用 |
| `aclose()` | 关闭底层 HTTP 会话 |

## 空间站与节点

### `SpaceManager`

| 方法 | 说明 |
| --- | --- |
| `get(space_id)` | 获取空间实例 |
| `alist()` | 异步列出空间 |
| `aexists(space_id)` | 判断空间是否存在 |
| `afind_by_name(space_name)` | 按名称查找空间 |
| `aget_default_space()` | 获取第一个空间 |

### `Space`

| 属性/方法 | 说明 |
| --- | --- |
| `datasheets` | 数据表管理器 |
| `nodes` | 节点管理器 |
| `member` / `role` / `team` | 组织单元管理器 |
| `datasheet(dst_id_or_url, field_key="name")` | 获取数据表实例 |
| `acreate_datasheet(name, ...)` | 创建数据表 |
| `aget_space_info()` | 从空间列表中返回当前空间的真实信息 |
| `asearch_nodes(query=None, node_type=None)` | 搜索节点 |

### `NodeManager`

| 方法 | 说明 |
| --- | --- |
| `alist()` / `aall()` | 获取节点列表 |
| `aget(node_id)` | 获取节点详情 |
| `asearch(node_type=None, permission=None, query=None, permissions=None)` | 使用 v2 API 搜索节点 |
| `afilter_by_type(node_type)` | 按节点类型过滤 |
| `aget_datasheets()` / `aget_folders()` / `aget_forms()` | 获取指定类型节点 |
| `acreate_embed_link()` / `aget_embed_links()` / `adelete_embed_link()` | 嵌入链接管理 |

## 数据表

### `DatasheetManager`

| 方法 | 说明 |
| --- | --- |
| `get(dst_id_or_url, field_key="name", field_key_map=None)` | 获取数据表实例 |
| `acreate(name, description=None, folder_id=None, pre_filled_records=None)` | 创建数据表 |
| `aexists(dst_id_or_url)` | 判断数据表是否存在 |
| `alist()` | 列出空间下的数据表节点 |
| `aget_datasheet_info(dst_id_or_url)` | 聚合数据表元信息 |

### `Datasheet`

| 属性/方法 | 说明 |
| --- | --- |
| `records` | 记录管理器 |
| `fields` | 字段管理器 |
| `views` | 视图管理器 |
| `attachments` | 附件管理器 |
| `aget_fields()` / `aget_field()` | 获取字段 |
| `aget_views()` / `aget_view()` | 获取视图 |
| `aget_meta()` | 获取字段与视图聚合元信息 |
| `arefresh()` | 清理字段、视图和元信息缓存 |
| `aupload_file(file_path)` | 上传附件 |

## 记录与查询

### `RecordManager`

| 方法 | 说明 |
| --- | --- |
| `all()` | 返回 `QuerySet` |
| `filter(...)` | 构造过滤查询 |
| `order_by(*fields)` | 排序，字段名前加 `-` 表示降序 |
| `fields(*field_names)` | 限定返回字段 |
| `view(view_id)` | 限定视图 |
| `aget(record_id=None, **kwargs)` | 获取单条记录 |
| `acreate(records)` / `aupdate(records)` / `adelete(records)` | 创建、更新、删除记录 |
| `abulk_create()` / `abulk_update()` / `abulk_delete()` | 批量操作别名 |

### `QuerySet`

| 方法 | 说明 |
| --- | --- |
| `filter(formula=None, fields=None, page_size=None, page_token=None, view_id=None, max_records=None)` | 设置查询条件 |
| `filter_by_formula(formula)` | 按 Vika 公式过滤 |
| `order_by(*fields)` / `sort(sort_config)` | 排序 |
| `fields(*field_names)` | 限定字段 |
| `view(view_id)` | 限定视图 |
| `limit(max_records)` | 限制返回数量 |
| `page_size(size)` / `page_num(page_number)` | 分页参数 |
| `filter_by_ids(record_ids)` | 按记录 ID 查询 |
| `aall(max_count=None)` | 自动分页读取多条记录，并尊重 limit |
| `afirst()` / `alast()` / `acount()` / `aexists()` / `aget(**kwargs)` | 常用读取方法 |

示例：

```python
records = await datasheet.records.all() \
    .filter(formula="{状态} = '已完成'") \
    .fields("标题", "状态") \
    .order_by("-创建时间") \
    .limit(100) \
    .aall()
```

### `Record`

| 属性/方法 | 说明 |
| --- | --- |
| `record_id` / `id` | 记录 ID |
| `fields` | 字段值字典 |
| `raw_data` | 原始响应数据 |
| `get(field_name)` | 按字段名取值并做部分类型解析 |
| `set_field(field_name, value)` / `update_fields(fields)` | 修改本地字段值 |
| `to_dict(include_meta=True)` | 转换为字典 |
| `asave()` / `adelete()` | 异步保存或删除 |
| `save()` / `delete()` | 同步兼容包装；已有事件循环中会要求改用异步方法 |

## 字段、视图和附件

### `FieldManager`

| 方法 | 说明 |
| --- | --- |
| `aall()` | 获取字段列表，带异步 TTL 缓存 |
| `aget(field_name_or_id)` | 按名称或 ID 获取字段 |
| `aget_primary_field()` | 获取主字段 |
| `afilter_by_type(field_type)` | 按字段类型过滤 |
| `acreate(field_type, name, property=None)` | 创建字段 |
| `adelete(field_name_or_id)` | 删除字段 |
| `aget_field_mapping()` / `aget_id_mapping()` | 字段名和字段 ID 映射 |

### `ViewManager`

| 方法 | 说明 |
| --- | --- |
| `aall()` | 获取视图列表，带异步 TTL 缓存 |
| `aget(view_name_or_id)` | 按名称或 ID 获取视图 |
| `aget_default_view()` | 获取默认视图 |
| `afilter_by_type(view_type)` | 按视图类型过滤 |
| `aget_grid_views()` / `aget_gallery_views()` / `aget_kanban_views()` | 常用视图类型过滤 |

### `AttachmentManager`

| 方法 | 说明 |
| --- | --- |
| `aupload(file_path)` / `aupload_file(file_path)` | 上传附件 |
| `adownload(attachment, save_path=None)` | 下载附件 |
| `adownload_from_url(url, save_path=None)` | 按 URL 下载附件 |

## 组织单元

`Member`、`Role`、`Team` 提供成员、角色和团队 API 封装。相关请求模型位于 `astral_vika.types.unit_model`。

已实现的常用能力包括成员获取/创建/更新/删除、角色列表/创建/更新/删除、团队列表/查找/创建/更新/删除、团队成员/子团队读取，以及通过成员 `teams` 列表维护团队成员关系。

`DatasheetManager` 不暴露数据表删除方法；当前公开 Fusion API 未提供可验证的数据表删除端点，避免用节点或临时结构伪造能力。

## 异常

请求层会优先解析 Vika JSON 错误体，再映射为结构化异常。

| 异常 | 说明 |
| --- | --- |
| `VikaException` / `ApiException` / `APIException` | 基础异常 |
| `AuthException` | token 或认证失败 |
| `ParameterException` | 参数或请求体错误 |
| `PermissionException` | 权限不足 |
| `RateLimitException` | HTTP 429 或限流错误 |
| `ServerException` | 服务端错误 |
| `AttachmentException` | 附件上传或下载错误 |
| `DatasheetNotFoundException` / `FieldNotFoundException` / `RecordNotFoundException` | 资源不存在 |

## MCP 可选集成

`vika_mcp` 不属于 `astral_vika` 默认运行时依赖。默认测试会在缺少 `vika_mcp` 时跳过 MCP 用例；若后续需要 MCP 服务，应作为独立源码包或可选依赖单独接入。

当前 MCP 工具层只负责可靠暴露 Vika 能力，不负责自然语言语义解析、任务规划或文档写入。

### MCP 配置

| 配置 | 默认值 | 说明 |
| --- | --- | --- |
| `VIKA_API_TOKEN` | - | Vika API Token |
| `VIKA_API_BASE` | `https://vika.cn` | Vika API 基础地址 |
| `VIKAMCP_CACHE__ENABLED` | `true` | 是否启用 catalog 缓存 |
| `VIKAMCP_CACHE__DB_PATH` | 用户缓存目录 | SQLite 缓存文件路径；测试可用 `:memory:` |
| `VIKAMCP_CACHE__TTL_HOURS` | `24` | 缓存 TTL 小时数 |

### MCP 工具分组

| 工具 | 说明 |
| --- | --- |
| `vika.status` | 返回配置状态，不做真实网络请求 |
| `vika.healthcheck` | 真实请求 Vika，返回 `reachable` 和错误信息 |
| `vika.spaces.list` | 列出空间，支持缓存 |
| `vika.nodes.list/search/tree/get` | 列出、搜索、树形展示和读取节点 |
| `vika.nodes.embedlinks.list/create/delete` | 节点嵌入链接管理 |
| `vika.catalog.refresh/status/search/get/clear` | SQLite catalog 缓存刷新、检索和清理 |
| `vika.schema.get` | 获取字段和视图 schema，优先读缓存 |
| `vika.records.query/read_all/get/create/update/delete` | 记录分页查询、受限批量读取和 CRUD |
| `vika.fields.get/create/delete` | 字段读取和变更；字段列表请使用 `vika.schema.get` |
| `vika.views.get` | 视图读取；视图列表请使用 `vika.schema.get` |
| `vika.attachments.upload/download` | 附件上传和下载 |
| `vika.datasheets.create` | 数据表创建；表结构请使用 `vika.schema.get` |

精简后的工具面不再注册 `vika.get_records`、`vika.list_datasheets`、`vika.datasheets.info`、`vika.fields.list`、`vika.views.list`。记录读取统一使用 `vika.records.query` / `vika.records.read_all`，数据表发现统一使用 `vika.nodes.search` 或 `vika.catalog.search`，表结构读取统一使用 `vika.schema.get`。

### 写操作安全

以下写类工具默认只返回预览，不会真实调用 Vika：

`records.create/update/delete`、`fields.create/delete`、`datasheets.create`、`attachments.upload`、`nodes.embedlinks.create/delete`。

默认或未确认时会返回明确的未执行结果：

```json
{
  "executed": false,
  "preview_only": true,
  "requires_confirmation": true,
  "confirmation_fields": {
    "dry_run": false,
    "confirm": true
  },
  "operation": "records.update",
  "target": {},
  "payload_preview": {},
  "message": "Preview only. No Vika write operation was executed."
}
```

`confirm=true` 不是 MCP 向用户发起确认，而是调用方声明已经完成上层确认。真实执行必须同时传入：

```json
{
  "dry_run": false,
  "confirm": true
}
```

真实执行成功时返回：

```json
{
  "executed": true,
  "preview_only": false,
  "requires_confirmation": false,
  "operation": "records.update",
  "result": {}
}
```
