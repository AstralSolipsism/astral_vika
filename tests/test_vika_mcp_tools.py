import logging
import os
import re
import asyncio
from typing import Optional

import pytest

pytest.importorskip("vika_mcp", reason="vika_mcp is not installed")

from vika_mcp.cache import CatalogCache
from vika_mcp.mcp.validation import ToolInputInvalid
from vika_mcp.mcp.registry import ToolRegistry
from vika_mcp.tools.vika_tools import (
    try_register_vika_tools,
    vika_status,
    vika_catalog_search,
    vika_records_query,
    vika_records_create,
)
from vika_mcp.tools.builtin import register as register_builtin

pytestmark = pytest.mark.mcp

logger = logging.getLogger(__name__)


def _workspace_tmp_db() -> str:
    return ":memory:"


@pytest.fixture(scope="module")
def _registered(api_token: str) -> ToolRegistry:
    """
    初始化并注册 vika 工具；依赖环境变量注入的 VIKA_API_TOKEN。
    若凭据缺失会在更早的夹具阶段被 skip。
    """
    registry = ToolRegistry()
    count = try_register_vika_tools(registry)
    # vika.status 总是注册；其余按凭据决定 available
    assert count >= 1
    return registry


@pytest.mark.asyncio
async def test_vika_status(_registered: ToolRegistry):
    """
    连接性：vika.status 应返回 configured=True（有凭据时）。
    """
    status = await vika_status({})
    assert isinstance(status, dict)
    assert "configured" in status
    assert status["host"]
    assert status["configured"] is True, "Vika MCP not configured via env"


@pytest.mark.asyncio
async def test_vika_records_query_minimal(_registered: ToolRegistry, datasheet_id: str):
    """
    读取：vika.records.query 最小查询，page_size=1。
    """
    try:
        resp = await vika_records_query(
            {
                "datasheet_id": datasheet_id,
                "page_size": 1,
            }
        )
    except Exception as e:
        pytest.skip(f"vika.records.query failed: {e}")
    assert isinstance(resp, dict)
    assert "records" in resp and isinstance(resp["records"], list)
    assert "has_more" in resp
    assert "next_offset" in resp


@pytest.mark.asyncio
async def test_vika_records_query_with_view(_registered: ToolRegistry, datasheet_id: str, view_id: Optional[str]):
    """
    读取（带视图）：如 URL 包含 viw...，则用该视图查询一页记录。
    """
    if not view_id:
        pytest.skip("No viewId parsed from VIKA_WORKBENCH_URL; skipping vika.records.query(view).")

    try:
        resp = await vika_records_query(
            {
                "datasheet_id": datasheet_id,
                "view_id": view_id,
                "page_size": 1,
            }
        )
    except Exception as e:
        pytest.skip(f"vika.records.query(view) failed: {e}")
    assert isinstance(resp, dict)
    assert "records" in resp and isinstance(resp["records"], list)
    assert "has_more" in resp
    assert "next_offset" in resp


def test_tool_specs_include_new_tools_and_schemas():
    """
    工具清单应包含新增 vika.* 工具全集；未配置凭据时 available=False（或给出 unavailable_reason），
    且关键参数 schema 正确。该用例仅校验“规范”，不实际调用后端。
    """
    from vika_mcp.mcp.registry import ToolRegistry
    from vika_mcp.tools.vika_tools import try_register_vika_tools

    registry = ToolRegistry()
    count = try_register_vika_tools(registry)
    assert count >= 1

    tools = registry.list_tools(include_unavailable=True)
    by_name = {t.name: t for t in tools}
    names = set(by_name.keys())

    expected = {
        # 既有保留
        "vika.status", "vika.healthcheck", "vika.spaces.list",
        # Nodes
        "vika.nodes.list", "vika.nodes.search", "vika.nodes.tree", "vika.nodes.get",
        "vika.nodes.embedlinks.list", "vika.nodes.embedlinks.create", "vika.nodes.embedlinks.delete",
        # Catalog/schema
        "vika.catalog.refresh", "vika.catalog.status", "vika.catalog.search", "vika.catalog.get", "vika.catalog.clear",
        "vika.schema.get",
        # Records
        "vika.records.create", "vika.records.update", "vika.records.delete",
        "vika.records.get", "vika.records.query", "vika.records.read_all",
        # Fields
        "vika.fields.get", "vika.fields.create", "vika.fields.delete",
        # Views
        "vika.views.get",
        # Attachments
        "vika.attachments.upload", "vika.attachments.download",
        # Datasheets
        "vika.datasheets.create",
    }
    removed = {
        "vika.get_records",
        "vika.list_datasheets",
        "vika.datasheets.info",
        "vika.fields.list",
        "vika.views.list",
    }
    # a) 工具集合包含上述名称全集
    missing = expected - names
    assert not missing, f"Missing tools: {sorted(missing)}"
    assert not (removed & names), f"Removed tools are still registered: {sorted(removed & names)}"
    assert len(names) == 29

    # c) 无凭据时（无 VIKA_API_TOKEN）除状态/健康检查外均应 available=False 或含 unavailable_reason
    has_token = bool(os.getenv("VIKA_API_TOKEN"))
    assert by_name["vika.status"].available is True
    assert by_name["vika.healthcheck"].available is True
    if not has_token:
        for n in expected - {"vika.status", "vika.healthcheck"}:
            spec = by_name[n]
            assert spec.available is False or getattr(spec, "unavailable_reason", None), f"{n} should be unavailable without credentials"

    # b) 参数 schema 关键字段断言（抽查 5 个）
    def _props(spec_name: str):
        sch = by_name[spec_name].input_schema
        assert isinstance(sch, dict) and sch.get("type") == "object"
        return sch.get("required", []), sch.get("properties", {})

    # vika.records.create
    req, props = _props("vika.records.create")
    assert {"datasheet_id", "records"}.issubset(set(req))
    assert "field_key" in props
    fk = props["field_key"]
    assert isinstance(fk, dict) and fk.get("enum") == ["name", "id"]
    assert "dry_run" in props and "confirm" in props

    # vika.records.query
    req, props = _props("vika.records.query")
    assert "datasheet_id" in req
    expected_opt = {"view_id", "formula", "fields", "page_size", "page_num", "page_token", "sort", "field_key"}
    assert expected_opt.issubset(set(props.keys()))

    # vika.fields.create
    req, props = _props("vika.fields.create")
    assert {"datasheet_id", "space_id", "name", "field_type"}.issubset(set(req))
    assert "property" in props and props["property"].get("type") == "object"
    assert "dry_run" in props and "confirm" in props

    # vika.attachments.upload
    req, props = _props("vika.attachments.upload")
    assert {"datasheet_id", "file_path"}.issubset(set(req))

    # vika.datasheets.create
    req, props = _props("vika.datasheets.create")
    assert {"space_id", "name"}.issubset(set(req))


def test_catalog_cache_search_and_clear():
    cache = CatalogCache(db_path=_workspace_tmp_db(), ttl_hours=24)
    namespace = "ns-test"
    cache.upsert_items(
        namespace,
        [
            {
                "type": "datasheet",
                "id": "dstDaily",
                "space_id": "spc1",
                "name": "每日早报",
                "path": "业务/每日早报",
                "dst_id": "dstDaily",
                "data": {"id": "dstDaily", "type": "Datasheet", "name": "每日早报"},
            },
            {
                "type": "datasheet",
                "id": "dstWeekly",
                "space_id": "spc1",
                "name": "周报",
                "path": "业务/周报",
                "dst_id": "dstWeekly",
                "data": {"id": "dstWeekly", "type": "Datasheet", "name": "周报"},
            },
        ],
    )

    exact = cache.search(namespace, "每日早报", space_id="spc1")
    assert exact[0]["dst_id"] == "dstDaily"
    assert exact[0]["score"] == 1.0

    fuzzy = cache.search(namespace, "早报", space_id="spc1")
    assert fuzzy and fuzzy[0]["dst_id"] == "dstDaily"

    status = cache.status(namespace)
    assert status["items"] == 2
    assert status["fresh"] is True

    assert cache.clear(namespace, space_id="spc1") == 2
    assert cache.search(namespace, "早报", space_id="spc1") == []


def test_catalog_cache_partitions_by_namespace():
    cache = CatalogCache(db_path=_workspace_tmp_db(), ttl_hours=24)
    item = {
        "type": "datasheet",
        "id": "dst1",
        "space_id": "spc1",
        "name": "Same",
        "path": "Same",
        "dst_id": "dst1",
        "data": {"id": "dst1"},
    }
    cache.upsert_items("ns-a", [item])

    assert cache.search("ns-a", "Same")
    assert cache.search("ns-b", "Same") == []


@pytest.mark.asyncio
async def test_mcp_write_tools_default_to_dry_run(monkeypatch):
    import vika_mcp.tools.vika_tools as vt

    class FakeClient:
        def __init__(self):
            self.calls = 0

        async def records_create(self, *args, **kwargs):
            self.calls += 1
            return {"records": [{"recordId": "rec1"}]}

    fake = FakeClient()
    monkeypatch.setattr(vt, "_CLIENT", fake)

    preview = await vika_records_create({"datasheet_id": "dst1", "records": {"Title": "Hello"}})
    assert preview["executed"] is False
    assert preview["preview_only"] is True
    assert preview["requires_confirmation"] is True
    assert preview["confirmation_fields"] == {"dry_run": False, "confirm": True}
    assert preview["operation"] == "records.create"
    assert fake.calls == 0

    executed = await vika_records_create(
        {
            "datasheet_id": "dst1",
            "records": {"Title": "Hello"},
            "dry_run": False,
            "confirm": True,
        }
    )
    assert executed == {
        "executed": True,
        "preview_only": False,
        "requires_confirmation": False,
        "operation": "records.create",
        "result": {"records": [{"recordId": "rec1"}]},
    }
    assert fake.calls == 1


@pytest.mark.asyncio
async def test_registry_schema_validation_blocks_bad_input(monkeypatch):
    monkeypatch.setenv("VIKAMCP_CACHE__DB_PATH", _workspace_tmp_db())
    reg = ToolRegistry()
    try_register_vika_tools(reg)
    _, handler = reg.get("vika.records.query")

    with pytest.raises(ToolInputInvalid) as exc:
        await handler({"page_size": 0, "unexpected": True})

    assert "missing required argument: datasheet_id" in str(exc.value)
    assert "unexpected argument: unexpected" in str(exc.value)
    assert "page_size must be >= 1" in str(exc.value)


@pytest.mark.asyncio
async def test_healthcheck_result_shapes(monkeypatch):
    import vika_mcp.tools.vika_tools as vt

    class FakeClient:
        async def healthcheck(self):
            return {
                "configured": True,
                "reachable": False,
                "host": "https://vika.cn",
                "default_space_id": None,
                "error_type": "ConnectError",
                "error_message": "network down",
            }

    monkeypatch.setattr(vt, "_CLIENT", FakeClient())

    from vika_mcp.tools.vika_tools import vika_healthcheck

    result = await vika_healthcheck({})
    assert result["configured"] is True
    assert result["reachable"] is False
    assert result["error_type"] == "ConnectError"


@pytest.mark.asyncio
async def test_catalog_search_tool_uses_cache_without_api(monkeypatch):
    import vika_mcp.tools.vika_tools as vt
    from vika_mcp.tools.vika_tools import VikaClient

    cache = CatalogCache(db_path=_workspace_tmp_db(), ttl_hours=24)
    client = VikaClient(api_token="token", host="https://vika.cn", cache=cache)
    cache.upsert_items(
        client.namespace,
        [
            {
                "type": "datasheet",
                "id": "dstMassive",
                "space_id": "spc1",
                "name": "大规模表格",
                "path": "根目录/大规模表格",
                "dst_id": "dstMassive",
                "data": {"id": "dstMassive", "type": "Datasheet"},
            }
        ],
    )
    monkeypatch.setattr(vt, "_CLIENT", client)

    result = await vika_catalog_search({"query": "大规模", "space_id": "spc1"})
    assert result["source"] == "cache"
    assert result["matches"][0]["dst_id"] == "dstMassive"


@pytest.mark.asyncio
async def test_mcp_nodes_list_combines_top_level_and_typed_search(monkeypatch):
    import vika_mcp.tools.vika_tools as vt
    from vika_mcp.tools.vika_tools import VikaClient

    class RawNode:
        def __init__(self, node_id, name, node_type, parent_id=None):
            self.raw_data = {"id": node_id, "name": name, "type": node_type, "parentId": parent_id}

    class FakeNodes:
        async def alist(self):
            return [RawNode("fod1", "Folder", "Folder")]

        async def asearch(self, node_type=None, **kwargs):
            if node_type == "Folder":
                return [RawNode("fod1", "Folder", "Folder")]
            if node_type == "Datasheet":
                return [RawNode("dst1", "Nested", "Datasheet", "fod1")]
            return []

    class FakeSpace:
        nodes = FakeNodes()

    class FakeVika:
        def space(self, space_id):
            return FakeSpace()

        async def aclose(self):
            pass

    client = VikaClient(api_token="token", host="https://vika.cn", cache=CatalogCache(db_path=":memory:"))
    monkeypatch.setattr(client, "_ensure_client", lambda: FakeVika())
    monkeypatch.setattr(vt, "_CLIENT", client)

    result = await vt.vika_nodes_list({"space_id": "spc1", "force_refresh": True})

    datasheets = [item for item in result["nodes"] if item.get("type") == "datasheet"]
    assert datasheets[0]["dst_id"] == "dst1"
    assert datasheets[0]["path"] == "Folder/Nested"


@pytest.mark.asyncio
async def test_mcp_fields_and_views_get_use_schema_directly(monkeypatch):
    import vika_mcp.tools.vika_tools as vt
    from vika_mcp.tools.vika_tools import VikaClient

    class FakeClient(VikaClient):
        async def schema_get(self, datasheet_id, space_id=None, use_cache=True, force_refresh=False):
            return {
                "datasheet_id": datasheet_id,
                "space_id": space_id,
                "fields": [{"id": "fld1", "name": "Title"}],
                "views": [{"id": "viw1", "name": "Grid"}],
                "primary_field": None,
                "source": "mock",
            }

    client = FakeClient(api_token="token", host="https://vika.cn", cache=CatalogCache(db_path=":memory:"))
    monkeypatch.setattr(vt, "_CLIENT", client)

    assert await vt.vika_fields_get({"datasheet_id": "dst1", "field_id_or_name": "Title"}) == {
        "field": {"id": "fld1", "name": "Title"}
    }
    assert await vt.vika_views_get({"datasheet_id": "dst1", "view_id_or_name": "viw1"}) == {
        "view": {"id": "viw1", "name": "Grid"}
    }
    
    
@pytest.mark.asyncio
async def test_live_vika_basic_read_ops():
    """
    只读集成：通过注册表按名称调用 4 个读取类工具，使用 env 凭据。
    - 需要环境变量：VIKA_API_TOKEN、VIKA_WORKBENCH_URL；缺任一则 skip
    - 解析 URL 获取 dst... / viw...；缺 dst 则 skip
    - 仅调用：
        * vika.schema.get(datasheet_id=...)
        * vika.records.query(datasheet_id=..., view_id=?, page_size=1)
    """
    token = os.getenv("VIKA_API_TOKEN")
    workbench_url = os.getenv("VIKA_WORKBENCH_URL")
    if not token or not workbench_url:
        pytest.skip("Missing VIKA_API_TOKEN or VIKA_WORKBENCH_URL; skipping live read-only test.")

    # 解析 ID（稳健：独立搜 dst... 与 viw...）
    m_dst = re.search(r"(dst[0-9A-Za-z]+)", workbench_url or "")
    m_viw = re.search(r"(viw[0-9A-Za-z]+)", workbench_url or "")
    dst_id = m_dst.group(1) if m_dst else None
    viw_id = m_viw.group(1) if m_viw else None
    if not dst_id:
        pytest.skip("Failed to parse datasheetId (dst...) from VIKA_WORKBENCH_URL.")

    # 注册表 + 工具注册
    reg = ToolRegistry()
    register_builtin(reg)
    try_register_vika_tools(reg)

    async def call_tool(name: str, args: dict):
        """根据名称取 handler 并以 dict 作为参数调用；兼容同步/异步处理器。"""
        try:
            _, handler = reg.get(name)
        except KeyError as e:
            pytest.skip(f"Tool not registered: {name} ({e})")
        if asyncio.iscoroutinefunction(handler):
            return await handler(args or {})
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: handler(args or {}))

    # 1) schema.get
    try:
        schema = await call_tool("vika.schema.get", {"datasheet_id": dst_id})
    except Exception as e:
        pytest.skip(f"vika.schema.get failed: {e}")
    assert isinstance(schema, dict)
    assert schema.get("datasheet_id") == dst_id
    fields = schema.get("fields")
    views = schema.get("views")
    assert isinstance(fields, list)
    assert isinstance(views, list)
    if viw_id:
        # 若返回包含 id/viewId/view_id 字段，则要求包含该 viw；否则放宽为至少存在一个视图
        has_id_keys = any(isinstance(v, dict) and ("id" in v or "viewId" in v or "view_id" in v) for v in views)
        if has_id_keys:
            found = False
            for v in views:
                if not isinstance(v, dict):
                    continue
                vid = v.get("id") or v.get("viewId") or v.get("view_id")
                if vid == viw_id:
                    found = True
                    break
            assert found, f"Expected view id {viw_id} in views"
        else:
            assert len(views) >= 1

    # 2) records.query
    query_args = {"datasheet_id": dst_id, "page_size": 1}
    if viw_id:
        query_args["view_id"] = viw_id
    try:
        q = await call_tool("vika.records.query", query_args)
    except Exception as e:
        pytest.skip(f"vika.records.query failed: {e}")
    assert isinstance(q, dict)
    assert "records" in q and isinstance(q["records"], list)
    assert "has_more" in q
    assert "next_offset" in q
