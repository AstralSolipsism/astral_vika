from __future__ import annotations

import re
from pathlib import Path

import pytest
import httpx

import astral_vika
from astral_vika.datasheet import query_set as query_set_module
from astral_vika.datasheet.datasheet_manager import DatasheetManager
from astral_vika.datasheet.query_set import QuerySet
from astral_vika.datasheet.record import Record, RecordBuilder
from astral_vika.exceptions import NotFoundException, RateLimitException, VikaException
from astral_vika.node.node_manager import NodeManager
from astral_vika.request import Session, _serialize_params
from astral_vika.space.space import Space
from astral_vika.unit.team import Team
from astral_vika.utils import timed_lru_cache


def test_package_version_matches_project_metadata():
    pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"
    match = re.search(r'^version = "([^"]+)"', pyproject.read_text(encoding="utf-8"), re.MULTILINE)

    assert match is not None
    assert astral_vika.__version__ == match.group(1)


def test_top_level_star_import_exposes_field_data():
    namespace = {}

    exec("from astral_vika import *", namespace)

    assert "FieldData" in namespace
    assert namespace["FieldData"] is astral_vika.FieldData


def test_request_param_serialization_for_vika_query_shapes():
    params = _serialize_params(
        {
            "fields": ["Title", "Status"],
            "recordIds": ["recA", "recB"],
            "sort": [
                {"field": "Priority", "order": "desc"},
                {"field": "Title", "order": "asc"},
            ],
        }
    )

    assert params == [
        ("fields", "Title"),
        ("fields", "Status"),
        ("recordIds", "recA"),
        ("recordIds", "recB"),
        ("sort[0][field]", "Priority"),
        ("sort[0][order]", "desc"),
        ("sort[1][field]", "Title"),
        ("sort[1][order]", "asc"),
    ]


@pytest.mark.asyncio
async def test_request_maps_http_429_to_rate_limit_exception():
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            429,
            json={"success": False, "code": 429, "message": "too many requests"},
        )

    session = Session("token")
    session.rate_limit_retries = 0
    await session.client.aclose()
    session.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:
        with pytest.raises(RateLimitException):
            await session.get("spaces")
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_request_retries_http_429(monkeypatch):
    sleeps = []
    calls = 0

    async def fake_sleep(delay):
        sleeps.append(delay)

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(
                429,
                json={"success": False, "code": 429, "message": "too many requests"},
            )
        return httpx.Response(200, json={"success": True, "data": {"spaces": []}})

    monkeypatch.setattr("astral_vika.request.asyncio.sleep", fake_sleep)
    session = Session("token")
    session.rate_limit_retries = 1
    await session.client.aclose()
    session.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:
        assert await session.get("spaces") == {"success": True, "data": {"spaces": []}}
        assert calls == 2
        assert sleeps == [0.6]
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_timed_lru_cache_supports_async_functions():
    calls = 0

    @timed_lru_cache(seconds=300)
    async def load_value(value):
        nonlocal calls
        calls += 1
        return {"value": value, "calls": calls}

    first = await load_value("alpha")
    second = await load_value("alpha")

    assert first == second
    assert calls == 1

    load_value.cache_clear()
    third = await load_value("alpha")

    assert third == {"value": "alpha", "calls": 2}
    assert calls == 2


class _FakeRecordStore:
    def __init__(self):
        self.created = None
        self.updated = None
        self.deleted = None

    async def acreate(self, records):
        self.created = records
        return ["created"]

    async def aupdate(self, records):
        self.updated = records
        return ["updated"]

    async def adelete(self, records):
        self.deleted = records
        return True


class _FakeRecordDatasheet:
    def __init__(self, records):
        self.records = records


@pytest.mark.asyncio
async def test_record_async_save_and_delete_use_async_manager_methods():
    manager = _FakeRecordStore()
    datasheet = _FakeRecordDatasheet(manager)

    new_record = Record({"fields": {"Name": "new"}}, datasheet)
    existing_record = Record({"recordId": "rec1", "fields": {"Name": "old"}}, datasheet)

    assert await new_record.asave() == ["created"]
    assert manager.created == [{"Name": "new"}]

    assert await existing_record.asave() == ["updated"]
    assert manager.updated == [{"recordId": "rec1", "fields": {"Name": "old"}}]

    assert await existing_record.adelete() is True
    assert manager.deleted == ["rec1"]


def test_record_sync_wrappers_run_without_existing_event_loop():
    manager = _FakeRecordStore()
    datasheet = _FakeRecordDatasheet(manager)

    assert Record({"fields": {"Name": "new"}}, datasheet).save() == ["created"]
    assert manager.created == [{"Name": "new"}]

    assert Record({"recordId": "rec1", "fields": {}}, datasheet).delete() is True
    assert manager.deleted == ["rec1"]

    builder = RecordBuilder(datasheet).set_field("Name", "built")
    assert builder.save() == ["created"]
    assert manager.created == [{"Name": "built"}]


@pytest.mark.asyncio
async def test_record_sync_wrappers_reject_running_event_loop():
    manager = _FakeRecordStore()
    datasheet = _FakeRecordDatasheet(manager)

    with pytest.raises(RuntimeError, match="await record.asave"):
        Record({"fields": {"Name": "new"}}, datasheet).save()

    with pytest.raises(RuntimeError, match="await record.adelete"):
        Record({"recordId": "rec1", "fields": {}}, datasheet).delete()

    with pytest.raises(RuntimeError, match="await builder.asave"):
        RecordBuilder(datasheet).save()


class _FakeRecordManager:
    def __init__(self):
        self.calls = []

    async def _aget_records(self, **kwargs):
        self.calls.append(kwargs)
        page_num = kwargs.get("page_num") or 1
        return {
            "success": True,
            "data": {
                "total": 4,
                "records": [
                    {
                        "recordId": f"rec{page_num}",
                        "fields": {"Name": f"row-{page_num}"},
                    }
                ],
            },
        }


class _FakeDatasheet:
    def __init__(self, records):
        self.records = records


def test_queryset_inherits_datasheet_field_key():
    manager = _FakeRecordManager()
    datasheet = _FakeDatasheet(manager)
    datasheet._field_key = "id"

    assert QuerySet(datasheet)._field_key == "id"


@pytest.mark.asyncio
async def test_queryset_aall_respects_limit():
    manager = _FakeRecordManager()
    records = await QuerySet(_FakeDatasheet(manager)).limit(1).aall()

    assert len(records) == 1
    assert records[0].record_id == "rec1"
    assert len(manager.calls) == 1
    assert manager.calls[0]["max_records"] == 1
    assert manager.calls[0]["page_size"] == 1


@pytest.mark.asyncio
async def test_queryset_aall_retries_rate_limit(monkeypatch):
    sleeps = []

    async def fake_sleep(delay):
        sleeps.append(delay)

    class FlakyRecordManager(_FakeRecordManager):
        async def _aget_records(self, **kwargs):
            self.calls.append(kwargs)
            if len(self.calls) == 1:
                raise RateLimitException("too many requests", code=429)
            return {
                "success": True,
                "data": {
                    "total": 1,
                    "records": [{"recordId": "rec1", "fields": {}}],
                },
            }

    monkeypatch.setattr(query_set_module.asyncio, "sleep", fake_sleep)
    manager = FlakyRecordManager()
    records = await QuerySet(_FakeDatasheet(manager)).limit(1).aall()

    assert len(records) == 1
    assert len(manager.calls) == 2
    assert sleeps == [0.5]


@pytest.mark.asyncio
async def test_space_asearch_nodes_passes_query_and_node_type_by_name():
    class RawNode:
        raw_data = {"id": "dst1", "type": "Datasheet"}

    class FakeNodeManager:
        def __init__(self):
            self.kwargs = None

        async def asearch(self, **kwargs):
            self.kwargs = kwargs
            return [RawNode()]

    manager = FakeNodeManager()
    space = Space.__new__(Space)
    space._node_manager = manager

    result = await Space.asearch_nodes(space, query="Tasks", node_type="Datasheet")

    assert result == [{"id": "dst1", "type": "Datasheet"}]
    assert manager.kwargs == {"query": "Tasks", "node_type": "Datasheet"}


@pytest.mark.asyncio
async def test_space_get_space_info_uses_spaces_list():
    class FakeSpaces:
        async def alist(self):
            return [{"id": "spc1", "name": "Main"}, {"id": "spc2", "name": "Other"}]

    class FakeApitable:
        spaces = FakeSpaces()

    assert await Space(FakeApitable(), "spc1").aget_space_info() == {"id": "spc1", "name": "Main"}

    with pytest.raises(NotFoundException):
        await Space(FakeApitable(), "spc3").aget_space_info()


@pytest.mark.asyncio
async def test_team_list_and_find_use_real_team_endpoints():
    class FakeAdapter:
        async def get(self, endpoint, params=None):
            if endpoint == "spaces/spc1/teams/0/children":
                return {"success": True, "data": {"children": [{"unitId": "teamA", "name": "Sales"}]}}
            if endpoint == "spaces/spc1/teams/teamA/children":
                return {"success": True, "data": {"children": []}}
            raise AssertionError(endpoint)

    class FakeApitable:
        request_adapter = FakeAdapter()

    class FakeSpace:
        _space_id = "spc1"
        _apitable = FakeApitable()

    team = Team(FakeSpace())

    assert await team.alist() == [{"unitId": "teamA", "name": "Sales"}]
    assert await team.afind_by_name("Sales") == {"unitId": "teamA", "name": "Sales"}
    assert await team.afind_by_name("Missing") is None


@pytest.mark.asyncio
async def test_team_add_and_remove_member_update_member_team_ids():
    class FakeMember:
        def __init__(self):
            self.teams = [{"unitId": "teamA", "name": "A"}]
            self.updates = []

        async def aget(self, member_id):
            return {"unitId": member_id, "teams": self.teams}

        async def _aupdate_member(self, member_id, data):
            self.updates.append((member_id, data))
            self.teams = data["teams"]
            return {"success": True, "data": {}}

    class FakeSpace:
        member = FakeMember()

    team = Team(FakeSpace())

    assert await team.aadd_member("teamB", "mem1") is True
    assert FakeSpace.member.updates[-1] == ("mem1", {"teams": ["teamA", "teamB"]})

    assert await team.aremove_member("teamA", "mem1") is True
    assert FakeSpace.member.updates[-1] == ("mem1", {"teams": ["teamB"]})


def test_datasheet_manager_does_not_expose_unimplemented_delete():
    assert not hasattr(DatasheetManager, "delete")


@pytest.mark.asyncio
async def test_datasheet_manager_alist_uses_datasheet_node_search():
    class RawNode:
        raw_data = {
            "id": "dstNested",
            "name": "Nested",
            "type": "Datasheet",
            "icon": "table",
            "parentId": "fod1",
        }

    class FakeNodes:
        async def aget_datasheets(self):
            return [RawNode()]

    class FakeSpace:
        nodes = FakeNodes()

    assert await DatasheetManager(FakeSpace()).alist() == [
        {
            "id": "dstNested",
            "name": "Nested",
            "type": "Datasheet",
            "icon": "table",
            "parentId": "fod1",
        }
    ]


@pytest.mark.asyncio
async def test_node_manager_uses_permissions_parameter_name():
    captured = {}
    manager = NodeManager.__new__(NodeManager)

    async def fake_search(params):
        captured.update(params)
        return {"success": True, "data": {"nodes": []}}

    manager._asearch_nodes = fake_search

    await manager.asearch(node_type="Datasheet", permission=0)

    assert captured == {"type": "Datasheet", "permissions": 0}


@pytest.mark.asyncio
async def test_node_manager_get_nodes_propagates_vika_exception():
    class FakeRequestAdapter:
        async def get(self, endpoint):
            raise VikaException("node request failed", code=500)

    class FakeApitable:
        request_adapter = FakeRequestAdapter()

    class FakeSpace:
        _space_id = "spc1"
        _apitable = FakeApitable()

    manager = NodeManager(FakeSpace())

    with pytest.raises(VikaException, match="node request failed"):
        await manager._aget_nodes()


def test_docs_describe_async_sdk_boundary():
    root = Path(__file__).resolve().parents[1]
    readme = (root / "README.md").read_text(encoding="utf-8")
    api_reference = (root / "docs" / "API_REFERENCE.md").read_text(encoding="utf-8")
    combined = readme + "\n" + api_reference

    assert "异步优先" in combined
    assert "Python 3.8+" in combined
    assert "vika_mcp" in combined
    assert "可选" in combined
    assert "完全兼容原" not in combined
    assert "明确未实现边界" not in combined
    assert "NotImplementedError" not in combined
