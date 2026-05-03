from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional, Tuple

import pytest

from astral_vika import Vika
from astral_vika.exceptions import ParameterException
from astral_vika.datasheet.field_manager import Field

pytestmark = pytest.mark.astral

logger = logging.getLogger(__name__)


ALLOWED_SIMPLE_TYPES_PRIORITY = [
    "SingleText",  # 优先选择
    "Text",
    "Number",
]


def pick_writable_simple_field(fields: list[Field]) -> Optional[Field]:
    """
    从字段元数据中自动选择一个可写且简单的字段（优先文本/数字）。
    过滤策略：
      - editable=True
      - 非主字段（isPrimary=False）
      - 尽量排除“必填”字段（properties.required / properties.isRequired / 顶层 isRequired 不为 True）
      - 类型优先：SingleText > Text > Number
    """
    def _not_required(f: Field) -> bool:
        try:
            props = getattr(f, "properties", {}) or {}
            top = getattr(f, "raw_data", {})  # type: ignore[attr-defined]
            # 常见键：required / isRequired（不同版本/返回结构可能差异）
            for k in ("required", "isRequired"):
                if isinstance(props, dict) and props.get(k) is True:
                    return False
                if isinstance(top, dict) and top.get(k) is True:
                    return False
        except Exception:
            # 保守放行（若取不到，则不据此剔除）
            pass
        return True

    # 先按优先类型过滤（保持顺序），且必须 editable、非主字段、非必填
    by_type: dict[str, list[Field]] = {t: [] for t in ALLOWED_SIMPLE_TYPES_PRIORITY}
    for f in fields:
        if not getattr(f, "editable", True):
            continue
        if getattr(f, "is_primary", False):
            continue
        if not _not_required(f):
            continue
        if f.type in by_type:
            by_type[f.type].append(f)

    for t in ALLOWED_SIMPLE_TYPES_PRIORITY:
        if by_type[t]:
            # 避免空名称
            for f in by_type[t]:
                if f.name:
                    return f

    # 兜底：任何满足 editable、非主字段、非必填 的字段
    for f in fields:
        if getattr(f, "editable", True) and not getattr(f, "is_primary", False) and _not_required(f) and f.name:
            return f
    return None


def make_value_for_type(field_type: str, unique_suffix: str) -> Tuple[object, object]:
    """
    为类型生成创建/更新值 (create_value, update_value)。
    """
    if field_type in ("SingleText", "Text"):
        base = f"pytest-{unique_suffix}"
        return base, base + "-updated"
    if field_type == "Number":
        n = int(time.time()) % 1000000
        return n, n + 1
    # 不在可写简单类型内的，返回占位给上层决定 skip
    return None, None  # type: ignore[return-value]


@pytest.mark.asyncio
async def test_connection_and_metadata(api_token: str, datasheet_id: str):
    """
    初始化客户端并连通：获取字段或记录列表以验证可用性。
    """
    vika = Vika(api_token)
    try:
        ds = vika.datasheet(datasheet_id)
        # 读取元信息（字段+视图），或至少读取字段
        fields = await ds.aget_fields()
        assert isinstance(fields, list)
        # 最小记录拉取（通过 QuerySet）
        recs = await ds.records.all().limit(1)._aevaluate()
        assert isinstance(recs, list)
    finally:
        await vika.aclose()


@pytest.mark.asyncio
async def test_record_crud_roundtrip(api_token: str, datasheet_id: str, created_record_ids: list[str]):
    """
    字段发现 -> 创建 -> 查询 -> 更新 -> 删除 的最小闭环验证。
    """
    vika = Vika(api_token)
    try:
        ds = vika.datasheet(datasheet_id)

        # 字段发现（自动选择一个可写、简单类型字段）
        fields = await ds.aget_fields()
        field = pick_writable_simple_field(fields)
        if field is None:
            pytest.skip("No writable simple field available; skipping CRUD tests.")
        # 下述访问仅在非 None 情况下进行
        assert field is not None

        create_val, update_val = make_value_for_type(field.type, unique_suffix=str(int(time.time() * 1000)))
        if create_val is None:
            pytest.skip(f"Field '{field.name}' type '{field.type}' not supported for CRUD test.")

        # 创建
        created = await ds.records.acreate({field.name: create_val})
        assert created and created[0].record_id, "Create failed or recordId missing"
        rid = created[0].record_id  # type: ignore[assignment]
        assert isinstance(rid, str) and rid
        created_record_ids.append(rid)
        logger.info("Created record id=%s on field=%s", rid, field.name)

        # 查询
        fetched = await ds.records.aget(record_id=rid)
        assert fetched.record_id == rid
        # 对比字段值（文本/数字等简单类型）
        assert fetched.fields.get(field.name) == create_val

        # 更新
        updated = await ds.records.aupdate({"recordId": rid, "fields": {field.name: update_val}})
        assert updated and updated[0].record_id == rid
        refetched = await ds.records.aget(record_id=rid)
        assert refetched.fields.get(field.name) == update_val

        # 删除
        ok = await ds.records.adelete(rid)
        assert ok is True
        created_record_ids.remove(rid)
        with pytest.raises(ParameterException):
            await ds.records.aget(record_id=rid)
    finally:
        await vika.aclose()


@pytest.mark.asyncio
async def test_view_access(api_token: str, datasheet_id: str, view_id: Optional[str]):
    """
    视图读取：如提供 viewId 则按视图查询或获取视图定义。
    """
    if not view_id:
        pytest.skip("No viewId parsed from VIKA_WORKBENCH_URL; skipping view tests.")
    # 为类型检查与静态分析明确非 None
    assert view_id is not None

    vika = Vika(api_token)
    try:
        ds = vika.datasheet(datasheet_id)
        view = await ds.views.aget(view_id)
        assert view.id, "View must have id"
        # 按视图读取一页记录
        recs = await ds.records.view(view_id).limit(1)._aevaluate()
        assert isinstance(recs, list)
    finally:
        await vika.aclose()
