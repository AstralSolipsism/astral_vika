"""
维格表API响应类型定义

兼容原vika.py库的响应类型
"""
from typing import Optional, Dict, Any, List, Union
from pydantic import BaseModel, Field
from datetime import datetime


class APIResponse(BaseModel):
    """API响应基类"""
    success: bool
    code: int
    message: str
    data: Optional[Dict[str, Any]] = None


class RecordData(BaseModel):
    """记录数据模型"""
    recordId: Optional[str] = None
    fields: Dict[str, Any]
    createdAt: Optional[str] = None
    updatedAt: Optional[str] = None


class FieldData(BaseModel):
    """字段数据模型"""
    id: str
    name: str
    type: str
    property: Optional[Dict[str, Any]] = None
    editable: Optional[bool] = None
    isPrimary: Optional[bool] = None


class ViewData(BaseModel):
    """视图数据模型"""
    id: str
    name: str
    type: str
    property: Optional[Dict[str, Any]] = None


class AttachmentData(BaseModel):
    """附件数据模型"""
    token: str
    name: str
    size: int
    mimeType: str
    url: str
    width: Optional[int] = None
    height: Optional[int] = None


class NodeData(BaseModel):
    """节点数据模型"""
    id: str
    name: str
    type: str
    icon: Optional[str] = None
    children: Optional[List['NodeData']] = None
    parentId: Optional[str] = None


class SpaceData(BaseModel):
    """空间数据模型"""
    id: str
    name: str
    description: Optional[str] = None
    logo: Optional[str] = None


class RecordsResponse(APIResponse):
    """记录响应模型"""
    data: Optional[Dict[str, Any]] = Field(None, description="包含records和pageToken的数据")


class FieldsResponse(APIResponse):
    """字段响应模型"""
    data: Optional[Dict[str, Any]] = Field(None, description="包含fields的数据")


class ViewsResponse(APIResponse):
    """视图响应模型"""
    data: Optional[Dict[str, Any]] = Field(None, description="包含views的数据")


class DatasheetResponse(APIResponse):
    """数据表响应模型"""
    data: Optional[Dict[str, Any]] = Field(None, description="包含datasheet信息的数据")


class SpaceResponse(APIResponse):
    """空间响应模型"""
    data: Optional[Dict[str, Any]] = Field(None, description="包含spaces的数据")


class NodeResponse(APIResponse):
    """节点响应模型"""
    data: Optional[Dict[str, Any]] = Field(None, description="包含nodes的数据")


class AttachmentResponse(APIResponse):
    """附件响应模型"""
    data: Optional[Dict[str, Any]] = Field(None, description="包含attachment信息的数据")


class PostDatasheetMetaResponse(APIResponse):
    """创建数据表元数据响应（与原库兼容）"""
    data: Optional[Dict[str, Any]] = Field(None, description="数据表元数据")


class PaginationInfo(BaseModel):
    """分页信息"""
    pageToken: Optional[str] = None
    total: Optional[int] = None


class QueryResult(BaseModel):
    """查询结果模型"""
    records: List[RecordData]
    pagination: Optional[PaginationInfo] = None


# 为了与原库完全兼容，创建一些别名
PostDatasheetMeta = PostDatasheetMetaResponse


__all__ = [
    'APIResponse',
    'RecordData',
    'FieldData', 
    'ViewData',
    'AttachmentData',
    'NodeData',
    'SpaceData',
    'RecordsResponse',
    'FieldsResponse',
    'ViewsResponse',
    'DatasheetResponse',
    'SpaceResponse',
    'NodeResponse',
    'AttachmentResponse',
    'PostDatasheetMetaResponse',
    'PostDatasheetMeta',
    'PaginationInfo',
    'QueryResult'
]
