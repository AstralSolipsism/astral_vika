"""
维格表API响应类型定义

兼容原vika.py库的响应类型
"""
from typing import Annotated, Any, Dict, List, Literal, Optional, Union
from pydantic import BaseModel
from pydantic import Field as PydanticField
from .field_property import (
    ButtonProperty, CheckboxProperty, CurrencyProperty, DateTimeProperty,
    DefaultValueProperty, EmptyProperty, FormulaProperty, LinkProperty,
    MemberProperty, NumberProperty, PercentProperty, RatingProperty,
    SelectProperty, TwoWayLinkProperty, UserProperty
)


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


class BaseField(BaseModel):
    """字段基础模型"""
    id: str
    name: str
    desc: Optional[str] = None
    editable: Optional[bool] = None
    isPrimary: Optional[bool] = None


# Text Fields
class SingleTextField(BaseField):
    type: Literal["SingleText"]
    property: DefaultValueProperty


class TextField(BaseField):
    type: Literal["Text"]
    property: Optional[EmptyProperty] = None


# Select Fields
class SingleSelectField(BaseField):
    type: Literal["SingleSelect"]
    property: SelectProperty


class MultiSelectField(BaseField):
    type: Literal["MultiSelect"]
    property: SelectProperty


# Number Fields
class NumberField(BaseField):
    type: Literal["Number"]
    property: NumberProperty


class CurrencyField(BaseField):
    type: Literal["Currency"]
    property: CurrencyProperty


class PercentField(BaseField):
    type: Literal["Percent"]
    property: PercentProperty


# DateTime Fields
class DateTimeField(BaseField):
    type: Literal["DateTime"]
    property: DateTimeProperty


class CreatedTimeField(BaseField):
    type: Literal["CreatedTime"]
    property: DateTimeProperty


class LastModifiedTimeField(BaseField):
    type: Literal["LastModifiedTime"]
    property: DateTimeProperty


# Other Fields
class AttachmentField(BaseField):
    type: Literal["Attachment"]
    property: Optional[EmptyProperty] = None


class MemberField(BaseField):
    type: Literal["Member"]
    property: MemberProperty


class CheckboxField(BaseField):
    type: Literal["Checkbox"]
    property: CheckboxProperty


class RatingField(BaseField):
    type: Literal["Rating"]
    property: RatingProperty


class URLField(BaseField):
    type: Literal["URL"]
    property: Optional[EmptyProperty] = None


class PhoneField(BaseField):
    type: Literal["Phone"]
    property: Optional[EmptyProperty] = None


class EmailField(BaseField):
    type: Literal["Email"]
    property: Optional[EmptyProperty] = None


class WorkDocField(BaseField):
    type: Literal["WorkDoc"]
    property: Optional[EmptyProperty] = None


# Link Fields
class OneWayLinkField(BaseField):
    type: Literal["OneWayLink"]
    property: LinkProperty


class TwoWayLinkField(BaseField):
    type: Literal["TwoWayLink"]
    property: TwoWayLinkProperty


# Formula/Auto Fields
class FormulaField(BaseField):
    type: Literal["Formula"]
    property: FormulaProperty


class AutoNumberField(BaseField):
    type: Literal["AutoNumber"]
    property: Optional[EmptyProperty] = None


# User Fields
class CreatedByField(BaseField):
    type: Literal["CreatedBy"]
    property: UserProperty


class LastModifiedByField(BaseField):
    type: Literal["LastModifiedBy"]
    property: UserProperty


# Button Field
class ButtonField(BaseField):
    type: Literal["Button"]
    property: ButtonProperty


Field = Annotated[
    Union[
        SingleTextField,
        TextField,
        SingleSelectField,
        MultiSelectField,
        NumberField,
        CurrencyField,
        PercentField,
        DateTimeField,
        CreatedTimeField,
        LastModifiedTimeField,
        AttachmentField,
        MemberField,
        CheckboxField,
        RatingField,
        URLField,
        PhoneField,
        EmailField,
        WorkDocField,
        OneWayLinkField,
        TwoWayLinkField,
        FormulaField,
        AutoNumberField,
        CreatedByField,
        LastModifiedByField,
        ButtonField,
    ],
    PydanticField(discriminator="type"),
]


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


class UrlData(BaseModel):
    """URL数据模型"""
    title: str
    text: str
    favicon: str


class WorkDocData(BaseModel):
    """维格文档数据模型"""
    document_id: str = PydanticField(..., alias="documentId")
    title: str


class NodeData(BaseModel):
    """节点数据模型"""
    id: str
    name: str
    type: str
    icon: Optional[str] = None
    isFav: Optional[bool] = None
    permission: Optional[int] = None
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
    data: Optional[Dict[str, Any]] = PydanticField(None, description="包含records和pageToken的数据")


class FieldsResponseData(BaseModel):
    fields: List[Field]


class FieldsResponse(APIResponse):
    """字段响应模型"""
    data: Optional[FieldsResponseData] = PydanticField(None, description="包含fields的数据")


class CreateFieldResponseData(BaseModel):
    """创建字段响应数据模型"""
    id: str
    name: str


class CreateFieldResponse(APIResponse):
    """创建字段响应模型"""
    data: CreateFieldResponseData


class ViewsData(BaseModel):
    """视图列表数据模型"""
    views: List[ViewData]


class ViewsResponse(APIResponse):
    """视图响应模型"""
    data: Optional[ViewsData] = PydanticField(None, description="包含views的数据")


class DatasheetResponse(APIResponse):
    """数据表响应模型"""
    data: Optional[Dict[str, Any]] = PydanticField(None, description="包含datasheet信息的数据")


class SpaceResponse(APIResponse):
    """空间响应模型"""
    data: Optional[Dict[str, Any]] = PydanticField(None, description="包含spaces的数据")


class NodesData(BaseModel):
    """节点列表数据模型"""
    nodes: List[NodeData]


class NodeResponse(APIResponse):
    """节点响应模型"""
    data: Optional[NodesData] = PydanticField(None, description="包含nodes的数据")


class AttachmentResponse(APIResponse):
    """附件响应模型"""
    data: Optional[Dict[str, Any]] = PydanticField(None, description="包含attachment信息的数据")


class PostDatasheetMetaResponse(APIResponse):
    """创建数据表元数据响应（与原库兼容）"""
    data: Optional[Dict[str, Any]] = PydanticField(None, description="数据表元数据")


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
    'Field',
    'ViewData',
    'AttachmentData',
    'UrlData',
    'WorkDocData',
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
