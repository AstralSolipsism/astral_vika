"""
维格表单元模型类型定义

兼容原vika.py库的单元类型（成员、角色、团队）
"""
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field


class UnitModel(BaseModel):
    """单元基础模型"""
    unitId: str
    type: str
    name: str


class MemberModel(UnitModel):
    """成员模型"""
    uuid: Optional[str] = None
    userId: Optional[str] = None
    avatar: Optional[str] = None
    email: Optional[str] = None
    mobile: Optional[str] = None
    isActive: Optional[bool] = None


class RoleModel(UnitModel):
    """角色模型"""
    sequence: int
    color: Optional[str] = None


class TeamModel(UnitModel):
    """团队模型"""
    memberCount: int
    parentId: Optional[str] = None


# 请求模型
class UnitRoleCreateRo(BaseModel):
    """创建角色请求模型"""
    name: str
    color: Optional[str] = None


class UnitRoleUpdateRo(BaseModel):
    """更新角色请求模型"""
    name: str
    color: Optional[str] = None


class UnitMemberCreateRo(BaseModel):
    """创建成员请求模型"""
    email: Optional[str] = None
    mobile: Optional[str] = None
    name: Optional[str] = None


class UnitTeamCreateRo(BaseModel):
    """创建团队请求模型"""
    name: str
    parentId: Optional[str] = None


class UnitTeamUpdateRo(BaseModel):
    """更新团队请求模型"""
    name: str


class UnitMemberUpdateRo(BaseModel):
    """更新成员请求模型"""
    isActive: Optional[bool] = None


# 响应模型
class UnitListResponse(BaseModel):
    """单元列表响应模型"""
    success: bool
    code: int
    message: str
    data: Optional[Dict[str, Any]] = None


class UnitCreateResponse(BaseModel):
    """单元创建响应模型"""
    success: bool
    code: int
    message: str
    data: Optional[Dict[str, Any]] = None


class UnitUpdateResponse(BaseModel):
    """单元更新响应模型"""
    success: bool
    code: int
    message: str
    data: Optional[Dict[str, Any]] = None


class UnitDeleteResponse(BaseModel):
    """单元删除响应模型"""
    success: bool
    code: int
    message: str
    data: Optional[Dict[str, Any]] = None


# 权限相关模型
class PermissionModel(BaseModel):
    """权限模型"""
    permissionId: str
    permissionName: str
    enabled: bool


class RolePermissionModel(BaseModel):
    """角色权限模型"""
    roleId: str
    permissions: List[PermissionModel]


# 成员角色关联模型
class MemberRoleModel(BaseModel):
    """成员角色关联模型"""
    memberId: str
    roleIds: List[str]


# 团队成员模型
class TeamMemberModel(BaseModel):
    """团队成员模型"""
    teamId: str
    memberIds: List[str]


__all__ = [
    # 基础模型
    'UnitModel',
    'MemberModel',
    'RoleModel',
    'TeamModel',
    
    # 请求模型
    'UnitRoleCreateRo',
    'UnitRoleUpdateRo',
    'UnitMemberCreateRo',
    'UnitTeamCreateRo',
    'UnitTeamUpdateRo',
    'UnitMemberUpdateRo',
    
    # 响应模型
    'UnitListResponse',
    'UnitCreateResponse',
    'UnitUpdateResponse',
    'UnitDeleteResponse',
    
    # 权限模型
    'PermissionModel',
    'RolePermissionModel',
    'MemberRoleModel',
    'TeamMemberModel'
]
