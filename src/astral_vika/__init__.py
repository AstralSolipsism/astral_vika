"""
Astrbot专用维格表(Vika)工具库

完全兼容原vika.py库的API接口，解决版本依赖冲突问题。

一个完整的Python客户端库，支持维格表API的所有功能，包括：
- 记录管理 (CRUD)
- 字段和视图管理  
- 表格和空间站管理
- 附件上传
- 文件节点管理
- 成员、小组和角色管理
- AI功能
- 嵌入链接管理

作者: AstralSolipsism
版本: 0.9.0
兼容: Python 3.10+，与原vika.py库API完全兼容
"""

# 主入口类
from .apitable import Vika, Apitable

# 核心模块
from .datasheet import (
    Datasheet, DatasheetManager, Record, RecordManager, 
    Field, FieldManager, View, ViewManager, QuerySet,
    Attachment, AttachmentManager
)
from .space import Space, SpaceManager
from .node import NodeManager
from .unit import Member, Role, Team

# 类型定义
from .types import *

# 异常类
from .exceptions import *

# 常量
from .const import *

# 工具函数
from .utils import get_dst_id, get_space_id

# 版本信息
__version__ = "0.9.2"
__author__ = "AstralSolipsism"

# 主要导出（与原库兼容）
__all__ = [
    # 主入口类（兼容原库）
    'Vika',
    'Apitable',
    
    # 核心类
    'Datasheet',
    'DatasheetManager',
    'Record', 
    'RecordManager',
    'Field',
    'FieldManager',
    'View',
    'ViewManager',
    'QuerySet',
    'Attachment',
    'AttachmentManager',
    'Space',
    'SpaceManager',
    'NodeManager',
    'Member',
    'Role',
    'Team',
    
    # 类型定义（从types导入）
    'APIResponse',
    'RecordData',
    'FieldData',
    'ViewData',
    'SpaceData',
    'AttachmentData',
    'NodeData',
    'RecordsResponse',
    'FieldsResponse',
    'ViewsResponse',
    'DatasheetResponse',
    'SpaceResponse',
    'NodeResponse',
    'AttachmentResponse',
    'PostDatasheetMetaResponse',
    'PostDatasheetMeta',
    'UnitRoleCreateRo',
    'UnitRoleUpdateRo',
    'UnitMemberCreateRo',
    'UnitTeamCreateRo',
    'UnitModel',
    'MemberModel',
    'RoleModel',
    'TeamModel',
    
    # 异常类（从exceptions导入）
    'VikaException',
    'ApiException',
    'APIException',
    'AuthException',
    'ParameterException',
    'PermissionException',
    'RateLimitException',
    'ServerException',
    'AttachmentException',
    'DatasheetNotFoundException',
    'FieldNotFoundException',
    'RecordNotFoundException',
    
    # 工具函数
    'get_dst_id',
    'get_space_id',
    
    # 常量
    'DEFAULT_API_BASE',
    'MAX_RECORDS_PER_REQUEST',
    'MAX_RECORDS_PER_PROCESS',
    'FIELD_TYPE_MAP',
    'PYTHON_TYPE_MAP'
]
