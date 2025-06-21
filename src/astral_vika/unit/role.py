"""
维格表角色管理

兼容原vika.py库的Role类
"""
from typing import Dict, Any, Optional, List
from ..types.unit_model import UnitRoleCreateRo, UnitRoleUpdateRo
from ..exceptions import ParameterException


class Role:
    """
    角色管理类，提供角色相关操作
    
    兼容原vika.py库的Role接口
    """
    
    def __init__(self, space):
        """
        初始化角色管理器
        
        Args:
            space: 空间实例
        """
        self._space = space
    
    def get(self, unit_id: str) -> Dict[str, Any]:
        """
        获取角色信息
        
        Args:
            unit_id: 角色单元ID
            
        Returns:
            角色信息
        """
        # 通过获取角色成员列表来获取角色信息
        response = self._get_role_members(unit_id)
        return response.get('data', {})
    
    def list(self) -> List[Dict[str, Any]]:
        """
        获取角色列表
        
        Returns:
            角色列表
        """
        response = self._get_roles()
        roles_data = response.get('data', {}).get('roles', [])
        return roles_data
    
    def create(self, role_data: UnitRoleCreateRo) -> Dict[str, Any]:
        """
        创建角色
        
        Args:
            role_data: 角色创建数据
            
        Returns:
            创建结果
        """
        response = self._create_role(role_data.dict())
        return response.get('data', {})
    
    def update(self, unit_id: str, role_data: UnitRoleUpdateRo) -> Dict[str, Any]:
        """
        更新角色信息
        
        Args:
            unit_id: 角色单元ID
            role_data: 角色更新数据
            
        Returns:
            更新结果
        """
        response = self._update_role(unit_id, role_data.dict())
        return response.get('data', {})
    
    def delete(self, unit_id: str) -> bool:
        """
        删除角色
        
        Args:
            unit_id: 角色单元ID
            
        Returns:
            是否删除成功
        """
        self._delete_role(unit_id)
        return True
    
    def get_members(self, unit_id: str) -> List[Dict[str, Any]]:
        """
        获取角色成员列表
        
        Args:
            unit_id: 角色单元ID
            
        Returns:
            角色成员列表
        """
        response = self._get_role_members(unit_id)
        members_data = response.get('data', {}).get('members', [])
        return members_data
    
    def get_teams(self, unit_id: str) -> List[Dict[str, Any]]:
        """
        获取角色关联的团队列表
        
        Args:
            unit_id: 角色单元ID
            
        Returns:
            角色团队列表
        """
        response = self._get_role_members(unit_id)
        teams_data = response.get('data', {}).get('teams', [])
        return teams_data
    
    def exists(self, unit_id: str) -> bool:
        """
        检查角色是否存在
        
        Args:
            unit_id: 角色单元ID
            
        Returns:
            角色是否存在
        """
        try:
            self.get(unit_id)
            return True
        except Exception:
            return False
    
    def find_by_name(self, role_name: str) -> Optional[Dict[str, Any]]:
        """
        根据角色名查找角色
        
        Args:
            role_name: 角色名称
            
        Returns:
            角色信息或None
        """
        roles = self.list()
        for role in roles:
            if role.get('name') == role_name:
                return role
        return None
    
    def get_role_by_name(self, role_name: str) -> Dict[str, Any]:
        """
        根据角色名获取角色
        
        Args:
            role_name: 角色名称
            
        Returns:
            角色信息
            
        Raises:
            ParameterException: 角色不存在时
        """
        role = self.find_by_name(role_name)
        if not role:
            raise ParameterException(f"Role '{role_name}' not found")
        return role
    
    # 内部API调用方法
    def _get_roles(self) -> Dict[str, Any]:
        """获取角色列表的内部API调用"""
        endpoint = f"spaces/{self._space._space_id}/roles"
        return self._space._apitable._session.get(endpoint)
    
    def _get_role_members(self, unit_id: str) -> Dict[str, Any]:
        """获取角色成员的内部API调用"""
        endpoint = f"spaces/{self._space._space_id}/roles/{unit_id}/members"
        return self._space._apitable._session.get(endpoint)
    
    def _create_role(self, role_data: Dict[str, Any]) -> Dict[str, Any]:
        """创建角色的内部API调用"""
        endpoint = f"spaces/{self._space._space_id}/roles"
        return self._space._apitable._session.post(endpoint, json=role_data)
    
    def _update_role(self, unit_id: str, role_data: Dict[str, Any]) -> Dict[str, Any]:
        """更新角色的内部API调用"""
        endpoint = f"spaces/{self._space._space_id}/roles/{unit_id}"
        return self._space._apitable._session.put(endpoint, json=role_data)
    
    def _delete_role(self, unit_id: str) -> Dict[str, Any]:
        """删除角色的内部API调用"""
        endpoint = f"spaces/{self._space._space_id}/roles/{unit_id}"
        return self._space._apitable._session.delete(endpoint)
    
    def __str__(self) -> str:
        return f"Role({self._space})"
    
    def __repr__(self) -> str:
        return f"Role(space={self._space._space_id})"


__all__ = ['Role']
