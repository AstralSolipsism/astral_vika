"""
维格表成员管理

兼容原vika.py库的Member类
"""
from typing import Dict, Any, Optional, List
from ..types.unit_model import UnitMemberCreateRo, UnitMemberUpdateRo
from ..exceptions import ParameterException


class Member:
    """
    成员管理类，提供成员相关操作
    
    兼容原vika.py库的Member接口
    """
    
    def __init__(self, space):
        """
        初始化成员管理器
        
        Args:
            space: 空间实例
        """
        self._space = space
    
    def get(self, unit_id: str) -> Dict[str, Any]:
        """
        获取成员信息
        
        Args:
            unit_id: 成员单元ID
            
        Returns:
            成员信息
        """
        response = self._get_member(unit_id)
        return response.get('data', {})
    
    def list(self) -> List[Dict[str, Any]]:
        """
        获取成员列表
        
        Returns:
            成员列表
        """
        # 注意：原API可能没有直接的成员列表接口
        # 这里返回空列表，实际使用时需要根据具体API调整
        return []
    
    def create(self, member_data: UnitMemberCreateRo) -> Dict[str, Any]:
        """
        创建成员
        
        Args:
            member_data: 成员创建数据
            
        Returns:
            创建结果
        """
        response = self._create_member(member_data.dict())
        return response.get('data', {})
    
    def update(self, unit_id: str, member_data: UnitMemberUpdateRo) -> Dict[str, Any]:
        """
        更新成员信息
        
        Args:
            unit_id: 成员单元ID
            member_data: 成员更新数据
            
        Returns:
            更新结果
        """
        response = self._update_member(unit_id, member_data.dict())
        return response.get('data', {})
    
    def delete(self, unit_id: str) -> bool:
        """
        删除成员
        
        Args:
            unit_id: 成员单元ID
            
        Returns:
            是否删除成功
        """
        self._delete_member(unit_id)
        return True
    
    def activate(self, unit_id: str) -> Dict[str, Any]:
        """
        激活成员
        
        Args:
            unit_id: 成员单元ID
            
        Returns:
            更新结果
        """
        update_data = UnitMemberUpdateRo(isActive=True)
        return self.update(unit_id, update_data)
    
    def deactivate(self, unit_id: str) -> Dict[str, Any]:
        """
        停用成员
        
        Args:
            unit_id: 成员单元ID
            
        Returns:
            更新结果
        """
        update_data = UnitMemberUpdateRo(isActive=False)
        return self.update(unit_id, update_data)
    
    def exists(self, unit_id: str) -> bool:
        """
        检查成员是否存在
        
        Args:
            unit_id: 成员单元ID
            
        Returns:
            成员是否存在
        """
        try:
            self.get(unit_id)
            return True
        except Exception:
            return False
    
    # 内部API调用方法
    def _get_member(self, unit_id: str) -> Dict[str, Any]:
        """获取成员的内部API调用"""
        endpoint = f"spaces/{self._space._space_id}/members/{unit_id}"
        return self._space._apitable._session.get(endpoint)
    
    def _create_member(self, member_data: Dict[str, Any]) -> Dict[str, Any]:
        """创建成员的内部API调用"""
        endpoint = f"spaces/{self._space._space_id}/members"
        return self._space._apitable._session.post(endpoint, json=member_data)
    
    def _update_member(self, unit_id: str, member_data: Dict[str, Any]) -> Dict[str, Any]:
        """更新成员的内部API调用"""
        endpoint = f"spaces/{self._space._space_id}/members/{unit_id}"
        return self._space._apitable._session.put(endpoint, json=member_data)
    
    def _delete_member(self, unit_id: str) -> Dict[str, Any]:
        """删除成员的内部API调用"""
        endpoint = f"spaces/{self._space._space_id}/members/{unit_id}"
        return self._space._apitable._session.delete(endpoint)
    
    def __str__(self) -> str:
        return f"Member({self._space})"
    
    def __repr__(self) -> str:
        return f"Member(space={self._space._space_id})"


__all__ = ['Member']
