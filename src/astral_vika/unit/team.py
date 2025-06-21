"""
维格表团队管理

兼容原vika.py库的Team类
"""
from typing import Dict, Any, Optional, List
from ..types.unit_model import UnitTeamCreateRo, UnitTeamUpdateRo
from ..exceptions import ParameterException


class Team:
    """
    团队管理类，提供团队相关操作
    
    兼容原vika.py库的Team接口
    """
    
    def __init__(self, space):
        """
        初始化团队管理器
        
        Args:
            space: 空间实例
        """
        self._space = space
    
    def get(self, unit_id: str) -> Dict[str, Any]:
        """
        获取团队信息
        
        Args:
            unit_id: 团队单元ID
            
        Returns:
            团队信息
        """
        # 通过获取团队成员来获取团队信息
        response = self._get_team_members(unit_id)
        return response.get('data', {})
    
    def list(self) -> List[Dict[str, Any]]:
        """
        获取团队列表
        
        Returns:
            团队列表
        """
        # 注意：原API可能没有直接的团队列表接口
        # 这里返回空列表，实际使用时需要根据具体API调整
        return []
    
    def create(self, team_data: UnitTeamCreateRo) -> Dict[str, Any]:
        """
        创建团队
        
        Args:
            team_data: 团队创建数据
            
        Returns:
            创建结果
        """
        response = self._create_team(team_data.dict())
        return response.get('data', {})
    
    def update(self, unit_id: str, team_data: UnitTeamUpdateRo) -> Dict[str, Any]:
        """
        更新团队信息
        
        Args:
            unit_id: 团队单元ID
            team_data: 团队更新数据
            
        Returns:
            更新结果
        """
        response = self._update_team(unit_id, team_data.dict())
        return response.get('data', {})
    
    def delete(self, unit_id: str) -> bool:
        """
        删除团队
        
        Args:
            unit_id: 团队单元ID
            
        Returns:
            是否删除成功
        """
        self._delete_team(unit_id)
        return True
    
    def get_members(self, unit_id: str) -> List[Dict[str, Any]]:
        """
        获取团队成员列表
        
        Args:
            unit_id: 团队单元ID
            
        Returns:
            团队成员列表
        """
        response = self._get_team_members(unit_id)
        members_data = response.get('data', {}).get('members', [])
        return members_data
    
    def get_children(self, unit_id: str) -> List[Dict[str, Any]]:
        """
        获取子团队列表
        
        Args:
            unit_id: 团队单元ID
            
        Returns:
            子团队列表
        """
        response = self._get_team_children(unit_id)
        children_data = response.get('data', {}).get('children', [])
        return children_data
    
    def exists(self, unit_id: str) -> bool:
        """
        检查团队是否存在
        
        Args:
            unit_id: 团队单元ID
            
        Returns:
            团队是否存在
        """
        try:
            self.get(unit_id)
            return True
        except Exception:
            return False
    
    def find_by_name(self, team_name: str) -> Optional[Dict[str, Any]]:
        """
        根据团队名查找团队
        
        Args:
            team_name: 团队名称
            
        Returns:
            团队信息或None
        """
        teams = self.list()
        for team in teams:
            if team.get('name') == team_name:
                return team
        return None
    
    def get_team_by_name(self, team_name: str) -> Dict[str, Any]:
        """
        根据团队名获取团队
        
        Args:
            team_name: 团队名称
            
        Returns:
            团队信息
            
        Raises:
            ParameterException: 团队不存在时
        """
        team = self.find_by_name(team_name)
        if not team:
            raise ParameterException(f"Team '{team_name}' not found")
        return team
    
    def add_member(self, unit_id: str, member_id: str) -> bool:
        """
        向团队添加成员
        
        Args:
            unit_id: 团队单元ID
            member_id: 成员ID
            
        Returns:
            是否添加成功
        """
        # 这个功能可能需要特定的API，暂时抛出未实现异常
        raise NotImplementedError("Add member to team is not implemented yet")
    
    def remove_member(self, unit_id: str, member_id: str) -> bool:
        """
        从团队移除成员
        
        Args:
            unit_id: 团队单元ID
            member_id: 成员ID
            
        Returns:
            是否移除成功
        """
        # 这个功能可能需要特定的API，暂时抛出未实现异常
        raise NotImplementedError("Remove member from team is not implemented yet")
    
    # 内部API调用方法
    def _get_team_members(self, unit_id: str) -> Dict[str, Any]:
        """获取团队成员的内部API调用"""
        endpoint = f"spaces/{self._space._space_id}/teams/{unit_id}/members"
        return self._space._apitable._session.get(endpoint)
    
    def _get_team_children(self, unit_id: str) -> Dict[str, Any]:
        """获取子团队的内部API调用"""
        endpoint = f"spaces/{self._space._space_id}/teams/{unit_id}/children"
        return self._space._apitable._session.get(endpoint)
    
    def _create_team(self, team_data: Dict[str, Any]) -> Dict[str, Any]:
        """创建团队的内部API调用"""
        endpoint = f"spaces/{self._space._space_id}/teams"
        return self._space._apitable._session.post(endpoint, json=team_data)
    
    def _update_team(self, unit_id: str, team_data: Dict[str, Any]) -> Dict[str, Any]:
        """更新团队的内部API调用"""
        endpoint = f"spaces/{self._space._space_id}/teams/{unit_id}"
        return self._space._apitable._session.put(endpoint, json=team_data)
    
    def _delete_team(self, unit_id: str) -> Dict[str, Any]:
        """删除团队的内部API调用"""
        endpoint = f"spaces/{self._space._space_id}/teams/{unit_id}"
        return self._space._apitable._session.delete(endpoint)
    
    def __str__(self) -> str:
        return f"Team({self._space})"
    
    def __repr__(self) -> str:
        return f"Team(space={self._space._space_id})"


__all__ = ['Team']
