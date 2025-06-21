"""
维格表空间管理器

兼容原vika.py库的SpaceManager类
"""
from typing import Dict, Any, Optional, List
from .space import Space
from ..utils import get_space_id
from ..exceptions import ParameterException


class SpaceManager:
    """
    空间管理器，提供空间相关操作
    
    兼容原vika.py库的SpaceManager接口
    """
    
    def __init__(self, apitable):
        """
        初始化空间管理器
        
        Args:
            apitable: Apitable实例
        """
        self._apitable = apitable
    
    def get(self, space_id: str) -> Space:
        """
        获取空间实例
        
        Args:
            space_id: 空间站ID或URL
            
        Returns:
            空间实例
        """
        space_id = get_space_id(space_id)
        return Space(self._apitable, space_id)
    
    def list(self) -> List[Dict[str, Any]]:
        """
        获取空间列表
        
        Returns:
            空间列表
        """
        response = self._get_spaces()
        spaces_data = response.get('data', {}).get('spaces', [])
        return spaces_data
    
    def exists(self, space_id: str) -> bool:
        """
        检查空间是否存在
        
        Args:
            space_id: 空间站ID
            
        Returns:
            空间是否存在
        """
        try:
            space_id = get_space_id(space_id)
            spaces = self.list()
            for space in spaces:
                if space.get('id') == space_id:
                    return True
            return False
        except Exception:
            return False
    
    def find_by_name(self, space_name: str) -> Optional[Dict[str, Any]]:
        """
        根据空间名查找空间
        
        Args:
            space_name: 空间名称
            
        Returns:
            空间信息或None
        """
        spaces = self.list()
        for space in spaces:
            if space.get('name') == space_name:
                return space
        return None
    
    def get_space_by_name(self, space_name: str) -> Space:
        """
        根据空间名获取空间
        
        Args:
            space_name: 空间名称
            
        Returns:
            空间实例
            
        Raises:
            ParameterException: 空间不存在时
        """
        space_data = self.find_by_name(space_name)
        if not space_data:
            raise ParameterException(f"Space '{space_name}' not found")
        
        return self.get(space_data['id'])
    
    def get_default_space(self) -> Optional[Space]:
        """
        获取默认空间（第一个空间）
        
        Returns:
            默认空间实例或None
        """
        spaces = self.list()
        if spaces:
            return self.get(spaces[0]['id'])
        return None
    
    # 内部API调用方法
    def _get_spaces(self) -> Dict[str, Any]:
        """获取空间列表的内部API调用"""
        endpoint = "spaces"
        return self._apitable._session.get(endpoint)
    
    def __call__(self, space_id: str) -> Space:
        """
        支持直接调用获取空间
        
        Args:
            space_id: 空间站ID
            
        Returns:
            空间实例
        """
        return self.get(space_id)
    
    def __str__(self) -> str:
        return f"SpaceManager({self._apitable})"
    
    def __repr__(self) -> str:
        return f"SpaceManager()"


__all__ = ['SpaceManager']
