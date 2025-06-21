"""
维格表视图管理器

兼容原vika.py库的ViewManager类
"""
from typing import List, Dict, Any, Optional
from ..utils import timed_lru_cache
from ..exceptions import ParameterException


class View:
    """视图类"""
    
    def __init__(self, view_data: Dict[str, Any]):
        self._data = view_data
    
    @property
    def id(self) -> str:
        """视图ID"""
        return self._data.get('id', '')
    
    @property
    def name(self) -> str:
        """视图名"""
        return self._data.get('name', '')
    
    @property
    def type(self) -> str:
        """视图类型"""
        return self._data.get('type', '')
    
    @property
    def properties(self) -> Dict[str, Any]:
        """视图属性"""
        return self._data.get('property', {})
    
    @property
    def raw_data(self) -> Dict[str, Any]:
        """原始数据"""
        return self._data
    
    def __str__(self) -> str:
        return f"View({self.name}, {self.type})"
    
    def __repr__(self) -> str:
        return f"View(id='{self.id}', name='{self.name}', type='{self.type}')"


class ViewManager:
    """
    视图管理器，提供视图相关操作
    
    兼容原vika.py库的ViewManager接口
    """
    
    def __init__(self, datasheet):
        """
        初始化视图管理器
        
        Args:
            datasheet: 数据表实例
        """
        self._datasheet = datasheet
    
    @timed_lru_cache(seconds=300)
    def all(self) -> List[View]:
        """
        获取所有视图
        
        Returns:
            视图列表
        """
        response = self._get_views()
        views_data = response.get('data', {}).get('views', [])
        return [View(view_data) for view_data in views_data]
    
    def get(self, view_name_or_id: str) -> View:
        """
        获取指定视图
        
        Args:
            view_name_or_id: 视图名或视图ID
            
        Returns:
            视图实例
            
        Raises:
            ParameterException: 视图不存在时
        """
        views = self.all()
        
        for view in views:
            if view.name == view_name_or_id or view.id == view_name_or_id:
                return view
        
        raise ParameterException(f"View '{view_name_or_id}' not found")
    
    def get_by_name(self, view_name: str) -> View:
        """
        根据视图名获取视图
        
        Args:
            view_name: 视图名
            
        Returns:
            视图实例
        """
        return self.get(view_name)
    
    def get_by_id(self, view_id: str) -> View:
        """
        根据视图ID获取视图
        
        Args:
            view_id: 视图ID
            
        Returns:
            视图实例
        """
        return self.get(view_id)
    
    def get_default_view(self) -> Optional[View]:
        """
        获取默认视图（通常是第一个视图）
        
        Returns:
            默认视图实例或None
        """
        views = self.all()
        return views[0] if views else None
    
    def filter_by_type(self, view_type: str) -> List[View]:
        """
        根据视图类型过滤视图
        
        Args:
            view_type: 视图类型
            
        Returns:
            匹配的视图列表
        """
        views = self.all()
        return [view for view in views if view.type == view_type]
    
    def exists(self, view_name_or_id: str) -> bool:
        """
        检查视图是否存在
        
        Args:
            view_name_or_id: 视图名或视图ID
            
        Returns:
            视图是否存在
        """
        try:
            self.get(view_name_or_id)
            return True
        except ParameterException:
            return False
    
    def get_view_names(self) -> List[str]:
        """
        获取所有视图名
        
        Returns:
            视图名列表
        """
        views = self.all()
        return [view.name for view in views]
    
    def get_view_ids(self) -> List[str]:
        """
        获取所有视图ID
        
        Returns:
            视图ID列表
        """
        views = self.all()
        return [view.id for view in views]
    
    def get_view_mapping(self) -> Dict[str, str]:
        """
        获取视图名到视图ID的映射
        
        Returns:
            视图名到视图ID的映射字典
        """
        views = self.all()
        return {view.name: view.id for view in views}
    
    def get_id_mapping(self) -> Dict[str, str]:
        """
        获取视图ID到视图名的映射
        
        Returns:
            视图ID到视图名的映射字典
        """
        views = self.all()
        return {view.id: view.name for view in views}
    
    def get_grid_views(self) -> List[View]:
        """
        获取表格视图
        
        Returns:
            表格视图列表
        """
        return self.filter_by_type("Grid")
    
    def get_gallery_views(self) -> List[View]:
        """
        获取画廊视图
        
        Returns:
            画廊视图列表
        """
        return self.filter_by_type("Gallery")
    
    def get_kanban_views(self) -> List[View]:
        """
        获取看板视图
        
        Returns:
            看板视图列表
        """
        return self.filter_by_type("Kanban")
    
    def get_form_views(self) -> List[View]:
        """
        获取表单视图
        
        Returns:
            表单视图列表
        """
        return self.filter_by_type("Form")
    
    def get_calendar_views(self) -> List[View]:
        """
        获取日历视图
        
        Returns:
            日历视图列表
        """
        return self.filter_by_type("Calendar")
    
    def get_gantt_views(self) -> List[View]:
        """
        获取甘特视图
        
        Returns:
            甘特视图列表
        """
        return self.filter_by_type("Gantt")
    
    # 内部API调用方法
    def _get_views(self) -> Dict[str, Any]:
        """获取视图的内部API调用"""
        endpoint = f"datasheets/{self._datasheet._dst_id}/views"
        return self._datasheet._apitable._session.get(endpoint)
    
    def __len__(self) -> int:
        """返回视图数量"""
        return len(self.all())
    
    def __iter__(self):
        """支持迭代"""
        return iter(self.all())
    
    def __contains__(self, view_name_or_id: str) -> bool:
        """支持in操作符"""
        return self.exists(view_name_or_id)
    
    def __str__(self) -> str:
        return f"ViewManager({self._datasheet})"
    
    def __repr__(self) -> str:
        return f"ViewManager(datasheet={self._datasheet._dst_id})"


__all__ = ['View', 'ViewManager']
