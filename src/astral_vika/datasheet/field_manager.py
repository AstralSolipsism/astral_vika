"""
维格表字段管理器

兼容原vika.py库的FieldManager类
"""
from typing import List, Dict, Any, Optional
from ..utils import timed_lru_cache
from ..exceptions import ParameterException, FieldNotFoundException


class Field:
    """字段类"""
    
    def __init__(self, field_data: Dict[str, Any]):
        self._data = field_data
    
    @property
    def id(self) -> str:
        """字段ID"""
        return self._data.get('id', '')
    
    @property
    def name(self) -> str:
        """字段名"""
        return self._data.get('name', '')
    
    @property
    def type(self) -> str:
        """字段类型"""
        return self._data.get('type', '')
    
    @property
    def properties(self) -> Dict[str, Any]:
        """字段属性"""
        return self._data.get('property', {})
    
    @property
    def editable(self) -> bool:
        """是否可编辑"""
        return self._data.get('editable', True)
    
    @property
    def is_primary(self) -> bool:
        """是否为主字段"""
        return self._data.get('isPrimary', False)
    
    @property
    def raw_data(self) -> Dict[str, Any]:
        """原始数据"""
        return self._data
    
    def __str__(self) -> str:
        return f"Field({self.name}, {self.type})"
    
    def __repr__(self) -> str:
        return f"Field(id='{self.id}', name='{self.name}', type='{self.type}')"


class FieldManager:
    """
    字段管理器，提供字段相关操作
    
    兼容原vika.py库的FieldManager接口
    """
    
    def __init__(self, datasheet):
        """
        初始化字段管理器
        
        Args:
            datasheet: 数据表实例
        """
        self._datasheet = datasheet
    
    @timed_lru_cache(seconds=300)
    def all(self) -> List[Field]:
        """
        获取所有字段
        
        Returns:
            字段列表
        """
        response = self._get_fields()
        fields_data = response.get('data', {}).get('fields', [])
        return [Field(field_data) for field_data in fields_data]
    
    def get(self, field_name_or_id: str) -> Field:
        """
        获取指定字段
        
        Args:
            field_name_or_id: 字段名或字段ID
            
        Returns:
            字段实例
            
        Raises:
            FieldNotFoundException: 字段不存在时
        """
        fields = self.all()
        
        for field in fields:
            if field.name == field_name_or_id or field.id == field_name_or_id:
                return field
        
        raise FieldNotFoundException(f"Field '{field_name_or_id}' not found")
    
    def get_by_name(self, field_name: str) -> Field:
        """
        根据字段名获取字段
        
        Args:
            field_name: 字段名
            
        Returns:
            字段实例
        """
        return self.get(field_name)
    
    def get_by_id(self, field_id: str) -> Field:
        """
        根据字段ID获取字段
        
        Args:
            field_id: 字段ID
            
        Returns:
            字段实例
        """
        return self.get(field_id)
    
    def get_primary_field(self) -> Optional[Field]:
        """
        获取主字段
        
        Returns:
            主字段实例或None
        """
        fields = self.all()
        for field in fields:
            if field.is_primary:
                return field
        return None
    
    def filter_by_type(self, field_type: str) -> List[Field]:
        """
        根据字段类型过滤字段
        
        Args:
            field_type: 字段类型
            
        Returns:
            匹配的字段列表
        """
        fields = self.all()
        return [field for field in fields if field.type == field_type]
    
    def get_editable_fields(self) -> List[Field]:
        """
        获取可编辑字段
        
        Returns:
            可编辑字段列表
        """
        fields = self.all()
        return [field for field in fields if field.editable]
    
    def exists(self, field_name_or_id: str) -> bool:
        """
        检查字段是否存在
        
        Args:
            field_name_or_id: 字段名或字段ID
            
        Returns:
            字段是否存在
        """
        try:
            self.get(field_name_or_id)
            return True
        except FieldNotFoundException:
            return False
    
    def create(
        self,
        name: str,
        field_type: str,
        property: Optional[Dict[str, Any]] = None
    ) -> Field:
        """
        创建字段
        
        Args:
            name: 字段名
            field_type: 字段类型
            property: 字段属性
            
        Returns:
            创建的字段实例
        """
        if not self._datasheet._spc_id:
            raise ParameterException("Space ID is required for field creation")
        
        response = self._create_field(name, field_type, property)
        field_data = response.get('data', {})
        
        # 清除缓存以获取最新字段列表
        self.all.cache_clear()
        
        return Field(field_data)
    
    def delete(self, field_name_or_id: str) -> bool:
        """
        删除字段
        
        Args:
            field_name_or_id: 字段名或字段ID
            
        Returns:
            是否删除成功
        """
        if not self._datasheet._spc_id:
            raise ParameterException("Space ID is required for field deletion")
        
        field = self.get(field_name_or_id)
        self._delete_field(field.id)
        
        # 清除缓存以获取最新字段列表
        self.all.cache_clear()
        
        return True
    
    def get_field_names(self) -> List[str]:
        """
        获取所有字段名
        
        Returns:
            字段名列表
        """
        fields = self.all()
        return [field.name for field in fields]
    
    def get_field_ids(self) -> List[str]:
        """
        获取所有字段ID
        
        Returns:
            字段ID列表
        """
        fields = self.all()
        return [field.id for field in fields]
    
    def get_field_mapping(self) -> Dict[str, str]:
        """
        获取字段名到字段ID的映射
        
        Returns:
            字段名到字段ID的映射字典
        """
        fields = self.all()
        return {field.name: field.id for field in fields}
    
    def get_id_mapping(self) -> Dict[str, str]:
        """
        获取字段ID到字段名的映射
        
        Returns:
            字段ID到字段名的映射字典
        """
        fields = self.all()
        return {field.id: field.name for field in fields}
    
    # 内部API调用方法
    def _get_fields(self) -> Dict[str, Any]:
        """获取字段的内部API调用"""
        endpoint = f"datasheets/{self._datasheet._dst_id}/fields"
        return self._datasheet._apitable._session.get(endpoint)
    
    def _create_field(
        self,
        name: str,
        field_type: str,
        property: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """创建字段的内部API调用"""
        endpoint = f"spaces/{self._datasheet._spc_id}/datasheets/{self._datasheet._dst_id}/fields"
        
        data = {
            "name": name,
            "type": field_type
        }
        if property:
            data["property"] = property
        
        return self._datasheet._apitable._session.post(endpoint, json=data)
    
    def _delete_field(self, field_id: str) -> Dict[str, Any]:
        """删除字段的内部API调用"""
        endpoint = f"spaces/{self._datasheet._spc_id}/datasheets/{self._datasheet._dst_id}/fields/{field_id}"
        return self._datasheet._apitable._session.delete(endpoint)
    
    def __len__(self) -> int:
        """返回字段数量"""
        return len(self.all())
    
    def __iter__(self):
        """支持迭代"""
        return iter(self.all())
    
    def __contains__(self, field_name_or_id: str) -> bool:
        """支持in操作符"""
        return self.exists(field_name_or_id)
    
    def __str__(self) -> str:
        return f"FieldManager({self._datasheet})"
    
    def __repr__(self) -> str:
        return f"FieldManager(datasheet={self._datasheet._dst_id})"


__all__ = ['Field', 'FieldManager']
