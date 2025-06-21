"""
维格表节点管理器

兼容原vika.py库的NodeManager类
"""
from typing import Dict, Any, Optional, List
from ..exceptions import ParameterException


class Node:
    """节点类"""
    
    def __init__(self, node_data: Dict[str, Any]):
        self._data = node_data
    
    @property
    def id(self) -> str:
        """节点ID"""
        return self._data.get('id', '')
    
    @property
    def name(self) -> str:
        """节点名"""
        return self._data.get('name', '')
    
    @property
    def type(self) -> str:
        """节点类型"""
        return self._data.get('type', '')
    
    @property
    def icon(self) -> Optional[str]:
        """节点图标"""
        return self._data.get('icon')
    
    @property
    def parent_id(self) -> Optional[str]:
        """父节点ID"""
        return self._data.get('parentId')
    
    @property
    def children(self) -> List['Node']:
        """子节点列表"""
        children_data = self._data.get('children', [])
        return [Node(child_data) for child_data in children_data]
    
    @property
    def raw_data(self) -> Dict[str, Any]:
        """原始数据"""
        return self._data
    
    def __str__(self) -> str:
        return f"Node({self.name}, {self.type})"
    
    def __repr__(self) -> str:
        return f"Node(id='{self.id}', name='{self.name}', type='{self.type}')"


class NodeManager:
    """
    节点管理器，提供文件节点相关操作
    
    兼容原vika.py库的NodeManager接口
    """
    
    def __init__(self, space):
        """
        初始化节点管理器
        
        Args:
            space: 空间实例
        """
        self._space = space
    
    def list(self) -> List[Node]:
        """
        获取节点列表
        
        Returns:
            节点列表
        """
        response = self._get_nodes()
        nodes_data = response.get('data', {}).get('nodes', [])
        return [Node(node_data) for node_data in nodes_data]
    
    def all(self) -> List[Node]:
        """
        获取所有节点（别名方法）
        
        Returns:
            节点列表
        """
        return self.list()
    
    def get(self, node_id: str) -> Node:
        """
        获取指定节点
        
        Args:
            node_id: 节点ID
            
        Returns:
            节点实例
        """
        response = self._get_node_detail(node_id)
        node_data = response.get('data', {})
        return Node(node_data)
    
    def search(self, query: Optional[str] = None, node_type: Optional[str] = None) -> List[Node]:
        """
        搜索节点
        
        Args:
            query: 搜索关键词
            node_type: 节点类型过滤
            
        Returns:
            匹配的节点列表
        """
        response = self._search_nodes(query, node_type)
        nodes_data = response.get('data', {}).get('nodes', [])
        return [Node(node_data) for node_data in nodes_data]
    
    def filter_by_type(self, node_type: str) -> List[Node]:
        """
        根据节点类型过滤节点
        
        Args:
            node_type: 节点类型
            
        Returns:
            匹配的节点列表
        """
        nodes = self.list()
        return [node for node in nodes if node.type == node_type]
    
    def get_datasheets(self) -> List[Node]:
        """
        获取数据表节点
        
        Returns:
            数据表节点列表
        """
        return self.filter_by_type("Datasheet")
    
    def get_folders(self) -> List[Node]:
        """
        获取文件夹节点
        
        Returns:
            文件夹节点列表
        """
        return self.filter_by_type("Folder")
    
    def get_forms(self) -> List[Node]:
        """
        获取表单节点
        
        Returns:
            表单节点列表
        """
        return self.filter_by_type("Form")
    
    def exists(self, node_id: str) -> bool:
        """
        检查节点是否存在
        
        Args:
            node_id: 节点ID
            
        Returns:
            节点是否存在
        """
        try:
            self.get(node_id)
            return True
        except Exception:
            return False
    
    def find_by_name(self, node_name: str, node_type: Optional[str] = None) -> Optional[Node]:
        """
        根据节点名查找节点
        
        Args:
            node_name: 节点名称
            node_type: 节点类型（可选）
            
        Returns:
            节点实例或None
        """
        nodes = self.list()
        for node in nodes:
            if node.name == node_name:
                if node_type is None or node.type == node_type:
                    return node
        return None
    
    def get_node_by_name(self, node_name: str, node_type: Optional[str] = None) -> Node:
        """
        根据节点名获取节点
        
        Args:
            node_name: 节点名称
            node_type: 节点类型（可选）
            
        Returns:
            节点实例
            
        Raises:
            ParameterException: 节点不存在时
        """
        node = self.find_by_name(node_name, node_type)
        if not node:
            type_info = f" of type '{node_type}'" if node_type else ""
            raise ParameterException(f"Node '{node_name}'{type_info} not found")
        return node
    
    def create_embed_link(
        self,
        node_id: str,
        theme: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        创建嵌入链接
        
        Args:
            node_id: 节点ID
            theme: 主题
            payload: 额外参数
            
        Returns:
            创建结果
        """
        response = self._create_embed_link(node_id, theme, payload)
        return response.get('data', {})
    
    def get_embed_links(self, node_id: str) -> List[Dict[str, Any]]:
        """
        获取嵌入链接列表
        
        Args:
            node_id: 节点ID
            
        Returns:
            嵌入链接列表
        """
        response = self._get_embed_links(node_id)
        links_data = response.get('data', {}).get('embedLinks', [])
        return links_data
    
    def delete_embed_link(self, node_id: str, link_id: str) -> bool:
        """
        删除嵌入链接
        
        Args:
            node_id: 节点ID
            link_id: 链接ID
            
        Returns:
            是否删除成功
        """
        self._delete_embed_link(node_id, link_id)
        return True
    
    # 内部API调用方法
    def _get_nodes(self) -> Dict[str, Any]:
        """获取节点列表的内部API调用"""
        endpoint = f"spaces/{self._space._space_id}/nodes"
        return self._space._apitable._session.get(endpoint)
    
    def _get_node_detail(self, node_id: str) -> Dict[str, Any]:
        """获取节点详情的内部API调用"""
        endpoint = f"spaces/{self._space._space_id}/nodes/{node_id}"
        return self._space._apitable._session.get(endpoint)
    
    def _search_nodes(self, query: Optional[str] = None, node_type: Optional[str] = None) -> Dict[str, Any]:
        """搜索节点的内部API调用"""
        endpoint = f"spaces/{self._space._space_id}/nodes"
        # 使用v2 API进行搜索
        endpoint = endpoint.replace('/v1/', '/v2/')
        
        params = {}
        if query:
            params['query'] = query
        if node_type:
            params['type'] = node_type
        
        return self._space._apitable._session.get(endpoint, params=params)
    
    def _create_embed_link(
        self,
        node_id: str,
        theme: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """创建嵌入链接的内部API调用"""
        endpoint = f"spaces/{self._space._space_id}/nodes/{node_id}/embedlinks"
        
        data = {}
        if theme:
            data['theme'] = theme
        if payload:
            data['payload'] = payload
        
        return self._space._apitable._session.post(endpoint, json=data)
    
    def _get_embed_links(self, node_id: str) -> Dict[str, Any]:
        """获取嵌入链接的内部API调用"""
        endpoint = f"spaces/{self._space._space_id}/nodes/{node_id}/embedlinks"
        return self._space._apitable._session.get(endpoint)
    
    def _delete_embed_link(self, node_id: str, link_id: str) -> Dict[str, Any]:
        """删除嵌入链接的内部API调用"""
        endpoint = f"spaces/{self._space._space_id}/nodes/{node_id}/embedlinks/{link_id}"
        return self._space._apitable._session.delete(endpoint)
    
    def __len__(self) -> int:
        """返回节点数量"""
        return len(self.list())
    
    def __iter__(self):
        """支持迭代"""
        return iter(self.list())
    
    def __str__(self) -> str:
        return f"NodeManager({self._space})"
    
    def __repr__(self) -> str:
        return f"NodeManager(space={self._space._space_id})"


__all__ = ['Node', 'NodeManager']
