"""
维格表节点管理器

兼容原vika.py库的NodeManager类
"""
import logging
from typing import Dict, Any, Optional, List
from ..exceptions import ParameterException, VikaException
from ..types.response import NodeData, NodesData


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
    def is_fav(self) -> Optional[bool]:
        """是否收藏"""
        return self._data.get('isFav')

    @property
    def permission(self) -> Optional[int]:
        """节点权限"""
        return self._data.get('permission')
    
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
    
    async def alist(self) -> List[Node]:
        """
        获取节点列表（异步）
        
        Returns:
            节点列表
        """
        response = await self._aget_nodes()
        nodes_data = response.get('data', {}).get('nodes', [])
        return [Node(node_data) for node_data in nodes_data]
    
    async def aall(self) -> List[Node]:
        """
        获取所有节点（异步，别名方法）
        
        Returns:
            节点列表
        """
        return await self.alist()
    
    async def aget(self, node_id: str) -> Node:
        """
        获取指定节点（异步）
        
        Args:
            node_id: 节点ID
            
        Returns:
            节点实例
        """
        response = await self._aget_node_detail(node_id)
        node_data = response.get('data', {})
        return Node(node_data)
    
    async def asearch(
        self,
        node_type: Optional[str] = None,
        permission: Optional[int] = None
    ) -> List[Node]:
        """
        根据类型和权限搜索节点（异步），使用v2 API。
        
        Args:
            node_type: 节点类型，例如 'Datasheet'
            permission: 权限级别
            
        Returns:
            匹配的节点列表
        """
        params = {}
        if node_type:
            params['type'] = node_type
        if permission is not None:
            params['permission'] = permission
        
        response = await self._asearch_nodes(params)
        nodes_data = response.get('data', {}).get('nodes', [])
        return [Node(node_data) for node_data in nodes_data]
    
    async def afilter_by_type(self, node_type: str) -> List[Node]:
        """
        根据节点类型过滤节点（异步）
        
        Args:
            node_type: 节点类型
            
        Returns:
            匹配的节点列表
        """
        nodes = await self.alist()
        return [node for node in nodes if node.type == node_type]
    
    async def aget_datasheets(self) -> List[Node]:
        """
        获取数据表节点（异步）
        
        Returns:
            数据表节点列表
        """
        return await self.afilter_by_type("Datasheet")
    
    async def aget_folders(self) -> List[Node]:
        """
        获取文件夹节点（异步）
        
        Returns:
            文件夹节点列表
        """
        return await self.afilter_by_type("Folder")
    
    async def aget_forms(self) -> List[Node]:
        """
        获取表单节点（异步）
        
        Returns:
            表单节点列表
        """
        return await self.afilter_by_type("Form")
    
    async def aexists(self, node_id: str) -> bool:
        """
        检查节点是否存在（异步）
        
        Args:
            node_id: 节点ID
            
        Returns:
            节点是否存在
        """
        try:
            await self.aget(node_id)
            return True
        except Exception:
            return False
    
    async def afind_by_name(self, node_name: str, node_type: Optional[str] = None) -> Optional[Node]:
        """
        根据节点名查找节点（异步）
        
        Args:
            node_name: 节点名称
            node_type: 节点类型（可选）
            
        Returns:
            节点实例或None
        """
        nodes = await self.alist()
        for node in nodes:
            if node.name == node_name:
                if node_type is None or node.type == node_type:
                    return node
        return None
    
    async def aget_node_by_name(self, node_name: str, node_type: Optional[str] = None) -> Node:
        """
        根据节点名获取节点（异步）
        
        Args:
            node_name: 节点名称
            node_type: 节点类型（可选）
            
        Returns:
            节点实例
            
        Raises:
            ParameterException: 节点不存在时
        """
        node = await self.afind_by_name(node_name, node_type)
        if not node:
            type_info = f" of type '{node_type}'" if node_type else ""
            raise ParameterException(f"Node '{node_name}'{type_info} not found")
        return node
    
    async def acreate_embed_link(
        self,
        node_id: str,
        theme: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        创建嵌入链接（异步）
        
        Args:
            node_id: 节点ID
            theme: 主题
            payload: 额外参数
            
        Returns:
            创建结果
        """
        response = await self._acreate_embed_link(node_id, theme, payload)
        return response.get('data', {})
    
    async def aget_embed_links(self, node_id: str) -> List[Dict[str, Any]]:
        """
        获取嵌入链接列表（异步）
        
        Args:
            node_id: 节点ID
            
        Returns:
            嵌入链接列表
        """
        response = await self._aget_embed_links(node_id)
        links_data = response.get('data', {}).get('embedLinks', [])
        return links_data
    
    async def adelete_embed_link(self, node_id: str, link_id: str) -> bool:
        """
        删除嵌入链接（异步）
        
        Args:
            node_id: 节点ID
            link_id: 链接ID
            
        Returns:
            是否删除成功
        """
        await self._adelete_embed_link(node_id, link_id)
        return True
    
    # 内部API调用方法
    async def _aget_nodes(self) -> Dict[str, Any]:
        """获取节点列表的内部API调用"""
        endpoint = f"spaces/{self._space._space_id}/nodes"
        try:
            return await self._space._apitable.request_adapter.aget(endpoint)
        except VikaException as e:
            logging.error(f"Failed to get nodes for space {self._space._space_id}: {e}", exc_info=True)
            return {"success": False, "code": e.code if hasattr(e, 'code') else 500, "message": str(e), "data": {"nodes": []}}
        except Exception as e:
            logging.error(f"An unexpected error occurred while getting nodes for space {self._space._space_id}: {e}", exc_info=True)
            return {"success": False, "code": 500, "message": str(e), "data": {"nodes": []}}

    async def _asearch_nodes(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """搜索节点的内部API调用 (v2)"""
        # 注意：硬编码v2 API路径。这是一种临时解决方案。
        v2_endpoint = f"/fusion/v2/spaces/{self._space._space_id}/nodes"
        return await self._space._apitable.request_adapter.aget(v2_endpoint, params=params)
    
    async def _aget_node_detail(self, node_id: str) -> Dict[str, Any]:
        """获取节点详情的内部API调用"""
        endpoint = f"spaces/{self._space._space_id}/nodes/{node_id}"
        return await self._space._apitable.request_adapter.aget(endpoint)
    
    
    async def _acreate_embed_link(
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
        
        return await self._space._apitable.request_adapter.apost(endpoint, json=data)
    
    async def _aget_embed_links(self, node_id: str) -> Dict[str, Any]:
        """获取嵌入链接的内部API调用"""
        endpoint = f"spaces/{self._space._space_id}/nodes/{node_id}/embedlinks"
        return await self._space._apitable.request_adapter.aget(endpoint)
    
    async def _adelete_embed_link(self, node_id: str, link_id: str) -> Dict[str, Any]:
        """删除嵌入链接的内部API调用"""
        endpoint = f"spaces/{self._space._space_id}/nodes/{node_id}/embedlinks/{link_id}"
        return await self._space._apitable.request_adapter.adelete(endpoint)
    
    async def __len__(self) -> int:
        """返回节点数量"""
        nodes = await self.alist()
        return len(nodes)
    
    async def __aiter__(self):
        """支持异步迭代"""
        nodes = await self.alist()
        for node in nodes:
            yield node
    
    def __str__(self) -> str:
        return f"NodeManager({self._space})"
    
    def __repr__(self) -> str:
        return f"NodeManager(space={self._space._space_id})"


__all__ = ['Node', 'NodeManager']
