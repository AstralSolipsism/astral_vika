"""
维格表HTTP请求处理模块

兼容原vika.py库的请求处理方式
"""
import asyncio
import aiohttp
from typing import Dict, Any, Optional, Union
from .const import DEFAULT_API_BASE, FUSION_API_PREFIX
from .utils import handle_response, build_api_url
from .exceptions import VikaException


class HttpClient:
    """HTTP客户端，兼容原库的requests.session()使用方式"""
    
    def __init__(self, token: str, api_base: str = DEFAULT_API_BASE):
        self.token = token
        self.api_base = api_base.rstrip('/')
        self.session: Optional[aiohttp.ClientSession] = None
        
        # 设置默认请求头
        self.headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json',
            'User-Agent': 'vika-py/2.0.0'
        }
    
    async def _ensure_session(self):
        """确保会话已创建"""
        if self.session is None or self.session.closed:
            connector = aiohttp.TCPConnector(
                limit=100,
                limit_per_host=10,
                keepalive_timeout=30
            )
            
            timeout = aiohttp.ClientTimeout(
                total=30,
                connect=10,
                sock_read=20
            )
            
            self.session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
                headers=self.headers
            )
    
    async def close(self):
        """关闭会话"""
        if self.session and not self.session.closed:
            await self.session.close()
    
    def _build_url(self, endpoint: str) -> str:
        """构建完整URL"""
        if endpoint.startswith('http'):
            return endpoint
        
        # 添加fusion API前缀
        if not endpoint.startswith('/fusion'):
            endpoint = f"{FUSION_API_PREFIX}/{endpoint.lstrip('/')}"
        
        return build_api_url(self.api_base, endpoint)
    
    async def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
        files: Optional[aiohttp.FormData] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """发送HTTP请求"""
        await self._ensure_session()
        
        url = self._build_url(endpoint)
        
        # 合并请求头
        request_headers = self.headers.copy()
        if headers:
            request_headers.update(headers)
        
        # 处理文件上传
        request_data = data
        if files:
            request_headers.pop('Content-Type', None)
            request_data = files  # 使用files作为data
            json_data = None  # 文件上传时不能同时使用json
        
        try:
            async with self.session.request(
                method=method.upper(),
                url=url,
                params=params,
                data=request_data,
                json=json_data,
                headers=request_headers
            ) as response:
                
                # 获取响应数据
                try:
                    response_data = await response.json()
                except (aiohttp.ContentTypeError, ValueError) as e:
                    # 处理非JSON响应或JSON解析错误
                    text_content = await response.text()
                    response_data = {'message': f'Response parsing error: {text_content}', 'success': False}
                
                # 处理响应
                return handle_response(response_data, response.status)
                
        except aiohttp.ClientError as e:
            raise VikaException(f"Network error: {str(e)}")
        except asyncio.TimeoutError:
            raise VikaException("Request timeout")
    
    async def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """GET请求"""
        return await self._request('GET', endpoint, params=params)
    
    async def post(
        self, 
        endpoint: str, 
        json_data: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        files: Optional[aiohttp.FormData] = None
    ) -> Dict[str, Any]:
        """POST请求"""
        return await self._request('POST', endpoint, json_data=json_data, data=data, files=files)
    
    async def patch(self, endpoint: str, json_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """PATCH请求"""
        return await self._request('PATCH', endpoint, json_data=json_data)
    
    async def put(self, endpoint: str, json_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """PUT请求"""
        return await self._request('PUT', endpoint, json_data=json_data)
    
    async def delete(self, endpoint: str, json_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """DELETE请求"""
        return await self._request('DELETE', endpoint, json_data=json_data)


class RequestAdapter:
    """
    请求适配器，提供与原库兼容的同步接口
    """
    
    def __init__(self, token: str, api_base: str = DEFAULT_API_BASE):
        self.client = HttpClient(token, api_base)
    
    def _run_async(self, coro):
        """运行异步协程"""
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # 在已有事件循环中运行
                task = asyncio.create_task(coro)
                return loop.run_until_complete(task)
            else:
                return loop.run_until_complete(coro)
        except RuntimeError:
            # 没有事件循环，创建新的
            return asyncio.run(coro)
    
    def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """同步GET请求"""
        return self._run_async(self.client.get(endpoint, params))
    
    def post(
        self, 
        endpoint: str, 
        json: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        files: Optional[aiohttp.FormData] = None
    ) -> Dict[str, Any]:
        """同步POST请求"""
        return self._run_async(self.client.post(endpoint, json_data=json, data=data, files=files))
    
    def patch(self, endpoint: str, json: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """同步PATCH请求"""
        return self._run_async(self.client.patch(endpoint, json))
    
    def put(self, endpoint: str, json: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """同步PUT请求"""
        return self._run_async(self.client.put(endpoint, json))
    
    def delete(self, endpoint: str, json: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """同步DELETE请求"""
        return self._run_async(self.client.delete(endpoint, json))
    
    def close(self):
        """关闭客户端"""
        self._run_async(self.client.close())
    
    # 上下文管理器支持
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def create_session(token: str, api_base: str = DEFAULT_API_BASE) -> RequestAdapter:
    """
    创建请求会话，兼容原库的使用方式
    
    Args:
        token: API token
        api_base: API基础URL
        
    Returns:
        请求适配器实例
    """
    return RequestAdapter(token, api_base)


__all__ = [
    'HttpClient',
    'RequestAdapter', 
    'create_session'
]
