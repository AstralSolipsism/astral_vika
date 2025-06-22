"""
维格表HTTP请求处理模块

兼容原vika.py库的请求处理方式
"""
import httpx
from typing import Dict, Any, Optional, Callable, Awaitable

from .const import DEFAULT_API_BASE, FUSION_API_PREFIX
from .exceptions import VikaException
from .utils import build_api_url, handle_response


class Session:
    """
    一个原生异步的HTTP请求会话，使用httpx库。
    """

    def __init__(self, token: str, api_base: str = DEFAULT_API_BASE, status_callback: Optional[Callable[[str], Awaitable[None]]] = None):
        self.token = token
        self.api_base = api_base.rstrip('/')
        self.status_callback = status_callback
        headers = {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json',
            'User-Agent': 'vika-py/2.0.0'
        }
        self.client = httpx.AsyncClient(headers=headers, timeout=30.0)

    def _build_url(self, endpoint: str) -> str:
        """构建完整URL"""
        if endpoint.startswith('http'):
            return endpoint

        if not endpoint.startswith('/fusion'):
            endpoint = f"{FUSION_API_PREFIX.rstrip('/')}/{endpoint.lstrip('/')}"
        else:
            # 如果已经是完整的 /fusion/vX/ 路径，则直接使用
            pass

        return build_api_url(self.api_base, endpoint)

    async def request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        files: Optional[Dict] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        发送HTTP请求（异步）
        """
        url = self._build_url(endpoint)

        # httpx 会自动处理文件上传的 Content-Type
        # request_headers = self.headers.copy()
        # if headers:
        #     request_headers.update(headers)
        # if files:
        #     request_headers.pop('Content-Type', None)

        try:
            if self.status_callback:
                await self.status_callback(f"正在向 {url} 发送 {method} 请求...")
            response = await self.client.request(
                method=method.upper(),
                url=url,
                params=params,
                json=json,
                data=data,
                files=files,
                headers=headers,  # 允许覆盖默认头
            )

            # raise_for_status 会在 4xx 或 5xx 响应时引发 HTTPStatusError
            response.raise_for_status()

            if self.status_callback:
                await self.status_callback(f"成功接收到来自 {url} 的响应。")

            try:
                response_data = response.json()
            except httpx.JSONDecodeError:
                response_data = {
                    'message': f'Response parsing error: {response.text}',
                    'success': False
                }

            return handle_response(response_data, response.status_code)

        except httpx.HTTPStatusError as e:
            raise VikaException(
                f"HTTP error: {e.response.status_code} - {e.response.text}"
            ) from e
        except httpx.RequestError as e:
            raise VikaException(f"Network error: {str(e)}") from e

    async def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return await self.request('GET', endpoint, params=params)

    async def aget(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        return await self.request('GET', endpoint, params=params, headers=headers)

    async def post(
        self,
        endpoint: str,
        json: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        files: Optional[Dict] = None
    ) -> Dict[str, Any]:
        return await self.request('POST', endpoint, json=json, data=data, files=files)

    async def patch(self, endpoint: str, json: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return await self.request('PATCH', endpoint, json=json)

    async def put(self, endpoint: str, json: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return await self.request('PUT', endpoint, json=json)

    async def delete(self, endpoint: str, json: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return await self.request('DELETE', endpoint, json=json)

    async def close(self):
        """关闭客户端会话"""
        await self.client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


__all__ = ['Session']
