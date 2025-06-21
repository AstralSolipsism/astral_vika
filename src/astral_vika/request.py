"""
维格表HTTP请求处理模块

兼容原vika.py库的请求处理方式
"""
import httpx
from typing import Dict, Any, Optional
from .const import DEFAULT_API_BASE, FUSION_API_PREFIX
from .utils import handle_response, build_api_url
from .exceptions import VikaException


class RequestAdapter:
    """
    HTTP请求适配器，使用httpx库
    """
    def __init__(self, token: str, api_base: str = DEFAULT_API_BASE):
        self.token = token
        self.api_base = api_base.rstrip('/')
        self.headers = {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json',
            'User-Agent': 'vika-py/2.0.0'
        }

    def _build_url(self, endpoint: str) -> str:
        """构建完整URL"""
        if endpoint.startswith('http'):
            return endpoint
        
        if not endpoint.startswith('/fusion'):
            endpoint = f"{FUSION_API_PREFIX}/{endpoint.lstrip('/')}"
            
        return build_api_url(self.api_base, endpoint)

    async def arequest(
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
        
        request_headers = self.headers.copy()
        if headers:
            request_headers.update(headers)
            
        if files:
            request_headers.pop('Content-Type', None)

        try:
            async with httpx.AsyncClient() as client:
                response = await client.request(
                    method=method.upper(),
                    url=url,
                    params=params,
                    json=json,
                    data=data,
                    files=files,
                    headers=request_headers,
                    timeout=30.0
                )
                
                try:
                    response_data = response.json()
                except httpx.JSONDecodeError:
                    response_data = {'message': f'Response parsing error: {response.text}', 'success': False}
                
                return handle_response(response_data, response.status_code)

        except httpx.HTTPStatusError as e:
            raise VikaException(f"HTTP error: {e.response.status_code} - {e.response.text}")
        except httpx.RequestError as e:
            raise VikaException(f"Network error: {str(e)}")

    async def aget(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return await self.arequest('GET', endpoint, params=params)

    async def apost(
        self,
        endpoint: str,
        json: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        files: Optional[Dict] = None
    ) -> Dict[str, Any]:
        return await self.arequest('POST', endpoint, json=json, data=data, files=files)

    async def apatch(self, endpoint: str, json: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return await self.arequest('PATCH', endpoint, json=json)

    async def aput(self, endpoint: str, json: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return await self.arequest('PUT', endpoint, json=json)

    async def adelete(self, endpoint: str, json: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return await self.arequest('DELETE', endpoint, json=json)


__all__ = ['RequestAdapter']
