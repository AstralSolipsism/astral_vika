"""
维格表附件管理器

兼容原vika.py库的AttachmentManager类
"""
from typing import Dict, Any, Optional, List, Union
import aiohttp
import os
from pathlib import Path
from ..exceptions import ParameterException, AttachmentException
from ..utils import run_sync


class Attachment:
    """附件对象"""
    
    def __init__(self, attachment_data: Dict[str, Any]):
        self._data = attachment_data
    
    @property
    def token(self) -> str:
        """附件token"""
        return self._data.get('token', '')
    
    @property
    def name(self) -> str:
        """附件名称"""
        return self._data.get('name', '')
    
    @property
    def size(self) -> int:
        """附件大小（字节）"""
        return self._data.get('size', 0)
    
    @property
    def mime_type(self) -> str:
        """MIME类型"""
        return self._data.get('mimeType', '')
    
    @property
    def url(self) -> str:
        """附件访问URL"""
        return self._data.get('url', '')
    
    @property
    def width(self) -> Optional[int]:
        """图片宽度（如果是图片）"""
        return self._data.get('width')
    
    @property
    def height(self) -> Optional[int]:
        """图片高度（如果是图片）"""
        return self._data.get('height')
    
    @property
    def preview(self) -> Optional[str]:
        """预览URL"""
        return self._data.get('preview')
    
    @property
    def raw_data(self) -> Dict[str, Any]:
        """原始数据"""
        return self._data
    
    def __str__(self) -> str:
        return f"Attachment({self.name}, {self.size} bytes)"
    
    def __repr__(self) -> str:
        return f"Attachment(name='{self.name}', size={self.size}, mime_type='{self.mime_type}')"


class AttachmentManager:
    """
    附件管理器，提供附件上传和下载功能
    
    兼容原vika.py库的AttachmentManager接口
    """
    
    def __init__(self, datasheet):
        """
        初始化附件管理器
        
        Args:
            datasheet: 数据表实例
        """
        self._datasheet = datasheet
    
    async def aupload(self, file_path: str) -> Attachment:
        """
        上传附件（异步）
        
        Args:
            file_path: 本地文件路径
            
        Returns:
            上传后的附件对象
            
        Raises:
            AttachmentException: 上传失败时
            ParameterException: 参数错误时
        """
        if not os.path.exists(file_path):
            raise ParameterException(f"File not found: {file_path}")
        
        file_size = os.path.getsize(file_path)
        if file_size > 1024 * 1024 * 1024:  # 1GB limit
            raise AttachmentException("File size exceeds 1GB limit")
        
        response = await self._aupload_file(file_path)
        
        if response.get('success'):
            attachment_data = response.get('data', {})
            return Attachment(attachment_data)
        else:
            error_msg = response.get('message', 'Upload failed')
            raise AttachmentException(f"Failed to upload attachment: {error_msg}")
    
    async def aupload_file(self, file_path: str) -> Attachment:
        """
        上传文件（异步，原库兼容方法）
        
        Args:
            file_path: 本地文件路径
            
        Returns:
            上传后的附件对象
        """
        return await self.aupload(file_path)
    
    async def adownload(
        self,
        attachment: Union[Attachment, str, Dict[str, Any]],
        save_path: Optional[str] = None
    ) -> str:
        """
        下载附件（异步）
        
        Args:
            attachment: 附件对象、URL或附件数据字典
            save_path: 保存路径，如不指定则使用附件名称
            
        Returns:
            下载文件的本地路径
            
        Raises:
            AttachmentException: 下载失败时
        """
        # 获取下载URL和文件名
        if isinstance(attachment, Attachment):
            url = attachment.url
            filename = attachment.name
        elif isinstance(attachment, str):
            url = attachment
            filename = Path(attachment).name
        elif isinstance(attachment, dict):
            url = attachment.get('url', '')
            filename = attachment.get('name', 'attachment')
        else:
            raise ParameterException("Invalid attachment parameter")
        
        if not url:
            raise AttachmentException("No download URL available")
        
        # 确定保存路径
        if save_path:
            save_path = Path(save_path)
            if save_path.is_dir():
                save_path = save_path / filename
        else:
            save_path = Path(filename)
        
        # 下载文件
        try:
            downloaded_path = await self._adownload_file(url, str(save_path))
            return downloaded_path
        except Exception as e:
            raise AttachmentException(f"Failed to download attachment: {str(e)}")
    
    async def adownload_from_url(self, url: str, save_path: Optional[str] = None) -> str:
        """
        从URL下载附件（异步）
        
        Args:
            url: 附件URL
            save_path: 保存路径
            
        Returns:
            下载文件的本地路径
        """
        return await self.adownload(url, save_path)
    
    # 内部API调用方法
    async def _aupload_file(self, file_path: str) -> Dict[str, Any]:
        """上传文件的内部API调用"""
        endpoint = f"datasheets/{self._datasheet._dst_id}/attachments"
        
        # 准备文件数据
        return await self._async_upload_file(endpoint, file_path)
    
    async def _async_upload_file(self, endpoint: str, file_path: str) -> Dict[str, Any]:
        """异步上传文件"""
        file_path = Path(file_path)
        filename = file_path.name
        
        # 使用 apitable 实例的 api_base 构建 URL
        base_url = self._datasheet._apitable.api_base
        # 确保 base_url 后面有 /fusion/v1/
        if '/fusion/v1' not in base_url:
            base_url = base_url.rstrip('/') + '/fusion/v1'
        
        url = f"{base_url}/{endpoint}"
        
        headers = {
            'Authorization': f'Bearer {self._datasheet._apitable.token}'
        }
        
        # 创建FormData并在文件打开期间发送请求
        async with aiohttp.ClientSession() as session:
            form_data = aiohttp.FormData()
            with open(file_path, 'rb') as f:
                form_data.add_field(
                    'file',
                    f,
                    filename=filename,
                    content_type='application/octet-stream'
                )
                
                async with session.post(url, headers=headers, data=form_data) as response:
                    # 处理响应
                    text_content = await response.text()
                    
                    try:
                        import json
                        response_data = json.loads(text_content)
                    except json.JSONDecodeError:
                        response_data = {'message': text_content, 'success': False}
                    
                    # 检查状态码
                    if response.status >= 400:
                        from ..exceptions import create_exception_from_response
                        raise create_exception_from_response(response_data, response.status)
                    
                    return response_data
    
    async def _adownload_file(self, url: str, save_path: str) -> str:
        """下载文件的内部实现"""
        return await self._async_download_file(url, save_path)
    
    async def _async_download_file(self, url: str, save_path: str) -> str:
        """异步下载文件"""
        import aiohttp
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    raise AttachmentException(f"Failed to download file: HTTP {response.status}")
                
                # 确保目录存在
                save_path = Path(save_path)
                save_path.parent.mkdir(parents=True, exist_ok=True)
                
                # 读取内容
                content = await response.read()
                
                # 写入文件
                with open(save_path, 'wb') as f:
                    f.write(content)
                
                return str(save_path)
    
    def __str__(self) -> str:
        return f"AttachmentManager({self._datasheet})"
    
    def __repr__(self) -> str:
        return f"AttachmentManager(datasheet={self._datasheet._dst_id})"


__all__ = ['Attachment', 'AttachmentManager']
