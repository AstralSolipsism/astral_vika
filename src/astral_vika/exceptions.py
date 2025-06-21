"""
维格表API异常类定义

兼容原vika.py库的异常结构
"""
from typing import Optional, Dict, Any


class VikaException(Exception):
    """维格表API基础异常类"""
    
    def __init__(self, message: str, code: Optional[int] = None, response: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.code = code
        self.response = response

    def __str__(self):
        if self.code:
            return f"[{self.code}] {self.message}"
        return self.message


class ApiException(VikaException):
    """API异常（与原库兼容的别名）"""
    pass


class AuthException(VikaException):
    """认证异常"""
    pass


class ParameterException(VikaException):
    """参数异常"""
    pass


class PermissionException(VikaException):
    """权限异常"""
    pass


class RateLimitException(VikaException):
    """频率限制异常"""
    pass


class ServerException(VikaException):
    """服务器异常"""
    pass


class AttachmentException(VikaException):
    """附件异常"""
    pass


class DatasheetNotFoundException(VikaException):
    """数据表未找到异常"""
    pass


class FieldNotFoundException(VikaException):
    """字段未找到异常"""
    pass


class RecordNotFoundException(VikaException):
    """记录未找到异常"""
    pass


# 为了与原库完全兼容，创建别名
class APIException(ApiException):
    """API异常别名"""
    pass


def create_exception_from_response(response_data: Dict[str, Any], status_code: int) -> VikaException:
    """根据响应数据创建相应的异常"""
    message = response_data.get('message', f'HTTP {status_code} Error')
    code = response_data.get('code', status_code)
    
    # 根据状态码选择异常类型
    if status_code == 401:
        return AuthException(message, code, response_data)
    elif status_code == 403:
        return PermissionException(message, code, response_data)
    elif status_code == 404 or code == 301:
        return DatasheetNotFoundException(message, code, response_data)
    elif status_code == 400:
        return ParameterException(message, code, response_data)
    elif status_code == 429:
        return RateLimitException(message, code, response_data)
    elif status_code >= 500:
        return ServerException(message, code, response_data)
    elif code in [426, 428]:
        return AttachmentException(message, code, response_data)
    else:
        return ApiException(message, code, response_data)


__all__ = [
    'VikaException',
    'ApiException', 
    'APIException',
    'AuthException',
    'ParameterException',
    'PermissionException',
    'RateLimitException',
    'ServerException',
    'AttachmentException',
    'DatasheetNotFoundException',
    'FieldNotFoundException',
    'RecordNotFoundException',
    'create_exception_from_response'
]
