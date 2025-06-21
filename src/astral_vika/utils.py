"""
维格表工具函数

兼容原vika.py库的工具函数
"""
import re
import json
from typing import Dict, Any, Optional, Union
from urllib.parse import urlparse, parse_qs
from functools import wraps, lru_cache
import time
import asyncio
from .exceptions import create_exception_from_response, ParameterException


def get_dst_id(dst_id_or_url: str) -> str:
    """
    从数据表ID或URL中提取数据表ID
    
    Args:
        dst_id_or_url: 数据表ID或URL
        
    Returns:
        数据表ID
    """
    if not dst_id_or_url:
        raise ParameterException("dst_id_or_url cannot be empty")
    
    # 如果是URL，提取ID
    if dst_id_or_url.startswith('http'):
        try:
            parsed_url = urlparse(dst_id_or_url)
            path_parts = parsed_url.path.split('/')
            
            # 查找dst开头的ID
            for part in path_parts:
                if part.startswith('dst'):
                    return part
                    
            # 从查询参数中查找
            query_params = parse_qs(parsed_url.query)
            if 'dst' in query_params:
                return query_params['dst'][0]
                
            raise ParameterException(f"Cannot extract datasheet ID from URL: {dst_id_or_url}")
        except Exception as e:
            raise ParameterException(f"Invalid URL format: {dst_id_or_url}") from e
    
    # 验证ID格式
    if not dst_id_or_url.startswith('dst'):
        raise ParameterException(f"Invalid datasheet ID format: {dst_id_or_url}")
    
    return dst_id_or_url


def get_space_id(space_id_or_url: str) -> str:
    """
    从空间ID或URL中提取空间ID
    
    Args:
        space_id_or_url: 空间ID或URL
        
    Returns:
        空间ID
    """
    if not space_id_or_url:
        raise ParameterException("space_id_or_url cannot be empty")
    
    # 如果是URL，提取ID
    if space_id_or_url.startswith('http'):
        try:
            parsed_url = urlparse(space_id_or_url)
            path_parts = parsed_url.path.split('/')
            
            # 查找spc开头的ID
            for part in path_parts:
                if part.startswith('spc'):
                    return part
                    
            raise ParameterException(f"Cannot extract space ID from URL: {space_id_or_url}")
        except Exception as e:
            raise ParameterException(f"Invalid URL format: {space_id_or_url}") from e
    
    # 验证ID格式
    if not space_id_or_url.startswith('spc'):
        raise ParameterException(f"Invalid space ID format: {space_id_or_url}")
    
    return space_id_or_url


def handle_response(response_data: Dict[str, Any], status_code: int = 200) -> Dict[str, Any]:
    """
    处理API响应
    
    Args:
        response_data: 响应数据
        status_code: HTTP状态码
        
    Returns:
        处理后的响应数据
        
    Raises:
        VikaException: 当响应包含错误时
    """
    # 检查HTTP状态码
    if status_code >= 400:
        raise create_exception_from_response(response_data, status_code)
    
    # 检查API响应中的success字段
    if not response_data.get('success', True):
        error_code = response_data.get('code', status_code)
        raise create_exception_from_response(response_data, error_code)
    
    return response_data


def timed_lru_cache(seconds: int = 300, maxsize: int = 128):
    """
    带时间过期的LRU缓存装饰器
    
    Args:
        seconds: 缓存过期时间（秒）
        maxsize: 最大缓存条目数
    """
    def decorator(func):
        func = lru_cache(maxsize=maxsize)(func)
        func.lifetime = seconds
        func.expiration = time.time() + seconds
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            if time.time() >= func.expiration:
                func.cache_clear()
                func.expiration = time.time() + func.lifetime
            return func(*args, **kwargs)
        
        wrapper.cache_info = func.cache_info
        wrapper.cache_clear = func.cache_clear
        return wrapper
    
    return decorator


def validate_field_key(field_key: Optional[str]) -> str:
    """
    验证字段键类型
    
    Args:
        field_key: 字段键类型
        
    Returns:
        验证后的字段键类型
    """
    if field_key is None:
        return "name"
    
    if field_key not in ["name", "id"]:
        raise ParameterException(f"field_key must be 'name' or 'id', got: {field_key}")
    
    return field_key


def validate_cell_format(cell_format: Optional[str]) -> str:
    """
    验证单元格格式
    
    Args:
        cell_format: 单元格格式
        
    Returns:
        验证后的单元格格式
    """
    if cell_format is None:
        return "json"
    
    if cell_format not in ["json", "string"]:
        raise ParameterException(f"cell_format must be 'json' or 'string', got: {cell_format}")
    
    return cell_format


def build_api_url(base_url: str, endpoint: str) -> str:
    """
    构建API URL
    
    Args:
        base_url: 基础URL
        endpoint: API端点
        
    Returns:
        完整的API URL
    """
    base_url = base_url.rstrip('/')
    endpoint = endpoint.lstrip('/')
    return f"{base_url}/{endpoint}"


def format_records_for_api(records: list, field_key: str = "name") -> list:
    """
    格式化记录数据以符合API要求
    
    Args:
        records: 记录列表
        field_key: 字段键类型
        
    Returns:
        格式化后的记录列表
    """
    formatted_records = []
    
    for record in records:
        if isinstance(record, dict):
            if 'fields' in record:
                # 已经是正确格式
                formatted_records.append(record)
            else:
                # 需要包装为fields结构
                formatted_records.append({"fields": record})
        else:
            raise ParameterException("Record must be a dictionary")
    
    return formatted_records


def safe_json_loads(data: str, default=None):
    """
    安全的JSON解析
    
    Args:
        data: JSON字符串
        default: 解析失败时的默认值
        
    Returns:
        解析结果或默认值
    """
    try:
        return json.loads(data)
    except (json.JSONDecodeError, TypeError):
        return default


def chunk_list(lst: list, chunk_size: int):
    """
    将列表分块
    
    Args:
        lst: 要分块的列表
        chunk_size: 块大小
        
    Yields:
        分块后的子列表
    """
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]


# 异步工具函数
def run_sync(coro):
    """
    在同步代码中运行异步协程
    
    Args:
        coro: 协程对象
        
    Returns:
        协程执行结果
    """
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # 如果已经在事件循环中，使用新的事件循环
            import nest_asyncio
            nest_asyncio.apply()
            return loop.run_until_complete(coro)
        else:
            return loop.run_until_complete(coro)
    except RuntimeError:
        # 没有事件循环，创建新的
        return asyncio.run(coro)


__all__ = [
    'get_dst_id',
    'get_space_id', 
    'handle_response',
    'timed_lru_cache',
    'validate_field_key',
    'validate_cell_format',
    'build_api_url',
    'format_records_for_api',
    'safe_json_loads',
    'chunk_list',
    'run_sync'
]
