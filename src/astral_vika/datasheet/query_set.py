"""
维格表查询集类

兼容原vika.py库的QuerySet类，支持链式调用
"""
from typing import List, Dict, Any, Optional, Union, Iterator
from .record import Record
from ..const import MAX_RECORDS_PER_REQUEST, MAX_RECORDS_RETURNED_BY_ALL
from ..exceptions import ParameterException


class QuerySet:
    """
    查询集类，支持链式调用和Django ORM风格的API
    
    兼容原vika.py库的QuerySet接口
    """
    
    def __init__(self, datasheet):
        """
        初始化查询集
        
        Args:
            datasheet: 数据表实例
        """
        self._datasheet = datasheet
        self._view_id = None
        self._fields = None
        self._filter_formula = None
        self._sort = None
        self._max_records = None
        self._record_ids = None
        self._page_size = None
        self._page_num = None
        self._field_key = "name"
        self._cell_format = "json"
        self._cached_records = None
        self._is_evaluated = False
    
    def filter(
        self, 
        formula: Optional[str] = None,
        fields: Optional[List[str]] = None,
        page_size: Optional[int] = None,
        page_token: Optional[str] = None,
        view_id: Optional[str] = None,
        max_records: Optional[int] = None,
        **kwargs
    ) -> 'QuerySet':
        """
        过滤记录（支持多种过滤条件）
        
        Args:
            formula: 过滤公式
            fields: 返回字段列表
            page_size: 每页记录数
            page_token: 分页标记
            view_id: 视图ID
            max_records: 最大记录数
            **kwargs: 其他参数
            
        Returns:
            新的QuerySet实例
        """
        new_qs = self._clone()
        
        if formula:
            new_qs._filter_formula = formula
        if fields:
            new_qs._fields = fields
        if page_size:
            new_qs._page_size = page_size
        if page_token:
            new_qs._page_token = page_token
        if view_id:
            new_qs._view_id = view_id
        if max_records:
            new_qs._max_records = max_records
            
        # 处理其他关键字参数
        for key, value in kwargs.items():
            if value is not None:
                setattr(new_qs, f'_{key}', value)
                
        return new_qs
    
    def filter_by_formula(self, formula: str) -> 'QuerySet':
        """
        按公式过滤记录（原库兼容方法）
        
        Args:
            formula: 过滤公式，如：{标题}="标题1"
            
        Returns:
            新的QuerySet实例
        """
        new_qs = self._clone()
        new_qs._filter_formula = formula
        return new_qs
    
    def order_by(self, *fields) -> 'QuerySet':
        """
        排序
        
        Args:
            *fields: 排序字段，支持'-'前缀表示降序
            
        Returns:
            新的QuerySet实例
        """
        new_qs = self._clone()
        sort_list = []
        
        for field in fields:
            if field.startswith('-'):
                sort_list.append({"field": field[1:], "order": "desc"})
            else:
                sort_list.append({"field": field, "order": "asc"})
        
        new_qs._sort = sort_list
        return new_qs
    
    def sort(self, sort_config: List[Dict[str, str]]) -> 'QuerySet':
        """
        排序（原库兼容方法）
        
        Args:
            sort_config: 排序配置列表
            
        Returns:
            新的QuerySet实例
        """
        new_qs = self._clone()
        new_qs._sort = sort_config
        return new_qs
    
    def fields(self, *field_names) -> 'QuerySet':
        """
        指定返回的字段
        
        Args:
            *field_names: 字段名列表
            
        Returns:
            新的QuerySet实例
        """
        new_qs = self._clone()
        new_qs._fields = list(field_names)
        return new_qs
    
    def view(self, view_id: str) -> 'QuerySet':
        """
        指定视图
        
        Args:
            view_id: 视图ID
            
        Returns:
            新的QuerySet实例
        """
        new_qs = self._clone()
        new_qs._view_id = view_id
        return new_qs
    
    def limit(self, max_records: int) -> 'QuerySet':
        """
        限制返回记录数
        
        Args:
            max_records: 最大记录数
            
        Returns:
            新的QuerySet实例
        """
        new_qs = self._clone()
        new_qs._max_records = max_records
        return new_qs
    
    def page_size(self, size: int) -> 'QuerySet':
        """
        设置分页大小
        
        Args:
            size: 分页大小
            
        Returns:
            新的QuerySet实例
        """
        new_qs = self._clone()
        new_qs._page_size = min(size, MAX_RECORDS_PER_REQUEST)
        return new_qs
    
    def page_num(self, page_number: int) -> 'QuerySet':
        """
        设置页码
        
        Args:
            page_number: 页码
            
        Returns:
            新的QuerySet实例
        """
        new_qs = self._clone()
        new_qs._page_num = page_number
        return new_qs

    def filter_by_ids(self, record_ids: List[str]) -> 'QuerySet':
        """
        按记录ID列表过滤记录
        
        Args:
            record_ids: 记录ID列表
            
        Returns:
            新的QuerySet实例
        """
        new_qs = self._clone()
        new_qs._record_ids = record_ids
        return new_qs

    def field_key(self, key: str) -> 'QuerySet':
        """
        设置字段键类型
        
        Args:
            key: 字段键类型 ("name" 或 "id")
            
        Returns:
            新的QuerySet实例
        """
        if key not in ["name", "id"]:
            raise ParameterException("field_key must be 'name' or 'id'")
        
        new_qs = self._clone()
        new_qs._field_key = key
        return new_qs
    
    def cell_format(self, format_type: str) -> 'QuerySet':
        """
        设置单元格格式
        
        Args:
            format_type: 格式类型 ("json" 或 "string")
            
        Returns:
            新的QuerySet实例
        """
        if format_type not in ["json", "string"]:
            raise ParameterException("cell_format must be 'json' or 'string'")
        
        new_qs = self._clone()
        new_qs._cell_format = format_type
        return new_qs
    
    async def aall(self, max_count: Optional[int] = None) -> List[Record]:
        """
        获取所有记录（异步，自动处理分页）
        
        Args:
            max_count: 最大记录数，默认为MAX_RECORDS_RETURNED_BY_ALL
            
        Returns:
            记录列表
        """
        if max_count is None:
            max_count = MAX_RECORDS_RETURNED_BY_ALL
        
        all_records = []
        page_token = None
        current_count = 0
        
        while current_count < max_count:
            remaining = max_count - current_count
            page_size = min(remaining, self._page_size or MAX_RECORDS_PER_REQUEST)
            
            response = await self._datasheet.records._aget_records(
                view_id=self._view_id,
                fields=self._fields,
                filter_by_formula=self._filter_formula,
                max_records=page_size,
                page_token=page_token,
                page_num=self._page_num,
                sort=self._sort,
                record_ids=self._record_ids,
                field_key=self._field_key,
                cell_format=self._cell_format
            )
            
            records_data = response.get('data', {}).get('records', [])
            if not records_data:
                break
            
            # 转换为Record对象
            for record_data in records_data:
                all_records.append(Record(record_data, self._datasheet))
                current_count += 1
                
                if current_count >= max_count:
                    break
            
            # 检查是否还有更多数据
            page_token = response.get('data', {}).get('pageToken')
            if not page_token:
                break
        
        return all_records
    
    async def afirst(self) -> Optional[Record]:
        """
        获取第一条记录（异步）
        
        Returns:
            第一条记录或None
        """
        records = await self.limit(1)._aevaluate()
        return records[0] if records else None
    
    async def alast(self) -> Optional[Record]:
        """
        获取最后一条记录（异步）
        
        Returns:
            最后一条记录或None
        """
        # 反转排序并获取第一条
        reversed_qs = self._clone()
        if reversed_qs._sort:
            # 反转现有排序
            new_sort = []
            for sort_item in reversed_qs._sort:
                new_order = "desc" if sort_item.get("order") == "asc" else "asc"
                new_sort.append({"field": sort_item["field"], "order": new_order})
            reversed_qs._sort = new_sort
        else:
            # 默认按创建时间降序
            reversed_qs._sort = [{"field": "创建时间", "order": "desc"}]
        
        records = await reversed_qs.limit(1)._aevaluate()
        return records[0] if records else None
    
    async def acount(self) -> int:
        """
        获取记录总数（异步）
        
        Returns:
            记录总数
        """
        # 获取第一页数据来获取总数信息
        response = await self._datasheet.records._aget_records(
            view_id=self._view_id,
            fields=["记录ID"] if self._fields is None else self._fields[:1],
            filter_by_formula=self._filter_formula,
            max_records=1,
            sort=self._sort,
            field_key=self._field_key,
            cell_format=self._cell_format
        )
        
        # 如果API返回总数信息，使用它；否则需要获取所有记录来计算
        data = response.get('data', {})
        if 'total' in data:
            return data['total']
        else:
            # 需要获取所有记录来计算总数
            all_records = await self.aall()
            return len(all_records)
    
    async def aexists(self) -> bool:
        """
        检查是否存在匹配的记录（异步）
        
        Returns:
            是否存在记录
        """
        return await self.afirst() is not None
    
    async def aget(self, **kwargs) -> Record:
        """
        获取单条记录（异步，如果有多条或没有记录会抛出异常）
        
        Args:
            **kwargs: 过滤条件
            
        Returns:
            单条记录
            
        Raises:
            ParameterException: 没有找到记录或找到多条记录
        """
        if kwargs:
            # 构建过滤公式
            conditions = []
            for field, value in kwargs.items():
                if isinstance(value, str):
                    conditions.append(f'{{field}} = "{value}"')
                else:
                    conditions.append(f'{{field}} = {value}')
            
            formula = " AND ".join(conditions)
            queryset = self.filter(formula)
        else:
            queryset = self
        
        records = await queryset.limit(2)._aevaluate()
        
        if not records:
            raise ParameterException("Record matching query does not exist")
        elif len(records) > 1:
            raise ParameterException("Query returned more than one record")
        
        return records[0]
    
    async def _aevaluate(self) -> List[Record]:
        """执行查询并返回记录列表（异步）"""
        if self._is_evaluated and self._cached_records is not None:
            return self._cached_records
        
        response = await self._datasheet.records._aget_records(
            view_id=self._view_id,
            fields=self._fields,
            filter_by_formula=self._filter_formula,
            max_records=self._max_records,
            page_size=self._page_size,
            page_num=self._page_num,
            sort=self._sort,
            record_ids=self._record_ids,
            field_key=self._field_key,
            cell_format=self._cell_format
        )
        
        records_data = response.get('data', {}).get('records', [])
        self._cached_records = [Record(record_data, self._datasheet) for record_data in records_data]
        self._is_evaluated = True
        
        return self._cached_records
    
    def _clone(self) -> 'QuerySet':
        """克隆QuerySet"""
        new_qs = QuerySet(self._datasheet)
        new_qs._view_id = self._view_id
        new_qs._fields = self._fields
        new_qs._filter_formula = self._filter_formula
        new_qs._sort = self._sort
        new_qs._max_records = self._max_records
        new_qs._record_ids = self._record_ids
        new_qs._page_size = self._page_size
        new_qs._page_num = self._page_num
        new_qs._field_key = self._field_key
        new_qs._cell_format = self._cell_format
        return new_qs
    
    # 支持异步迭代器接口
    async def __aiter__(self) -> Iterator[Record]:
        """异步迭代器支持"""
        records = await self._aevaluate()
        for record in records:
            yield record
    
    async def __alen__(self) -> int:
        """支持异步len()函数"""
        records = await self._aevaluate()
        return len(records)
    
    async def __agetitem__(self, key) -> Union[Record, List[Record]]:
        """支持异步索引和切片访问"""
        records = await self._aevaluate()
        return records[key]
    
    async def __abool__(self) -> bool:
        """支持异步bool()判断"""
        return await self.aexists()
    
    def __repr__(self) -> str:
        """字符串表示"""
        return f"<QuerySet: {self._datasheet}>"


__all__ = ['QuerySet']
