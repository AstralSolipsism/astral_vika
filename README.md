# Astral Vika

[![PyPI version](https://img.shields.io/pypi/v/astral-vika.svg)](https://pypi.org/project/astral-vika/)
[![License](https://img.shields.io/pypi/l/astral-vika.svg)](https://github.com/Astral-Lab/astral-vika/blob/main/LICENSE)

`astral_vika` 是一个为 [Vika 维格表](https://vika.cn/) 设计的、完全重构的现代异步 Python 客户端库。它基于 `asyncio` 和 `httpx`，提供了简洁、强大且类型友好的 API，旨在帮助开发者高效地与 Vika API 进行交互。

该项目是为了支持 [AstrBot](https://github.com/Astral-Lab/AstrBot) 与 Vika 的集成而诞生，但作为一个独立的库，它可以用于任何需要异步访问 Vika 数据的 Python 项目。

## ✨ 特性

- **完全异步**：所有 API 请求均为异步操作，完美适配现代 Python Web 框架和应用。
- **简洁的链式查询**：提供类似 ORM 的链式调用来构建复杂的查询。
- **自动分页处理**：使用 `.all().aall()` 或异步迭代 (`async for`) 获取所有记录，无需手动处理分页。
- **类型友好**：代码库包含类型提示，便于静态分析和获得更好的 IDE 支持。
- **Pydantic 模型**：API 响应被解析为 Pydantic 模型，提供便捷的数据访问和验证。
- **上下文管理**：支持 `async with` 语句，自动管理客户端会话。

## 🚀 安装

```bash
pip install astral-vika
```

## ⚡️ 快速入门

以下是一个完整的示例，展示了如何初始化客户端、获取维格表以及对记录进行增删改查（CRUD）操作。

```python
import asyncio
from astral_vika import Vika

# 假设维格表中有“标题”和“状态”两个字段

async def main():
    # 1. 初始化客户端
    # 建议从环境变量或安全配置中读取 Token
    vika = Vika(token="YOUR_API_TOKEN")

    # (可选) 验证认证信息是否有效
    if not await vika.aauth():
        print("API Token 无效或已过期")
        return

    # 2. 获取维格表对象
    # 你可以使用维格表的 ID (dst...) 或完整的 URL
    datasheet = vika.datasheet("YOUR_DATASHEET_ID_OR_URL")
    print(f"成功连接到维格表: {await datasheet.aname()}")

    # 3. 创建记录
    print("\n--- 正在创建新记录... ---")
    new_records_data = [
        {"标题": "学习 astral_vika 库", "状态": "进行中"},
        {"标题": "编写 README 文档", "状态": "待开始"},
    ]
    created_records = await datasheet.records.acreate(new_records_data)
    print(f"成功创建 {len(created_records)} 条记录。")
    for record in created_records:
        print(f"  - ID: {record.id}, 标题: {record.get('标题')}")

    # 4. 查询记录
    print("\n--- 正在查询记录... ---")
    # 获取所有记录并异步迭代
    print("所有记录:")
    async for record in datasheet.records.all():
        print(f"  - {record.get('标题')} ({record.get('状态')})")

    # 链式查询：查找“进行中”的记录，并只返回“标题”字段
    print("\n进行中的任务:")
    in_progress_records = await datasheet.records.filter_by_formula('{状态}="进行中"').fields('标题').aall()
    for record in in_progress_records:
        print(f"  - {record.get('标题')}")

    # 5. 更新记录
    print("\n--- 正在更新记录... ---")
    record_to_update = created_records[1]  # 我们来更新“编写 README”这条记录
    await datasheet.records.aupdate([
        {"recordId": record_to_update.id, "fields": {"状态": "已完成"}}
    ])
    print(f"记录 '{record_to_update.get('标题')}' 的状态已更新为 '已完成'。")

    # 6. 删除记录
    print("\n--- 正在删除记录... ---")
    record_ids_to_delete = [rec.id for rec in created_records]
    if await datasheet.records.adelete(record_ids_to_delete):
        print("所有在本示例中创建的记录均已成功删除。")

    # 客户端会话在使用 async with 时会自动关闭，或者在程序结束时由 httpx 清理
    # await vika.aclose()

if __name__ == "__main__":
    # 在你的应用中运行异步主函数
    asyncio.run(main())
```

## 📄 许可证

本项目根据 [MIT License](LICENSE) 授权。