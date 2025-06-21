# Vika for Astrabot

这是基于兼容性更强的依赖项对Vika交互工具库的重构项目，用于与 Vika 维格表（apitable.com）的 API 进行交互。

## Installation

通过 pip 可以轻松安装本库：

```bash
pip install astral_vika
```

## Usage

本项目重构过程严格遵守Vika官方文档的操作指引，目标是与原Vika库完全兼容
以下是一个基本的使用示例，展示了如何初始化客户端、获取数据表以及查询记录：

```python
from astral_vika import Vika

# 1. 初始化 Vika 客户端
# 建议从环境变量或安全配置中读取 API Token
vika = Vika(token="YOUR_API_TOKEN")

# 2. 获取指定的数据表
# 可以通过数据表 URL 或 ID 获取
datasheet = vika.datasheet("dstxxxxxxxxxxxxxx") # 替换为你的数据表ID

# 3. 查询所有记录
# records 是一个 Record 对象的列表
try:
    records = datasheet.records.all()
    for record in records:
        # 假设你的表中有“标题”字段
        print(f"记录ID: {record.id}, 标题: {record.get('标题')}")

except Exception as e:
    print(f"查询记录时出错: {e}")

# 4. 创建一条新记录
try:
    new_record = datasheet.records.create({
        "标题": "这是一条来自 Astrabot 的新记录",
        "备注": "通过 vika-for-astrabot SDK 创建"
    })
    print(f"成功创建记录: {new_record.id}")
except Exception as e:
    print(f"创建记录失败: {e}")

```

下面是一个在初始化时指定自定义 API 地址的示例，适用于私有化部署的 Vika 实例：

```python
# 使用自定义的 base URL 来初始化，适用于私有化部署
vika = Vika(
    token="YOUR_API_TOKEN",
    api_base="https://your-private-vika.com/fusion/v1"
)
```

## License

本项目根据 [MIT License](LICENSE) 授权。