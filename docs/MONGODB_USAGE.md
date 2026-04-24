# MongoDB 使用指南

本项目已集成 MongoDB 支持，提供同步（pymongo）和异步（motor）两种客户端。

## 配置

### 1. 配置文件方式

在 `config.yaml` 中配置：

```yaml
mongodb:
  url: mongodb://localhost:27017
  database: werss
```

### 2. 环境变量方式（推荐）

```bash
export MONGODB_URL="mongodb://user:password@localhost:27017"
export MONGODB_DATABASE="werss"
```

### 3. 连接字符串格式

- 无认证：`mongodb://host:port`
- 有认证：`mongodb://username:password@host:port`
- 指定数据库：`mongodb://host:port/database`
- 副本集：`mongodb://host1:port1,host2:port2,host3:port3/?replicaSet=myReplicaSet`

## 使用方式

### 1. 同步客户端（适用于普通场景）

```python
from core.mongodb import get_mongo_client
from core.models.mongo_article import ArticleDocument, get_article_mongo_repo

# 获取客户端
client = get_mongo_client()
db = client.get_db()
collection = client.get_collection("articles")

# 使用仓储
repo = get_article_mongo_repo()

# 创建文章
article = ArticleDocument(
    mp_id="MP_WXS_123456",
    title="测试文章",
    url="https://example.com/article/1",
    content="文章内容"
)
created = repo.create(article)
print(f"创建文章: {created.id}")

# 查询文章
articles = repo.find_by_mp_id("MP_WXS_123456", limit=10)
for article in articles:
    print(f"标题: {article.title}")

# 搜索文章
results = repo.search("关键词", limit=20)

# 更新文章
repo.mark_as_read(article.id)
repo.mark_as_favorite(article.id, is_favorite=True)

# 删除文章（软删除）
repo.soft_delete(article.id)
```

### 2. 异步客户端（适用于 FastAPI）

```python
from fastapi import APIRouter
from bson import ObjectId
from core.models.mongo_article import ArticleDocument, get_article_async_mongo_repo

router = APIRouter()

@router.get("/articles/{mp_id}")
async def get_articles(mp_id: str, skip: int = 0, limit: int = 30):
    """获取公众号文章列表"""
    repo = get_article_async_mongo_repo()
    articles = await repo.find_by_mp_id(mp_id, skip=skip, limit=limit)
    return {
        "data": [article.model_dump() for article in articles],
        "count": len(articles)
    }

@router.post("/articles")
async def create_article(article_data: dict):
    """创建文章"""
    repo = get_article_async_mongo_repo()
    article = ArticleDocument(**article_data)
    created = await repo.create(article)
    return {"id": str(created.id)}

@router.patch("/articles/{article_id}/read")
async def mark_read(article_id: str):
    """标记已读"""
    repo = get_article_async_mongo_repo()
    success = await repo.mark_as_read(ObjectId(article_id))
    return {"success": success}
```

## 自定义文档模型

### 1. 创建文档模型

```python
from typing import Optional, ClassVar, List, Dict, Any
from datetime import datetime
from bson import ObjectId
from pydantic import Field
from pymongo import ASCENDING, DESCENDING

from core.models.mongo_base import MongoDocument

class FeedDocument(MongoDocument):
    """公众号文档模型"""
    
    __collection__: ClassVar[str] = "feeds"
    
    __indexes__: ClassVar[List[Dict[str, Any]]] = [
        {"keys": [("mp_id", ASCENDING)], "unique": True},
        {"keys": [("name", ASCENDING)]},
    ]
    
    mp_id: str = Field(..., description="公众号ID")
    name: str = Field(..., description="公众号名称")
    cover: Optional[str] = Field(default=None, description="封面")
    description: Optional[str] = Field(default=None, description="描述")
    status: int = Field(default=0, description="状态")
```

### 2. 创建仓储

```python
from core.models.mongo_base import MongoRepository, AsyncMongoRepository

class FeedMongoRepository(MongoRepository):
    """公众号同步仓储"""
    
    def __init__(self):
        super().__init__(FeedDocument)
    
    def find_by_mp_id(self, mp_id: str) -> Optional[FeedDocument]:
        return self.find_one({"mp_id": mp_id})
    
    def find_active(self, skip: int = 0, limit: int = 100) -> List[FeedDocument]:
        return self.find_many(
            query={"status": 0},
            skip=skip,
            limit=limit,
            sort=[("name", ASCENDING)]
        )

class FeedAsyncMongoRepository(AsyncMongoRepository):
    """公众号异步仓储"""
    
    def __init__(self):
        super().__init__(FeedDocument)
    
    async def find_by_mp_id(self, mp_id: str) -> Optional[FeedDocument]:
        return await self.find_one({"mp_id": mp_id})
```

## 高级查询

### 1. 复杂查询条件

```python
from datetime import datetime, timedelta
from pymongo import ASCENDING, DESCENDING

# 时间范围查询
start_time = datetime.now() - timedelta(days=7)
articles = repo.find_many(
    query={
        "publish_time": {"$gte": start_time},
        "status": 0
    },
    sort=[("publish_time", DESCENDING)]
)

# 多条件查询
articles = repo.find_many(
    query={
        "mp_id": {"$in": ["mp1", "mp2", "mp3"]},
        "is_favorite": 1,
        "status": 0
    }
)

# 正则查询
articles = repo.find_many(
    query={
        "title": {"$regex": "关键词", "$options": "i"}
    }
)
```

### 2. 聚合查询

```python
from core.mongodb import get_mongo_client

client = get_mongo_client()
collection = client.get_collection("articles")

# 统计每个公众号的文章数
pipeline = [
    {"$match": {"status": 0}},
    {"$group": {
        "_id": "$mp_id",
        "count": {"$sum": 1},
        "latest": {"$max": "$publish_time"}
    }},
    {"$sort": {"count": -1}}
]

results = list(collection.aggregate(pipeline))
```

## 索引管理

### 1. 自动创建索引

在应用启动时，可以为所有文档模型创建索引：

```python
from core.mongodb import get_mongo_client
from core.models.mongo_article import ArticleDocument

client = get_mongo_client()

# 创建索引
indexes = ArticleDocument.get_indexes()
client.create_indexes(ArticleDocument.get_collection_name(), indexes)
```

### 2. 手动创建索引

```python
from core.mongodb import get_mongo_client
from pymongo import ASCENDING, DESCENDING, TEXT

client = get_mongo_client()
collection = client.get_collection("articles")

# 创建文本索引（用于全文搜索）
collection.create_index([("title", TEXT), ("content", TEXT)])

# 创建复合索引
collection.create_index([
    ("mp_id", ASCENDING),
    ("publish_time", DESCENDING)
])
```

## 数据迁移

如果需要从 SQL 数据库迁移到 MongoDB：

```python
from core.db import DB
from core.models.mongo_article import ArticleDocument, get_article_mongo_repo

def migrate_articles():
    """迁移文章数据"""
    # 从 SQL 读取
    session = DB.get_session()
    sql_articles = session.query(Article).all()
    
    # 写入 MongoDB
    repo = get_article_mongo_repo()
    for sql_article in sql_articles:
        mongo_article = ArticleDocument(
            article_id=sql_article.id,
            mp_id=sql_article.mp_id,
            title=sql_article.title,
            url=sql_article.url,
            content=sql_article.content,
            content_html=sql_article.content_html,
            publish_time=sql_article.publish_time,
            status=sql_article.status
        )
        repo.create(mongo_article)
        print(f"迁移文章: {mongo_article.title}")
```

## 性能优化建议

1. **合理使用索引**：为常用查询字段创建索引
2. **分页查询**：使用 skip 和 limit 进行分页
3. **投影查询**：只查询需要的字段
4. **批量操作**：使用 bulk_write 进行批量写入
5. **连接池**：已配置连接池，无需额外处理

## 注意事项

1. MongoDB 是可选的，不配置不会影响现有功能
2. 可以同时使用 SQL 和 MongoDB，根据场景选择
3. 文档模型使用 Pydantic，提供数据验证
4. 异步客户端适用于 FastAPI 的异步路由
5. 同步客户端适用于普通脚本和定时任务
