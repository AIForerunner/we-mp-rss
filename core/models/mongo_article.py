"""
MongoDB 文章文档模型
示例：将 Article SQL 模型转换为 MongoDB 文档模型
"""
from typing import Optional, List, ClassVar, Dict, Any
from datetime import datetime
from bson import ObjectId
from pydantic import Field
from pymongo import ASCENDING, DESCENDING

from core.models.mongo_base import MongoDocument, MongoRepository, AsyncMongoRepository


class ArticleDocument(MongoDocument):
    """
    文章文档模型
    对应 MongoDB 中的 articles 集合
    """
    
    # 集合名称
    __collection__: ClassVar[str] = "articles"
    
    # 索引定义
    __indexes__: ClassVar[List[Dict[str, Any]]] = [
        # mp_id 索引
        {"keys": [("mp_id", ASCENDING)]},
        # publish_time 降序索引
        {"keys": [("publish_time", DESCENDING)]},
        # url 唯一索引
        {"keys": [("url", ASCENDING)], "unique": True, "sparse": True},
        # 复合索引：mp_id + publish_time
        {"keys": [("mp_id", ASCENDING), ("publish_time", DESCENDING)]},
        # status 索引
        {"keys": [("status", ASCENDING)]},
    ]
    
    # 文章ID（格式：mp_id-original_id）
    article_id: Optional[str] = Field(default=None, description="文章唯一标识")
    
    # 公众号ID
    mp_id: str = Field(..., description="公众号ID")
    
    # 文章标题
    title: str = Field(..., description="文章标题")
    
    # 文章链接
    url: Optional[str] = Field(default=None, description="文章链接")
    
    # 文章摘要
    description: Optional[str] = Field(default=None, description="文章摘要")
    
    # 封面图片
    pic_url: Optional[str] = Field(default=None, description="封面图片URL")
    
    # 文章内容（纯文本）
    content: Optional[str] = Field(default=None, description="文章纯文本内容")
    
    # 文章内容（HTML）
    content_html: Optional[str] = Field(default=None, description="文章HTML内容")
    
    # 发布时间
    publish_time: Optional[datetime] = Field(default=None, description="发布时间")
    
    # 状态：0-正常，1-删除
    status: int = Field(default=0, description="状态")
    
    # 是否已读
    is_read: int = Field(default=0, description="是否已读")
    
    # 是否收藏
    is_favorite: int = Field(default=0, description="是否收藏")
    
    # 是否导出
    is_export: int = Field(default=0, description="是否导出")
    
    # 是否有内容
    has_content: int = Field(default=0, description="是否有内容")
    
    # 发布类型
    publish_type: Optional[str] = Field(default=None, description="发布类型")
    
    # 发布来源
    publish_src: Optional[str] = Field(default=None, description="发布来源")
    
    # 发布状态
    publish_status: Optional[int] = Field(default=None, description="发布状态")
    
    # 版权状态
    copyright_stat: Optional[int] = Field(default=None, description="版权状态")
    
    # 原创检测类型
    original_check_type: Optional[int] = Field(default=None, description="原创检测类型")
    
    # 服务类型
    service_type: Optional[int] = Field(default=None, description="服务类型")
    
    # 作者
    author: Optional[str] = Field(default=None, description="作者")
    
    # 来源
    source: Optional[str] = Field(default=None, description="来源")
    
    # 更新时间戳（毫秒）
    updated_at_millis: Optional[int] = Field(default=None, description="更新时间戳（毫秒）")


class ArticleMongoRepository(MongoRepository):
    """文章 MongoDB 仓储"""
    
    def __init__(self):
        super().__init__(ArticleDocument)
    
    def find_by_mp_id(
        self,
        mp_id: str,
        skip: int = 0,
        limit: int = 30,
        status: int = 0
    ) -> List[ArticleDocument]:
        """
        根据公众号ID查找文章
        
        Args:
            mp_id: 公众号ID
            skip: 跳过数量
            limit: 返回数量
            status: 文章状态
        
        Returns:
            文章列表
        """
        return self.find_many(
            query={"mp_id": mp_id, "status": status},
            skip=skip,
            limit=limit,
            sort=[("publish_time", DESCENDING)]
        )
    
    def find_by_url(self, url: str) -> Optional[ArticleDocument]:
        """
        根据URL查找文章
        
        Args:
            url: 文章URL
        
        Returns:
            文章或None
        """
        return self.find_one({"url": url})
    
    def search(
        self,
        keyword: str,
        mp_ids: Optional[List[str]] = None,
        skip: int = 0,
        limit: int = 30
    ) -> List[ArticleDocument]:
        """
        搜索文章（标题或内容）
        
        Args:
            keyword: 搜索关键词
            mp_ids: 公众号ID列表（可选）
            skip: 跳过数量
            limit: 返回数量
        
        Returns:
            文章列表
        """
        query = {
            "$or": [
                {"title": {"$regex": keyword, "$options": "i"}},
                {"content": {"$regex": keyword, "$options": "i"}}
            ],
            "status": 0
        }
        
        if mp_ids:
            query["mp_id"] = {"$in": mp_ids}
        
        return self.find_many(
            query=query,
            skip=skip,
            limit=limit,
            sort=[("publish_time", DESCENDING)]
        )
    
    def mark_as_read(self, article_id: ObjectId) -> bool:
        """标记为已读"""
        return self.update_by_id(article_id, {"is_read": 1})
    
    def mark_as_favorite(self, article_id: ObjectId, is_favorite: bool = True) -> bool:
        """标记收藏"""
        return self.update_by_id(article_id, {"is_favorite": 1 if is_favorite else 0})
    
    def soft_delete(self, article_id: ObjectId) -> bool:
        """软删除"""
        return self.update_by_id(article_id, {"status": 1})


class ArticleAsyncMongoRepository(AsyncMongoRepository):
    """文章异步 MongoDB 仓储"""
    
    def __init__(self):
        super().__init__(ArticleDocument)
    
    async def find_by_mp_id(
        self,
        mp_id: str,
        skip: int = 0,
        limit: int = 30,
        status: int = 0
    ) -> List[ArticleDocument]:
        """根据公众号ID查找文章"""
        return await self.find_many(
            query={"mp_id": mp_id, "status": status},
            skip=skip,
            limit=limit,
            sort=[("publish_time", DESCENDING)]
        )
    
    async def find_by_url(self, url: str) -> Optional[ArticleDocument]:
        """根据URL查找文章"""
        return await self.find_one({"url": url})
    
    async def search(
        self,
        keyword: str,
        mp_ids: Optional[List[str]] = None,
        skip: int = 0,
        limit: int = 30
    ) -> List[ArticleDocument]:
        """搜索文章"""
        query = {
            "$or": [
                {"title": {"$regex": keyword, "$options": "i"}},
                {"content": {"$regex": keyword, "$options": "i"}}
            ],
            "status": 0
        }
        
        if mp_ids:
            query["mp_id"] = {"$in": mp_ids}
        
        return await self.find_many(
            query=query,
            skip=skip,
            limit=limit,
            sort=[("publish_time", DESCENDING)]
        )
    
    async def mark_as_read(self, article_id: ObjectId) -> bool:
        """标记为已读"""
        return await self.update_by_id(article_id, {"is_read": 1})
    
    async def mark_as_favorite(self, article_id: ObjectId, is_favorite: bool = True) -> bool:
        """标记收藏"""
        return await self.update_by_id(article_id, {"is_favorite": 1 if is_favorite else 0})
    
    async def soft_delete(self, article_id: ObjectId) -> bool:
        """软删除"""
        return await self.update_by_id(article_id, {"status": 1})


# 全局仓储实例（延迟初始化）
_article_repo: Optional[ArticleMongoRepository] = None
_article_async_repo: Optional[ArticleAsyncMongoRepository] = None


def get_article_mongo_repo() -> ArticleMongoRepository:
    """获取文章 MongoDB 仓储"""
    global _article_repo
    if _article_repo is None:
        _article_repo = ArticleMongoRepository()
    return _article_repo


def get_article_async_mongo_repo() -> ArticleAsyncMongoRepository:
    """获取文章异步 MongoDB 仓储"""
    global _article_async_repo
    if _article_async_repo is None:
        _article_async_repo = ArticleAsyncMongoRepository()
    return _article_async_repo
