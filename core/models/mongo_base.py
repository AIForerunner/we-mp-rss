"""
MongoDB 文档模型基类
提供类似 SQLAlchemy 的模型定义体验
"""
from typing import Optional, Dict, Any, List, ClassVar
from datetime import datetime
from bson import ObjectId
from pydantic import BaseModel, Field, ConfigDict


class MongoDocument(BaseModel):
    """
    MongoDB 文档基类
    基于 Pydantic BaseModel，提供文档验证和序列化
    """
    
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        json_encoders={ObjectId: str}
    )
    
    # MongoDB _id 字段
    id: Optional[ObjectId] = Field(default=None, alias="_id")
    
    # 创建和更新时间
    created_at: Optional[datetime] = Field(default_factory=datetime.now)
    updated_at: Optional[datetime] = Field(default_factory=datetime.now)
    
    # 子类需要定义的集合名称
    __collection__: ClassVar[str] = ""
    
    # 子类需要定义的索引
    __indexes__: ClassVar[List[Dict[str, Any]]] = []
    
    def model_post_init(self, __context: Any) -> None:
        """模型初始化后处理"""
        if self.created_at is None:
            self.created_at = datetime.now()
        if self.updated_at is None:
            self.updated_at = datetime.now()
    
    def to_mongo(self, exclude_none: bool = True) -> Dict[str, Any]:
        """
        转换为 MongoDB 文档格式
        
        Args:
            exclude_none: 是否排除 None 值
        
        Returns:
            MongoDB 文档字典
        """
        data = self.model_dump(
            by_alias=True,
            exclude_none=exclude_none,
            exclude={'id'} if self.id is None else set()
        )
        
        # 处理 ObjectId
        if self.id is not None:
            data["_id"] = self.id
        
        return data
    
    @classmethod
    def from_mongo(cls, data: Dict[str, Any]) -> "MongoDocument":
        """
        从 MongoDB 文档创建模型实例
        
        Args:
            data: MongoDB 文档字典
        
        Returns:
            模型实例
        """
        if data is None:
            return None
        
        # 处理 _id 字段
        if "_id" in data:
            data["id"] = data.pop("_id")
        
        return cls(**data)
    
    def update_timestamp(self) -> None:
        """更新时间戳"""
        self.updated_at = datetime.now()
    
    @classmethod
    def get_collection_name(cls) -> str:
        """获取集合名称"""
        if not cls.__collection__:
            raise ValueError(f"模型 {cls.__name__} 未定义 __collection__")
        return cls.__collection__
    
    @classmethod
    def get_indexes(cls) -> List[Dict[str, Any]]:
        """获取索引定义"""
        return cls.__indexes__


class MongoRepository:
    """
    MongoDB 仓储基类
    提供常用的 CRUD 操作
    """
    
    def __init__(self, document_class: type, mongo_client=None):
        """
        初始化仓储
        
        Args:
            document_class: 文档模型类
            mongo_client: MongoDB 客户端（可选，默认使用全局客户端）
        """
        self.document_class = document_class
        self.collection_name = document_class.get_collection_name()
        
        if mongo_client is None:
            from core.mongodb import get_mongo_client
            self._client = get_mongo_client()
        else:
            self._client = mongo_client
    
    @property
    def collection(self):
        """获取集合"""
        return self._client.get_collection(self.collection_name)
    
    def create(self, document: MongoDocument) -> MongoDocument:
        """
        创建文档
        
        Args:
            document: 文档模型实例
        
        Returns:
            创建后的文档（包含 _id）
        """
        document.update_timestamp()
        data = document.to_mongo()
        result = self.collection.insert_one(data)
        document.id = result.inserted_id
        return document
    
    def find_by_id(self, id: ObjectId) -> Optional[MongoDocument]:
        """
        根据 ID 查找文档
        
        Args:
            id: 文档 ID
        
        Returns:
            文档模型实例或 None
        """
        data = self.collection.find_one({"_id": id})
        return self.document_class.from_mongo(data) if data else None
    
    def find_one(self, query: Dict[str, Any]) -> Optional[MongoDocument]:
        """
        查找单个文档
        
        Args:
            query: 查询条件
        
        Returns:
            文档模型实例或 None
        """
        data = self.collection.find_one(query)
        return self.document_class.from_mongo(data) if data else None
    
    def find_many(
        self,
        query: Dict[str, Any],
        skip: int = 0,
        limit: int = 100,
        sort: Optional[List[tuple]] = None
    ) -> List[MongoDocument]:
        """
        查找多个文档
        
        Args:
            query: 查询条件
            skip: 跳过数量
            limit: 返回数量限制
            sort: 排序规则，格式: [("field", 1), ...]
        
        Returns:
            文档模型实例列表
        """
        cursor = self.collection.find(query)
        
        if sort:
            cursor = cursor.sort(sort)
        
        cursor = cursor.skip(skip).limit(limit)
        
        return [self.document_class.from_mongo(doc) for doc in cursor]
    
    def update_by_id(
        self,
        id: ObjectId,
        update_data: Dict[str, Any],
        upsert: bool = False
    ) -> bool:
        """
        根据 ID 更新文档
        
        Args:
            id: 文档 ID
            update_data: 更新数据
            upsert: 如果文档不存在是否创建
        
        Returns:
            是否更新成功
        """
        update_data["updated_at"] = datetime.now()
        result = self.collection.update_one(
            {"_id": id},
            {"$set": update_data},
            upsert=upsert
        )
        return result.modified_count > 0 or result.upserted_id is not None
    
    def update_one(
        self,
        query: Dict[str, Any],
        update_data: Dict[str, Any],
        upsert: bool = False
    ) -> bool:
        """
        更新单个文档
        
        Args:
            query: 查询条件
            update_data: 更新数据
            upsert: 如果文档不存在是否创建
        
        Returns:
            是否更新成功
        """
        update_data["updated_at"] = datetime.now()
        result = self.collection.update_one(
            query,
            {"$set": update_data},
            upsert=upsert
        )
        return result.modified_count > 0 or result.upserted_id is not None
    
    def update_many(
        self,
        query: Dict[str, Any],
        update_data: Dict[str, Any]
    ) -> int:
        """
        更新多个文档
        
        Args:
            query: 查询条件
            update_data: 更新数据
        
        Returns:
            更新的文档数量
        """
        update_data["updated_at"] = datetime.now()
        result = self.collection.update_many(
            query,
            {"$set": update_data}
        )
        return result.modified_count
    
    def delete_by_id(self, id: ObjectId) -> bool:
        """
        根据 ID 删除文档
        
        Args:
            id: 文档 ID
        
        Returns:
            是否删除成功
        """
        result = self.collection.delete_one({"_id": id})
        return result.deleted_count > 0
    
    def delete_one(self, query: Dict[str, Any]) -> bool:
        """
        删除单个文档
        
        Args:
            query: 查询条件
        
        Returns:
            是否删除成功
        """
        result = self.collection.delete_one(query)
        return result.deleted_count > 0
    
    def delete_many(self, query: Dict[str, Any]) -> int:
        """
        删除多个文档
        
        Args:
            query: 查询条件
        
        Returns:
            删除的文档数量
        """
        result = self.collection.delete_many(query)
        return result.deleted_count
    
    def count(self, query: Dict[str, Any] = None) -> int:
        """
        统计文档数量
        
        Args:
            query: 查询条件
        
        Returns:
            文档数量
        """
        if query is None:
            query = {}
        return self.collection.count_documents(query)
    
    def exists(self, query: Dict[str, Any]) -> bool:
        """
        检查文档是否存在
        
        Args:
            query: 查询条件
        
        Returns:
            是否存在
        """
        return self.collection.count_documents(query, limit=1) > 0
    
    def create_indexes(self) -> None:
        """创建索引"""
        indexes = self.document_class.get_indexes()
        if indexes:
            from core.mongodb import get_mongo_client
            client = get_mongo_client()
            client.create_indexes(self.collection_name, indexes)


class AsyncMongoRepository:
    """
    MongoDB 异步仓储基类
    提供异步的 CRUD 操作，适用于 FastAPI
    """
    
    def __init__(self, document_class: type, async_mongo_client=None):
        """
        初始化异步仓储
        
        Args:
            document_class: 文档模型类
            async_mongo_client: 异步 MongoDB 客户端（可选，默认使用全局客户端）
        """
        self.document_class = document_class
        self.collection_name = document_class.get_collection_name()
        
        if async_mongo_client is None:
            from core.mongodb import get_async_mongo_client
            self._client = get_async_mongo_client()
        else:
            self._client = async_mongo_client
    
    @property
    def collection(self):
        """获取异步集合"""
        return self._client.get_collection(self.collection_name)
    
    async def create(self, document: MongoDocument) -> MongoDocument:
        """创建文档"""
        document.update_timestamp()
        data = document.to_mongo()
        result = await self.collection.insert_one(data)
        document.id = result.inserted_id
        return document
    
    async def find_by_id(self, id: ObjectId) -> Optional[MongoDocument]:
        """根据 ID 查找文档"""
        data = await self.collection.find_one({"_id": id})
        return self.document_class.from_mongo(data) if data else None
    
    async def find_one(self, query: Dict[str, Any]) -> Optional[MongoDocument]:
        """查找单个文档"""
        data = await self.collection.find_one(query)
        return self.document_class.from_mongo(data) if data else None
    
    async def find_many(
        self,
        query: Dict[str, Any],
        skip: int = 0,
        limit: int = 100,
        sort: Optional[List[tuple]] = None
    ) -> List[MongoDocument]:
        """查找多个文档"""
        cursor = self.collection.find(query)
        
        if sort:
            cursor = cursor.sort(sort)
        
        cursor = cursor.skip(skip).limit(limit)
        
        documents = []
        async for doc in cursor:
            documents.append(self.document_class.from_mongo(doc))
        
        return documents
    
    async def update_by_id(
        self,
        id: ObjectId,
        update_data: Dict[str, Any],
        upsert: bool = False
    ) -> bool:
        """根据 ID 更新文档"""
        update_data["updated_at"] = datetime.now()
        result = await self.collection.update_one(
            {"_id": id},
            {"$set": update_data},
            upsert=upsert
        )
        return result.modified_count > 0 or result.upserted_id is not None
    
    async def update_one(
        self,
        query: Dict[str, Any],
        update_data: Dict[str, Any],
        upsert: bool = False
    ) -> bool:
        """更新单个文档"""
        update_data["updated_at"] = datetime.now()
        result = await self.collection.update_one(
            query,
            {"$set": update_data},
            upsert=upsert
        )
        return result.modified_count > 0 or result.upserted_id is not None
    
    async def update_many(
        self,
        query: Dict[str, Any],
        update_data: Dict[str, Any]
    ) -> int:
        """更新多个文档"""
        update_data["updated_at"] = datetime.now()
        result = await self.collection.update_many(
            query,
            {"$set": update_data}
        )
        return result.modified_count
    
    async def delete_by_id(self, id: ObjectId) -> bool:
        """根据 ID 删除文档"""
        result = await self.collection.delete_one({"_id": id})
        return result.deleted_count > 0
    
    async def delete_one(self, query: Dict[str, Any]) -> bool:
        """删除单个文档"""
        result = await self.collection.delete_one(query)
        return result.deleted_count > 0
    
    async def delete_many(self, query: Dict[str, Any]) -> int:
        """删除多个文档"""
        result = await self.collection.delete_many(query)
        return result.deleted_count
    
    async def count(self, query: Dict[str, Any] = None) -> int:
        """统计文档数量"""
        if query is None:
            query = {}
        return await self.collection.count_documents(query)
    
    async def exists(self, query: Dict[str, Any]) -> bool:
        """检查文档是否存在"""
        count = await self.collection.count_documents(query, limit=1)
        return count > 0
