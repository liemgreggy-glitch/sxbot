"""
Telegram 用户采集模块
支持从多种渠道采集目标用户
"""

import enum
import asyncio
import logging
from datetime import datetime
from bson import ObjectId
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ConversationHandler
from telethon import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest, GetRepliesRequest
from telethon.tl.types import InputPeerEmpty, PeerChannel, PeerUser
from telethon.errors import (
    FloodWaitError, ChatAdminRequiredError, ChannelPrivateError,
    UsernameNotOccupiedError, UsernameInvalidError
)
import re

logger = logging.getLogger(__name__)


# ============================================================================
# 模块级变量
# ============================================================================
_db = None
_collection_manager = None


def init_db(database):
    """初始化数据库实例"""
    global _db
    _db = database


def init_collection_manager(manager):
    """初始化采集管理器实例"""
    global _collection_manager
    _collection_manager = manager


def _get_db():
    """获取数据库实例，如果未初始化则抛出异常"""
    if _db is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")
    return _db


def _get_collection_manager():
    """获取采集管理器实例，如果未初始化则抛出异常"""
    if _collection_manager is None:
        raise RuntimeError("Collection manager not initialized. Call init_collection_manager() first.")
    return _collection_manager


# ============================================================================
# 常量
# ============================================================================
# Telegram username pattern (5-32 characters, alphanumeric and underscore)
USERNAME_PATTERN = r'[a-zA-Z0-9_]{5,32}'


# ============================================================================
# 枚举类型
# ============================================================================
class CollectionType(enum.Enum):
    """采集类型"""
    PUBLIC_GROUP = "public_group"  # 公开群组采集
    PRIVATE_GROUP = "private_group"  # 私有群组采集
    CHANNEL_POST = "channel_post"  # 频道帖子采集
    CHANNEL_COMMENT = "channel_comment"  # 频道评论采集
    KEYWORD_SEARCH = "keyword_search"  # 关键词搜索


class CollectionStatus(enum.Enum):
    """采集状态"""
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"


# ============================================================================
# 数据库模型
# ============================================================================
class Collection:
    """采集任务模型"""
    COLLECTION_NAME = 'collections'
    
    def __init__(self, name, collection_type, status=None, account_id=None,
                 target_link=None, keyword=None, filters=None,
                 collected_users=0, collected_groups=0, 
                 created_at=None, started_at=None, completed_at=None,
                 updated_at=None, error_message=None, _id=None):
        self._id = _id
        self.name = name
        self.collection_type = collection_type
        self.status = status or CollectionStatus.PENDING.value
        self.account_id = account_id
        self.target_link = target_link
        self.keyword = keyword
        self.filters = filters or {}
        self.collected_users = collected_users
        self.collected_groups = collected_groups
        self.created_at = created_at or datetime.utcnow()
        self.started_at = started_at
        self.completed_at = completed_at
        self.updated_at = updated_at or datetime.utcnow()
        self.error_message = error_message
    
    def to_dict(self):
        """转换为字典"""
        doc = {
            'name': self.name,
            'collection_type': self.collection_type,
            'status': self.status,
            'account_id': self.account_id,
            'target_link': self.target_link,
            'keyword': self.keyword,
            'filters': self.filters,
            'collected_users': self.collected_users,
            'collected_groups': self.collected_groups,
            'created_at': self.created_at,
            'started_at': self.started_at,
            'completed_at': self.completed_at,
            'updated_at': self.updated_at,
            'error_message': self.error_message
        }
        if self._id:
            doc['_id'] = self._id
        return doc
    
    @classmethod
    def from_dict(cls, doc):
        """从字典创建实例"""
        if not doc:
            return None
        return cls(
            name=doc.get('name'),
            collection_type=doc.get('collection_type'),
            status=doc.get('status'),
            account_id=doc.get('account_id'),
            target_link=doc.get('target_link'),
            keyword=doc.get('keyword'),
            filters=doc.get('filters'),
            collected_users=doc.get('collected_users', 0),
            collected_groups=doc.get('collected_groups', 0),
            created_at=doc.get('created_at'),
            started_at=doc.get('started_at'),
            completed_at=doc.get('completed_at'),
            updated_at=doc.get('updated_at'),
            error_message=doc.get('error_message'),
            _id=doc.get('_id')
        )


class CollectedUser:
    """采集用户模型"""
    COLLECTION_NAME = 'collected_users'
    
    def __init__(self, collection_id, user_id=None, username=None, 
                 first_name=None, last_name=None, phone=None,
                 is_premium=False, is_admin=False, has_photo=False,
                 last_seen=None, created_at=None, _id=None):
        self._id = _id
        self.collection_id = collection_id
        self.user_id = user_id
        self.username = username
        self.first_name = first_name
        self.last_name = last_name
        self.phone = phone
        self.is_premium = is_premium
        self.is_admin = is_admin
        self.has_photo = has_photo
        self.last_seen = last_seen
        self.created_at = created_at or datetime.utcnow()
    
    def to_dict(self):
        """转换为字典"""
        doc = {
            'collection_id': self.collection_id,
            'user_id': self.user_id,
            'username': self.username,
            'first_name': self.first_name,
            'last_name': self.last_name,
            'phone': self.phone,
            'is_premium': self.is_premium,
            'is_admin': self.is_admin,
            'has_photo': self.has_photo,
            'last_seen': self.last_seen,
            'created_at': self.created_at
        }
        if self._id:
            doc['_id'] = self._id
        return doc
    
    @classmethod
    def from_dict(cls, doc):
        """从字典创建实例"""
        if not doc:
            return None
        return cls(
            collection_id=doc.get('collection_id'),
            user_id=doc.get('user_id'),
            username=doc.get('username'),
            first_name=doc.get('first_name'),
            last_name=doc.get('last_name'),
            phone=doc.get('phone'),
            is_premium=doc.get('is_premium', False),
            is_admin=doc.get('is_admin', False),
            has_photo=doc.get('has_photo', False),
            last_seen=doc.get('last_seen'),
            created_at=doc.get('created_at'),
            _id=doc.get('_id')
        )


class CollectedGroup:
    """采集群组模型"""
    COLLECTION_NAME = 'collected_groups'
    
    def __init__(self, collection_id, group_id=None, title=None, username=None,
                 link=None, member_count=0, is_public=True, description=None,
                 created_at=None, _id=None):
        self._id = _id
        self.collection_id = collection_id
        self.group_id = group_id
        self.title = title
        self.username = username
        self.link = link
        self.member_count = member_count
        self.is_public = is_public
        self.description = description
        self.created_at = created_at or datetime.utcnow()
    
    def to_dict(self):
        """转换为字典"""
        doc = {
            'collection_id': self.collection_id,
            'group_id': self.group_id,
            'title': self.title,
            'username': self.username,
            'link': self.link,
            'member_count': self.member_count,
            'is_public': self.is_public,
            'description': self.description,
            'created_at': self.created_at
        }
        if self._id:
            doc['_id'] = self._id
        return doc
    
    @classmethod
    def from_dict(cls, doc):
        """从字典创建实例"""
        if not doc:
            return None
        return cls(
            collection_id=doc.get('collection_id'),
            group_id=doc.get('group_id'),
            title=doc.get('title'),
            username=doc.get('username'),
            link=doc.get('link'),
            member_count=doc.get('member_count', 0),
            is_public=doc.get('is_public', True),
            description=doc.get('description'),
            created_at=doc.get('created_at'),
            _id=doc.get('_id')
        )


# ============================================================================
# 采集管理器
# ============================================================================
class CollectionManager:
    """采集管理器"""
    
    def __init__(self, db, account_manager):
        self.db = db
        self.account_manager = account_manager
        self.running_collections = {}  # {collection_id: task}
        self.stop_flags = {}  # {collection_id: bool}
        logger.info("CollectionManager initialized")
    
    async def create_collection(self, name, collection_type, account_id, 
                               target_link=None, keyword=None, filters=None):
        """创建采集任务"""
        collection = Collection(
            name=name,
            collection_type=collection_type,
            account_id=account_id,
            target_link=target_link,
            keyword=keyword,
            filters=filters
        )
        
        result = self.db[Collection.COLLECTION_NAME].insert_one(collection.to_dict())
        collection._id = result.inserted_id
        
        logger.info(f"Created collection {collection._id}: {name}")
        return collection
    
    async def start_collection(self, collection_id):
        """开始采集任务"""
        collection_doc = self.db[Collection.COLLECTION_NAME].find_one({'_id': ObjectId(collection_id)})
        if not collection_doc:
            raise ValueError("采集任务不存在")
        
        collection = Collection.from_dict(collection_doc)
        
        if collection.status == CollectionStatus.RUNNING.value:
            raise ValueError("采集任务已在运行中")
        
        # 更新状态
        self.db[Collection.COLLECTION_NAME].update_one(
            {'_id': ObjectId(collection_id)},
            {'$set': {
                'status': CollectionStatus.RUNNING.value,
                'started_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            }}
        )
        
        # 创建采集任务
        self.stop_flags[str(collection_id)] = False
        task = asyncio.create_task(self._run_collection(collection))
        self.running_collections[str(collection_id)] = task
        
        logger.info(f"Started collection {collection_id}")
        return True
    
    async def stop_collection(self, collection_id):
        """停止采集任务"""
        collection_id_str = str(collection_id)
        
        if collection_id_str not in self.running_collections:
            # 如果不在运行中，直接更新状态
            self.db[Collection.COLLECTION_NAME].update_one(
                {'_id': ObjectId(collection_id)},
                {'$set': {
                    'status': CollectionStatus.PAUSED.value,
                    'updated_at': datetime.utcnow()
                }}
            )
            return True
        
        # 设置停止标志
        self.stop_flags[collection_id_str] = True
        
        # 等待任务完成
        task = self.running_collections.get(collection_id_str)
        if task:
            try:
                await asyncio.wait_for(task, timeout=10.0)
            except asyncio.TimeoutError:
                task.cancel()
        
        # 清理
        if collection_id_str in self.running_collections:
            del self.running_collections[collection_id_str]
        if collection_id_str in self.stop_flags:
            del self.stop_flags[collection_id_str]
        
        # 更新状态
        self.db[Collection.COLLECTION_NAME].update_one(
            {'_id': ObjectId(collection_id)},
            {'$set': {
                'status': CollectionStatus.PAUSED.value,
                'updated_at': datetime.utcnow()
            }}
        )
        
        logger.info(f"Stopped collection {collection_id}")
        return True
    
    async def _run_collection(self, collection):
        """运行采集任务"""
        collection_id_str = str(collection._id)
        
        try:
            # 获取账户客户端
            client = await self.account_manager.get_client(collection.account_id)
            
            if collection.collection_type == CollectionType.PUBLIC_GROUP.value:
                await self._collect_public_group(client, collection)
            elif collection.collection_type == CollectionType.PRIVATE_GROUP.value:
                await self._collect_private_group(client, collection)
            elif collection.collection_type == CollectionType.CHANNEL_POST.value:
                await self._collect_channel_post(client, collection)
            elif collection.collection_type == CollectionType.CHANNEL_COMMENT.value:
                await self._collect_channel_comment(client, collection)
            elif collection.collection_type == CollectionType.KEYWORD_SEARCH.value:
                await self._collect_keyword_search(client, collection)
            
            # 更新状态为完成
            if not self.stop_flags.get(collection_id_str, False):
                self.db[Collection.COLLECTION_NAME].update_one(
                    {'_id': collection._id},
                    {'$set': {
                        'status': CollectionStatus.COMPLETED.value,
                        'completed_at': datetime.utcnow(),
                        'updated_at': datetime.utcnow()
                    }}
                )
                logger.info(f"Collection {collection._id} completed successfully")
        
        except Exception as e:
            logger.error(f"Collection {collection._id} failed: {e}")
            self.db[Collection.COLLECTION_NAME].update_one(
                {'_id': collection._id},
                {'$set': {
                    'status': CollectionStatus.FAILED.value,
                    'error_message': str(e),
                    'updated_at': datetime.utcnow()
                }}
            )
        
        finally:
            # 清理
            if collection_id_str in self.running_collections:
                del self.running_collections[collection_id_str]
            if collection_id_str in self.stop_flags:
                del self.stop_flags[collection_id_str]
    
    async def _collect_public_group(self, client, collection):
        """采集公开群组成员"""
        collection_id_str = str(collection._id)
        
        try:
            # 解析群组链接
            group_entity = await client.get_entity(collection.target_link)
            
            # 获取所有成员
            filters = collection.filters or {}
            collected_count = 0
            
            async for user in client.iter_participants(group_entity, aggressive=True):
                # 检查停止标志
                if self.stop_flags.get(collection_id_str, False):
                    logger.info(f"Collection {collection._id} stopped by user")
                    break
                
                # 应用过滤器
                if not self._apply_user_filters(user, filters):
                    continue
                
                # 保存用户
                await self._save_collected_user(collection._id, user, group_entity)
                collected_count += 1
                
                # 更新进度
                if collected_count % 10 == 0:
                    self.db[Collection.COLLECTION_NAME].update_one(
                        {'_id': collection._id},
                        {'$set': {
                            'collected_users': collected_count,
                            'updated_at': datetime.utcnow()
                        }}
                    )
                
                # 防止频率限制
                await asyncio.sleep(0.1)
            
            # 最终更新
            self.db[Collection.COLLECTION_NAME].update_one(
                {'_id': collection._id},
                {'$set': {
                    'collected_users': collected_count,
                    'updated_at': datetime.utcnow()
                }}
            )
            
            logger.info(f"Collected {collected_count} users from public group")
        
        except FloodWaitError as e:
            logger.warning(f"FloodWait: need to wait {e.seconds} seconds")
            raise
        except (ChatAdminRequiredError, ChannelPrivateError) as e:
            logger.error(f"Permission error: {e}")
            raise
        except Exception as e:
            logger.error(f"Error collecting public group: {e}")
            raise
    
    async def _collect_private_group(self, client, collection):
        """采集私有群组活跃用户（从消息历史）"""
        collection_id_str = str(collection._id)
        
        try:
            # 解析群组链接
            group_entity = await client.get_entity(collection.target_link)
            
            # 获取消息历史
            filters = collection.filters or {}
            min_id = filters.get('min_message_id', 0)
            max_id = filters.get('max_message_id', 0)
            limit = filters.get('message_limit', 1000)
            
            collected_users = set()
            collected_count = 0
            
            async for message in client.iter_messages(group_entity, limit=limit, 
                                                     min_id=min_id, max_id=max_id if max_id > 0 else None):
                # 检查停止标志
                if self.stop_flags.get(collection_id_str, False):
                    logger.info(f"Collection {collection._id} stopped by user")
                    break
                
                # 获取消息发送者
                if message.sender and hasattr(message.sender, 'id'):
                    user_id = message.sender.id
                    
                    # 避免重复
                    if user_id in collected_users:
                        continue
                    
                    # 应用过滤器
                    if not self._apply_user_filters(message.sender, filters):
                        continue
                    
                    # 保存用户
                    await self._save_collected_user(collection._id, message.sender, group_entity)
                    collected_users.add(user_id)
                    collected_count += 1
                    
                    # 更新进度
                    if collected_count % 10 == 0:
                        self.db[Collection.COLLECTION_NAME].update_one(
                            {'_id': collection._id},
                            {'$set': {
                                'collected_users': collected_count,
                                'updated_at': datetime.utcnow()
                            }}
                        )
                
                await asyncio.sleep(0.05)
            
            # 最终更新
            self.db[Collection.COLLECTION_NAME].update_one(
                {'_id': collection._id},
                {'$set': {
                    'collected_users': collected_count,
                    'updated_at': datetime.utcnow()
                }}
            )
            
            logger.info(f"Collected {collected_count} active users from private group")
        
        except Exception as e:
            logger.error(f"Error collecting private group: {e}")
            raise
    
    async def _collect_channel_post(self, client, collection):
        """采集频道帖子中的用户名和链接"""
        collection_id_str = str(collection._id)
        
        try:
            # 解析频道链接
            channel_entity = await client.get_entity(collection.target_link)
            
            # 获取帖子
            filters = collection.filters or {}
            limit = filters.get('post_limit', 100)
            
            collected_usernames = set()
            collected_count = 0
            
            async for message in client.iter_messages(channel_entity, limit=limit):
                # 检查停止标志
                if self.stop_flags.get(collection_id_str, False):
                    logger.info(f"Collection {collection._id} stopped by user")
                    break
                
                # 提取用户名和链接
                if message.text:
                    # 提取 @username
                    usernames = re.findall(f'@({USERNAME_PATTERN})', message.text)
                    # 提取 t.me/username
                    telegram_links = re.findall(rf't\.me/({USERNAME_PATTERN})', message.text)
                    
                    all_usernames = set(usernames + telegram_links)
                    
                    for username in all_usernames:
                        if username in collected_usernames:
                            continue
                        
                        try:
                            # 尝试获取用户信息
                            user = await client.get_entity(username)
                            
                            # 应用过滤器
                            if not self._apply_user_filters(user, filters):
                                continue
                            
                            # 保存用户
                            await self._save_collected_user(collection._id, user, None)
                            collected_usernames.add(username)
                            collected_count += 1
                            
                            # 更新进度
                            if collected_count % 5 == 0:
                                self.db[Collection.COLLECTION_NAME].update_one(
                                    {'_id': collection._id},
                                    {'$set': {
                                        'collected_users': collected_count,
                                        'updated_at': datetime.utcnow()
                                    }}
                                )
                            
                            await asyncio.sleep(0.2)
                        
                        except (UsernameNotOccupiedError, UsernameInvalidError):
                            continue
                        except Exception as e:
                            logger.warning(f"Error getting user {username}: {e}")
                            continue
                
                await asyncio.sleep(0.1)
            
            # 最终更新
            self.db[Collection.COLLECTION_NAME].update_one(
                {'_id': collection._id},
                {'$set': {
                    'collected_users': collected_count,
                    'updated_at': datetime.utcnow()
                }}
            )
            
            logger.info(f"Collected {collected_count} users from channel posts")
        
        except Exception as e:
            logger.error(f"Error collecting channel posts: {e}")
            raise
    
    async def _collect_channel_comment(self, client, collection):
        """采集频道评论区用户"""
        collection_id_str = str(collection._id)
        
        try:
            # 解析频道链接
            channel_entity = await client.get_entity(collection.target_link)
            
            # 获取帖子
            filters = collection.filters or {}
            post_limit = filters.get('post_limit', 50)
            
            collected_users = set()
            collected_count = 0
            
            async for message in client.iter_messages(channel_entity, limit=post_limit):
                # 检查停止标志
                if self.stop_flags.get(collection_id_str, False):
                    logger.info(f"Collection {collection._id} stopped by user")
                    break
                
                # 检查是否有评论
                if not message.replies or message.replies.replies == 0:
                    continue
                
                try:
                    # 获取评论
                    async for reply_message in client.iter_messages(
                        channel_entity,
                        reply_to=message.id,
                        limit=100
                    ):
                        if reply_message.sender and hasattr(reply_message.sender, 'id'):
                            user_id = reply_message.sender.id
                            
                            # 避免重复
                            if user_id in collected_users:
                                continue
                            
                            # 应用过滤器
                            if not self._apply_user_filters(reply_message.sender, filters):
                                continue
                            
                            # 保存用户
                            await self._save_collected_user(collection._id, reply_message.sender, channel_entity)
                            collected_users.add(user_id)
                            collected_count += 1
                            
                            # 更新进度
                            if collected_count % 10 == 0:
                                self.db[Collection.COLLECTION_NAME].update_one(
                                    {'_id': collection._id},
                                    {'$set': {
                                        'collected_users': collected_count,
                                        'updated_at': datetime.utcnow()
                                    }}
                                )
                        
                        await asyncio.sleep(0.1)
                
                except Exception as e:
                    logger.warning(f"Error getting replies for message {message.id}: {e}")
                    continue
                
                await asyncio.sleep(0.2)
            
            # 最终更新
            self.db[Collection.COLLECTION_NAME].update_one(
                {'_id': collection._id},
                {'$set': {
                    'collected_users': collected_count,
                    'updated_at': datetime.utcnow()
                }}
            )
            
            logger.info(f"Collected {collected_count} users from channel comments")
        
        except Exception as e:
            logger.error(f"Error collecting channel comments: {e}")
            raise
    
    async def _collect_keyword_search(self, client, collection):
        """关键词搜索群组/频道"""
        collection_id_str = str(collection._id)
        
        try:
            keyword = collection.keyword
            filters = collection.filters or {}
            limit = filters.get('search_limit', 50)
            
            collected_count = 0
            
            # 搜索公开群组/频道
            async for dialog in client.iter_dialogs(limit=None):
                # 检查停止标志
                if self.stop_flags.get(collection_id_str, False):
                    logger.info(f"Collection {collection._id} stopped by user")
                    break
                
                # 检查是否匹配关键词
                if not dialog.is_channel and not dialog.is_group:
                    continue
                
                if keyword.lower() not in dialog.title.lower():
                    continue
                
                # 保存群组/频道
                await self._save_collected_group(collection._id, dialog)
                collected_count += 1
                
                # 更新进度
                if collected_count % 5 == 0:
                    self.db[Collection.COLLECTION_NAME].update_one(
                        {'_id': collection._id},
                        {'$set': {
                            'collected_groups': collected_count,
                            'updated_at': datetime.utcnow()
                        }}
                    )
                
                # 达到限制
                if collected_count >= limit:
                    break
                
                await asyncio.sleep(0.2)
            
            # 最终更新
            self.db[Collection.COLLECTION_NAME].update_one(
                {'_id': collection._id},
                {'$set': {
                    'collected_groups': collected_count,
                    'updated_at': datetime.utcnow()
                }}
            )
            
            logger.info(f"Found {collected_count} groups/channels matching keyword")
        
        except Exception as e:
            logger.error(f"Error in keyword search: {e}")
            raise
    
    def _apply_user_filters(self, user, filters):
        """应用用户过滤器"""
        # 检查是否为机器人（始终过滤）
        if hasattr(user, 'bot') and user.bot:
            return False
        
        if not filters:
            return True
        
        # 过滤管理员
        if filters.get('exclude_admin', False):
            if hasattr(user, 'participant') and hasattr(user.participant, 'admin_rights'):
                if user.participant.admin_rights:
                    return False
        
        # 只采集高级会员
        if filters.get('premium_only', False):
            if not (hasattr(user, 'premium') and user.premium):
                return False
        
        # 必须有头像
        if filters.get('has_photo', False):
            if not (hasattr(user, 'photo') and user.photo):
                return False
        
        # 必须有用户名
        if filters.get('has_username', False):
            if not (hasattr(user, 'username') and user.username):
                return False
        
        return True
    
    async def _save_collected_user(self, collection_id, user, source_entity):
        """保存采集的用户"""
        try:
            # 检查是否已存在
            existing = self.db[CollectedUser.COLLECTION_NAME].find_one({
                'collection_id': collection_id,
                'user_id': user.id
            })
            
            if existing:
                return
            
            # 检查是否为管理员
            is_admin = False
            if source_entity and hasattr(user, 'participant'):
                is_admin = hasattr(user.participant, 'admin_rights') and user.participant.admin_rights is not None
            
            # 创建用户记录
            collected_user = CollectedUser(
                collection_id=collection_id,
                user_id=user.id,
                username=getattr(user, 'username', None),
                first_name=getattr(user, 'first_name', None),
                last_name=getattr(user, 'last_name', None),
                phone=getattr(user, 'phone', None),
                is_premium=getattr(user, 'premium', False),
                is_admin=is_admin,
                has_photo=bool(getattr(user, 'photo', None)),
                last_seen=getattr(user.status, 'was_online', None) if hasattr(user, 'status') else None
            )
            
            # 保存到数据库
            self.db[CollectedUser.COLLECTION_NAME].insert_one(collected_user.to_dict())
            
        except Exception as e:
            logger.error(f"Error saving collected user: {e}")
    
    async def _save_collected_group(self, collection_id, dialog):
        """保存采集的群组/频道"""
        try:
            entity = dialog.entity
            
            # 检查是否已存在
            existing = self.db[CollectedGroup.COLLECTION_NAME].find_one({
                'collection_id': collection_id,
                'group_id': entity.id
            })
            
            if existing:
                return
            
            # 构建链接
            link = None
            if hasattr(entity, 'username') and entity.username:
                link = f"https://t.me/{entity.username}"
            
            # 创建群组记录
            collected_group = CollectedGroup(
                collection_id=collection_id,
                group_id=entity.id,
                title=getattr(entity, 'title', None),
                username=getattr(entity, 'username', None),
                link=link,
                member_count=getattr(entity, 'participants_count', 0),
                is_public=bool(getattr(entity, 'username', None)),
                description=getattr(entity, 'about', None) if hasattr(entity, 'about') else None
            )
            
            # 保存到数据库
            self.db[CollectedGroup.COLLECTION_NAME].insert_one(collected_group.to_dict())
            
        except Exception as e:
            logger.error(f"Error saving collected group: {e}")
    
    def get_collection(self, collection_id):
        """获取采集任务"""
        doc = self.db[Collection.COLLECTION_NAME].find_one({'_id': ObjectId(collection_id)})
        return Collection.from_dict(doc)
    
    def list_collections(self, limit=20, skip=0):
        """列出采集任务"""
        docs = self.db[Collection.COLLECTION_NAME].find().sort('created_at', -1).skip(skip).limit(limit)
        return [Collection.from_dict(doc) for doc in docs]
    
    def delete_collection(self, collection_id):
        """删除采集任务及其数据"""
        # 删除采集的用户
        self.db[CollectedUser.COLLECTION_NAME].delete_many({'collection_id': ObjectId(collection_id)})
        # 删除采集的群组
        self.db[CollectedGroup.COLLECTION_NAME].delete_many({'collection_id': ObjectId(collection_id)})
        # 删除采集任务
        self.db[Collection.COLLECTION_NAME].delete_one({'_id': ObjectId(collection_id)})
        logger.info(f"Deleted collection {collection_id}")
    
    async def export_collected_users(self, collection_id):
        """导出采集的用户列表"""
        users = list(self.db[CollectedUser.COLLECTION_NAME].find({'collection_id': ObjectId(collection_id)}))
        
        result = []
        for user_doc in users:
            user = CollectedUser.from_dict(user_doc)
            tags = []
            if user.is_premium:
                tags.append('Premium')
            if user.is_admin:
                tags.append('Admin')
            if user.has_photo:
                tags.append('HasPhoto')
            
            result.append({
                'user_id': user.user_id,
                'username': user.username or '',
                'first_name': user.first_name or '',
                'last_name': user.last_name or '',
                'tags': ','.join(tags)
            })
        
        return result
    
    async def export_collected_groups(self, collection_id):
        """导出采集的群组列表"""
        groups = list(self.db[CollectedGroup.COLLECTION_NAME].find({'collection_id': ObjectId(collection_id)}))
        
        result = []
        for group_doc in groups:
            group = CollectedGroup.from_dict(group_doc)
            result.append({
                'group_id': group.group_id,
                'title': group.title or '',
                'username': group.username or '',
                'link': group.link or '',
                'member_count': group.member_count,
                'is_public': 'Yes' if group.is_public else 'No'
            })
        
        return result


# ============================================================================
# 数据库索引初始化
# ============================================================================
def init_collection_indexes(db):
    """初始化采集相关的数据库索引"""
    # Collection索引
    db[Collection.COLLECTION_NAME].create_index('status')
    db[Collection.COLLECTION_NAME].create_index('account_id')
    db[Collection.COLLECTION_NAME].create_index('collection_type')
    db[Collection.COLLECTION_NAME].create_index('created_at')
    
    # CollectedUser索引
    db[CollectedUser.COLLECTION_NAME].create_index('collection_id')
    db[CollectedUser.COLLECTION_NAME].create_index('user_id')
    db[CollectedUser.COLLECTION_NAME].create_index([('collection_id', 1), ('user_id', 1)], unique=True)
    
    # CollectedGroup索引
    db[CollectedGroup.COLLECTION_NAME].create_index('collection_id')
    db[CollectedGroup.COLLECTION_NAME].create_index('group_id')
    db[CollectedGroup.COLLECTION_NAME].create_index([('collection_id', 1), ('group_id', 1)], unique=True)
    
    logger.info("Collection indexes created")


# ============================================================================
# 会话状态常量
# ============================================================================
COLLECTION_NAME_INPUT = 0
COLLECTION_TYPE_SELECT = 1
COLLECTION_ACCOUNT_SELECT = 2
COLLECTION_TARGET_INPUT = 3
COLLECTION_KEYWORD_INPUT = 4
COLLECTION_FILTER_CONFIG = 5


# ============================================================================
# UI 界面函数
# ============================================================================
async def show_collection_menu(query):
    """显示采集菜单"""
    from bot import Account, AccountStatus
    
    # Use module-level _db
    db = _get_db()
    
    # 统计采集任务
    total_collections = db[Collection.COLLECTION_NAME].count_documents({})
    running_collections = db[Collection.COLLECTION_NAME].count_documents({'status': CollectionStatus.RUNNING.value})
    completed_collections = db[Collection.COLLECTION_NAME].count_documents({'status': CollectionStatus.COMPLETED.value})
    
    # 统计采集账户（只统计 collection 类型）
    total_accounts = db[Account.COLLECTION_NAME].count_documents({
        'account_type': 'collection',
        'session_name': {'$regex': r'\.(session|session\+json)$'}
    })
    active_accounts = db[Account.COLLECTION_NAME].count_documents({
        'status': AccountStatus.ACTIVE.value,
        'account_type': 'collection',
        'session_name': {'$regex': r'\.(session|session\+json)$'}
    })
    
    text = (
        "👥 <b>用户采集</b>\n\n"
        f"📊 采集任务: {total_collections}\n"
        f"🔄 运行中: {running_collections}\n"
        f"✅ 已完成: {completed_collections}\n\n"
        f"📱 采集账户: {active_accounts}/{total_accounts}\n\n"
        "选择操作："
    )
    
    keyboard = [
        [InlineKeyboardButton("📱 账户管理", callback_data='collection_accounts_menu')],
        [InlineKeyboardButton("📋 采集列表", callback_data='collection_list')],
        [InlineKeyboardButton("➕ 创建采集", callback_data='collection_create')],
        [InlineKeyboardButton("🔙 返回主菜单", callback_data='back_main')]
    ]
    
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')


async def show_collection_accounts_menu(query):
    """显示采集账户管理菜单"""
    db = _get_db()
    from bot import Account, AccountStatus
    
    # 统计采集账户
    total_accounts = db[Account.COLLECTION_NAME].count_documents({
        'account_type': 'collection',
        'session_name': {'$regex': r'\.(session|session\+json)$'}
    })
    active_accounts = db[Account.COLLECTION_NAME].count_documents({
        'status': AccountStatus.ACTIVE.value,
        'account_type': 'collection',
        'session_name': {'$regex': r'\.(session|session\+json)$'}
    })
    
    text = (
        "📱 <b>采集账户管理</b>\n\n"
        f"当前状态：可用 {active_accounts}/{total_accounts} 个账号\n\n"
        f"请选择操作："
    )
    
    keyboard = [
        [InlineKeyboardButton("📋 账号列表", callback_data='collection_accounts_list')],
        [InlineKeyboardButton("➕ 添加账号", callback_data='collection_accounts_add')],
        [InlineKeyboardButton("🔙 返回", callback_data='menu_collection')]
    ]
    
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')


async def list_collection_accounts(query):
    """显示采集账户列表"""
    db = _get_db()
    from bot import Account, AccountStatus
    
    # 只查询 collection 类型的账户
    account_docs = db[Account.COLLECTION_NAME].find({'account_type': 'collection'})
    accounts = [Account.from_dict(doc) for doc in account_docs]
    
    if not accounts:
        text = "📱 <b>采集账户列表</b>\n\n暂无采集账户"
        keyboard = [
            [InlineKeyboardButton("➕ 添加账户", callback_data='collection_accounts_add')],
            [InlineKeyboardButton("🔙 返回", callback_data='collection_accounts_menu')]
        ]
    else:
        text = f"📱 <b>采集账户列表</b>\n\n共 {len(accounts)} 个采集账户：\n\n"
        keyboard = []
        
        for account in accounts:
            status_emoji = {'active': '✅', 'banned': '🚫', 'limited': '⚠️', 'inactive': '❌'}.get(account.status, '❓')
            text += (
                f"{status_emoji} <b>{account.phone}</b>\n"
                f"   状态: {account.status}\n"
                f"   格式: {account.session_name.split('.')[-1]}\n\n"
            )
        
        keyboard.append([InlineKeyboardButton("🔙 返回", callback_data='collection_accounts_menu')])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='HTML')


async def show_collection_list(query, page=0):
    """显示采集任务列表"""
    # Use module-level _db
    db = _get_db()
    limit = 5
    skip = page * limit
    
    collections = list(db[Collection.COLLECTION_NAME].find().sort('created_at', -1).skip(skip).limit(limit))
    total = db[Collection.COLLECTION_NAME].count_documents({})
    
    if not collections:
        text = "📋 <b>采集列表</b>\n\n暂无采集任务"
        keyboard = [
            [InlineKeyboardButton("➕ 创建采集", callback_data='collection_create')],
            [InlineKeyboardButton("🔙 返回", callback_data='menu_collection')]
        ]
    else:
        text = f"📋 <b>采集列表</b> (第 {page + 1} 页，共 {(total + limit - 1) // limit} 页)\n\n"
        keyboard = []
        
        for coll_doc in collections:
            coll = Collection.from_dict(coll_doc)
            status_emoji = {
                'pending': '⏸️',
                'running': '🔄',
                'paused': '⏸️',
                'completed': '✅',
                'failed': '❌'
            }.get(coll.status, '❓')
            
            type_name = {
                'public_group': '公开群组',
                'private_group': '私有群组',
                'channel_post': '频道帖子',
                'channel_comment': '频道评论',
                'keyword_search': '关键词搜索'
            }.get(coll.collection_type, '未知')
            
            text += (
                f"{status_emoji} <b>{coll.name}</b>\n"
                f"   类型: {type_name} | 用户: {coll.collected_users} | 群组: {coll.collected_groups}\n\n"
            )
            
            keyboard.append([
                InlineKeyboardButton(f"📊 {coll.name}", callback_data=f'collection_detail_{str(coll._id)}')
            ])
        
        # 分页按钮
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("⬅️ 上一页", callback_data=f'collection_list_{page - 1}'))
        if (page + 1) * limit < total:
            nav_buttons.append(InlineKeyboardButton("➡️ 下一页", callback_data=f'collection_list_{page + 1}'))
        
        if nav_buttons:
            keyboard.append(nav_buttons)
        
        keyboard.append([InlineKeyboardButton("🔙 返回", callback_data='menu_collection')])
    
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')


async def show_collection_detail(query, collection_id):
    """显示采集任务详情"""
    from bson import ObjectId
    # Use module-level _db
    db = _get_db()
    coll_doc = db[Collection.COLLECTION_NAME].find_one({'_id': ObjectId(collection_id)})
    if not coll_doc:
        await query.answer("❌ 采集任务不存在", show_alert=True)
        return
    
    coll = Collection.from_dict(coll_doc)
    
    status_emoji = {
        'pending': '⏸️',
        'running': '🔄',
        'paused': '⏸️',
        'completed': '✅',
        'failed': '❌'
    }.get(coll.status, '❓')
    
    type_name = {
        'public_group': '公开群组',
        'private_group': '私有群组',
        'channel_post': '频道帖子',
        'channel_comment': '频道评论',
        'keyword_search': '关键词搜索'
    }.get(coll.collection_type, '未知')
    
    text = (
        f"📊 <b>采集详情</b>\n\n"
        f"📝 名称: {coll.name}\n"
        f"📁 类型: {type_name}\n"
        f"🔄 状态: {status_emoji} {coll.status}\n"
        f"👥 已采集用户: {coll.collected_users}\n"
        f"📢 已采集群组: {coll.collected_groups}\n"
    )
    
    if coll.target_link:
        text += f"🔗 目标: {coll.target_link}\n"
    if coll.keyword:
        text += f"🔍 关键词: {coll.keyword}\n"
    
    if coll.started_at:
        text += f"⏰ 开始时间: {coll.started_at.strftime('%Y-%m-%d %H:%M:%S')}\n"
    if coll.completed_at:
        text += f"✅ 完成时间: {coll.completed_at.strftime('%Y-%m-%d %H:%M:%S')}\n"
    
    if coll.error_message:
        text += f"\n❌ 错误: {coll.error_message}\n"
    
    keyboard = []
    
    # 根据状态显示不同按钮
    if coll.status == 'pending' or coll.status == 'paused':
        keyboard.append([InlineKeyboardButton("▶️ 开始采集", callback_data=f'collection_start_{collection_id}')])
    elif coll.status == 'running':
        keyboard.append([InlineKeyboardButton("⏸️ 停止采集", callback_data=f'collection_stop_{collection_id}')])
    
    # 导出按钮
    if coll.collected_users > 0:
        keyboard.append([InlineKeyboardButton("📥 导出用户", callback_data=f'collection_export_users_{collection_id}')])
    if coll.collected_groups > 0:
        keyboard.append([InlineKeyboardButton("📥 导出群组", callback_data=f'collection_export_groups_{collection_id}')])
    
    # 删除按钮
    keyboard.append([InlineKeyboardButton("🗑️ 删除", callback_data=f'collection_delete_{collection_id}')])
    keyboard.append([InlineKeyboardButton("🔙 返回", callback_data='collection_list')])
    
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')


async def start_create_collection(update, context):
    """开始创建采集任务"""
    query = update.callback_query
    await query.answer()
    await query.edit_message_text(
        "➕ <b>创建采集任务</b>\n\n"
        "请输入采集任务名称：",
        parse_mode='HTML'
    )
    return COLLECTION_NAME_INPUT


async def handle_collection_name(update, context):
    """处理采集任务名称输入"""
    name = update.message.text.strip()
    
    if not name:
        await update.message.reply_text("❌ 名称不能为空，请重新输入：")
        return COLLECTION_NAME_INPUT
    
    context.user_data['collection_name'] = name
    
    # 选择采集类型
    keyboard = [
        [InlineKeyboardButton("📢 公开群组采集", callback_data='coll_type_public_group')],
        [InlineKeyboardButton("🔒 私有群组采集", callback_data='coll_type_private_group')],
        [InlineKeyboardButton("📰 频道帖子采集", callback_data='coll_type_channel_post')],
        [InlineKeyboardButton("💬 频道评论采集", callback_data='coll_type_channel_comment')],
        [InlineKeyboardButton("🔍 关键词搜索", callback_data='coll_type_keyword_search')],
        [InlineKeyboardButton("❌ 取消", callback_data='menu_collection')]
    ]
    
    await update.message.reply_text(
        f"✅ 任务名称: {name}\n\n"
        "请选择采集类型：",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='HTML'
    )
    
    return COLLECTION_TYPE_SELECT


async def handle_collection_type(update, context):
    """处理采集类型选择"""
    query = update.callback_query
    await query.answer()
    from bot import Account, AccountStatus
    from bson import ObjectId
    
    coll_type = query.data.replace('coll_type_', '')
    context.user_data['collection_type'] = coll_type
    
    type_name = {
        'public_group': '公开群组采集',
        'private_group': '私有群组采集',
        'channel_post': '频道帖子采集',
        'channel_comment': '频道评论采集',
        'keyword_search': '关键词搜索'
    }.get(coll_type, '未知类型')
    
    # 获取采集专用账户（只显示 collection 类型的 session 格式账户）
    db = _get_db()
    accounts = list(db[Account.COLLECTION_NAME].find({
        'status': AccountStatus.ACTIVE.value,
        'account_type': 'collection',
        'session_name': {'$regex': r'\.(session|session\+json)$'}
    }).limit(10))
    
    if not accounts:
        await query.edit_message_text(
            "❌ 没有可用的采集账户\n\n"
            "采集功能需要专用的 session/session+json 格式账户\n"
            "请先添加采集账户",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📱 账户管理", callback_data='collection_accounts_menu')],
                [InlineKeyboardButton("🔙 返回", callback_data='menu_collection')]
            ]),
            parse_mode='HTML'
        )
        return ConversationHandler.END
    
    keyboard = []
    text = f"✅ 采集类型: {type_name}\n\n请选择使用的账户：\n\n"
    
    for acc_doc in accounts:
        acc = Account.from_dict(acc_doc)
        text += f"📱 {acc.phone} - {acc.session_name}\n"
        keyboard.append([
            InlineKeyboardButton(
                f"📱 {acc.phone}",
                callback_data=f'coll_account_{str(acc._id)}'
            )
        ])
    
    keyboard.append([InlineKeyboardButton("❌ 取消", callback_data='menu_collection')])
    
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
    
    return COLLECTION_ACCOUNT_SELECT


async def handle_collection_account(update, context):
    """处理账户选择"""
    query = update.callback_query
    await query.answer()
    from bot import Account
    from bson import ObjectId
    
    account_id = query.data.replace('coll_account_', '')
    context.user_data['collection_account_id'] = account_id
    
    # 获取账户信息
    db = _get_db()
    acc_doc = db[Account.COLLECTION_NAME].find_one({'_id': ObjectId(account_id)})
    if not acc_doc:
        await query.answer("❌ 账户不存在", show_alert=True)
        return ConversationHandler.END
    
    acc = Account.from_dict(acc_doc)
    coll_type = context.user_data.get('collection_type')
    
    # 根据采集类型要求不同的输入
    if coll_type == 'keyword_search':
        await query.edit_message_text(
            f"✅ 使用账户: {acc.phone}\n\n"
            "请输入搜索关键词：",
            parse_mode='HTML'
        )
        return COLLECTION_KEYWORD_INPUT
    else:
        await query.edit_message_text(
            f"✅ 使用账户: {acc.phone}\n\n"
            "请输入目标链接（群组/频道链接或用户名）：\n"
            "例如: @username 或 https://t.me/username",
            parse_mode='HTML'
        )
        return COLLECTION_TARGET_INPUT


async def handle_collection_target(update, context):
    """处理目标输入"""
    target = update.message.text.strip()
    
    if not target:
        await update.message.reply_text("❌ 目标不能为空，请重新输入：")
        return COLLECTION_TARGET_INPUT
    
    context.user_data['collection_target'] = target
    
    # 询问是否需要配置过滤器
    keyboard = [
        [InlineKeyboardButton("⚙️ 配置过滤器", callback_data='coll_configure_filters')],
        [InlineKeyboardButton("✅ 直接创建", callback_data='coll_create_now')],
        [InlineKeyboardButton("❌ 取消", callback_data='menu_collection')]
    ]
    
    await update.message.reply_text(
        f"✅ 目标: {target}\n\n"
        "是否需要配置过滤器？",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    
    return COLLECTION_FILTER_CONFIG


async def handle_collection_keyword(update, context):
    """处理关键词输入"""
    keyword = update.message.text.strip()
    
    if not keyword:
        await update.message.reply_text("❌ 关键词不能为空，请重新输入：")
        return COLLECTION_KEYWORD_INPUT
    
    context.user_data['collection_keyword'] = keyword
    
    # 询问是否需要配置过滤器
    keyboard = [
        [InlineKeyboardButton("⚙️ 配置搜索限制", callback_data='coll_configure_filters')],
        [InlineKeyboardButton("✅ 直接创建", callback_data='coll_create_now')],
        [InlineKeyboardButton("❌ 取消", callback_data='menu_collection')]
    ]
    
    await update.message.reply_text(
        f"✅ 关键词: {keyword}\n\n"
        "是否需要配置搜索限制？",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    
    return COLLECTION_FILTER_CONFIG


async def show_filter_config(update, context):
    """显示过滤器配置"""
    query = update.callback_query
    await query.answer()
    filters = context.user_data.get('collection_filters', {})
    
    text = "⚙️ <b>过滤器配置</b>\n\n"
    text += f"❌ 排除管理员: {'是' if filters.get('exclude_admin') else '否'}\n"
    text += f"💎 仅高级会员: {'是' if filters.get('premium_only') else '否'}\n"
    text += f"📷 必须有头像: {'是' if filters.get('has_photo') else '否'}\n"
    text += f"👤 必须有用户名: {'是' if filters.get('has_username') else '否'}\n"
    
    keyboard = [
        [
            InlineKeyboardButton(
                f"{'✅' if filters.get('exclude_admin') else '☑️'} 排除管理员",
                callback_data='coll_filter_toggle_exclude_admin'
            )
        ],
        [
            InlineKeyboardButton(
                f"{'✅' if filters.get('premium_only') else '☑️'} 仅高级会员",
                callback_data='coll_filter_toggle_premium_only'
            )
        ],
        [
            InlineKeyboardButton(
                f"{'✅' if filters.get('has_photo') else '☑️'} 必须有头像",
                callback_data='coll_filter_toggle_has_photo'
            )
        ],
        [
            InlineKeyboardButton(
                f"{'✅' if filters.get('has_username') else '☑️'} 必须有用户名",
                callback_data='coll_filter_toggle_has_username'
            )
        ],
        [InlineKeyboardButton("✅ 完成配置", callback_data='coll_create_now')],
        [InlineKeyboardButton("❌ 取消", callback_data='menu_collection')]
    ]
    
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')


async def toggle_filter(update, context):
    """切换过滤器选项"""
    query = update.callback_query
    await query.answer()
    filter_name = query.data.replace('coll_filter_toggle_', '')
    
    filters = context.user_data.get('collection_filters', {})
    filters[filter_name] = not filters.get(filter_name, False)
    context.user_data['collection_filters'] = filters
    
    await show_filter_config(update, context)


async def create_collection_now(update, context):
    """立即创建采集任务"""
    query = update.callback_query
    await query.answer()
    from bson import ObjectId
    
    try:
        collection_manager = _get_collection_manager()
        name = context.user_data.get('collection_name')
        coll_type = context.user_data.get('collection_type')
        account_id = context.user_data.get('collection_account_id')
        target = context.user_data.get('collection_target')
        keyword = context.user_data.get('collection_keyword')
        filters = context.user_data.get('collection_filters', {})
        
        # 创建采集任务
        collection = await collection_manager.create_collection(
            name=name,
            collection_type=coll_type,
            account_id=ObjectId(account_id),
            target_link=target,
            keyword=keyword,
            filters=filters
        )
        
        # 清理用户数据
        context.user_data.clear()
        
        await query.edit_message_text(
            f"✅ <b>采集任务创建成功！</b>\n\n"
            f"📝 名称: {name}\n"
            f"🆔 ID: {str(collection._id)}\n\n"
            f"可在采集列表中查看和管理",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📊 查看详情", callback_data=f'collection_detail_{str(collection._id)}')],
                [InlineKeyboardButton("📋 采集列表", callback_data='collection_list')],
                [InlineKeyboardButton("🔙 返回主菜单", callback_data='back_main')]
            ]),
            parse_mode='HTML'
        )
        
        return ConversationHandler.END
    
    except Exception as e:
        logger.error(f"Error creating collection: {e}")
        await query.edit_message_text(
            f"❌ 创建失败: {str(e)}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 返回", callback_data='menu_collection')]]),
            parse_mode='HTML'
        )
        return ConversationHandler.END