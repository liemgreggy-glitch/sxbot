"""
Telegram 私信机器人 - 完整集成版本
一个功能强大的 Telegram 机器人管理系统，用于管理多个 Telegram 账户并执行批量私信任务

功能特性：
- 多账户管理（session、tdata格式支持）
- 富媒体消息支持
- 消息个性化（变量替换）
- 智能防封策略
- 实时进度监控
- 内联按钮交互界面
"""

# ============================================================================
# 导入依赖
# ============================================================================
import asyncio
import os
import logging
import re
import enum
import shutil
import zipfile
import json
import random
import csv
import io
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import threading

# Telegram Bot API
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes, ConversationHandler
)
from telegram import error as telegram_error

# Telethon for account management
from telethon import TelegramClient
from telethon.errors import (
    SessionPasswordNeededError, PhoneCodeInvalidError,
    PhoneNumberInvalidError, FloodWaitError,
    UserPrivacyRestrictedError, UserIsBlockedError,
    ChatWriteForbiddenError, UserNotMutualContactError, PeerFloodError,
    TypeNotFoundError
)

# Database
from pymongo import MongoClient
from bson import ObjectId

# Collection module
import caiji
from caiji import (
    CollectionManager, Collection, CollectedUser, CollectedGroup,
    CollectionType, CollectionStatus, init_collection_indexes
)

# ============================================================================
# 配置加载
# ============================================================================
load_dotenv()

# Setup logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler('./logs/bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ============================================================================
# 配置类
# ============================================================================
class Config:
    """Bot configuration"""
    BOT_TOKEN = os.getenv('BOT_TOKEN', '')
    ADMIN_USER_ID = int(os.getenv('ADMIN_USER_ID', 0))
    MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
    MONGODB_DATABASE = os.getenv('MONGODB_DATABASE', 'telegram_bot')
    
    # Proxy
    PROXY_ENABLED = os.getenv('PROXY_ENABLED', 'false').lower() == 'true'
    PROXY_TYPE = os.getenv('PROXY_TYPE', 'socks5')
    PROXY_HOST = os.getenv('PROXY_HOST', '127.0.0.1')
    PROXY_PORT = int(os.getenv('PROXY_PORT', 1080))
    PROXY_USERNAME = os.getenv('PROXY_USERNAME', '')
    PROXY_PASSWORD = os.getenv('PROXY_PASSWORD', '')
    
    # Telegram API
    API_ID = os.getenv('API_ID', '')
    API_HASH = os.getenv('API_HASH', '')
    
    # Task settings
    DEFAULT_MIN_INTERVAL = int(os.getenv('DEFAULT_MIN_INTERVAL', 30))
    DEFAULT_MAX_INTERVAL = int(os.getenv('DEFAULT_MAX_INTERVAL', 120))
    DEFAULT_DAILY_LIMIT = int(os.getenv('DEFAULT_DAILY_LIMIT', 50))
    
    # Directories
    SESSIONS_DIR = os.getenv('SESSIONS_DIR', './sessions')
    UPLOADS_DIR = os.getenv('UPLOADS_DIR', './uploads')
    MEDIA_DIR = os.getenv('MEDIA_DIR', './media')
    RESULTS_DIR = os.getenv('RESULTS_DIR', './results')
    LOGS_DIR = os.getenv('LOGS_DIR', './logs')
    
    # Constants (moved from global scope)
    POSTBOT_CODE_MIN_LENGTH = 10
    POSTBOT_RESPONSE_WAIT_SECONDS = 2
    SPAMBOT_QUERY_DELAY = 2
    PROGRESS_MONITOR_INTERVAL = 10
    TASK_STOP_TIMEOUT_SECONDS = 2.0
    CONFIG_MESSAGE_DELETE_DELAY = 3
    AUTO_REFRESH_MIN_INTERVAL = 30
    AUTO_REFRESH_MAX_INTERVAL = 50
    AUTO_REFRESH_FAST_INTERVAL = 10
    AUTO_REFRESH_FAST_DURATION = 60
    MAX_AUTO_REFRESH_ERRORS = 5
    ACCOUNT_CHECK_LOOP_INTERVAL = 10
    CONSECUTIVE_FAILURES_THRESHOLD = 50
    
    # Display formatting constants
    MAX_TARGET_DISPLAY_LENGTH = 15
    MAX_MESSAGE_DISPLAY_LENGTH = 20
    PHONE_MASK_VISIBLE_DIGITS = 4
    STOP_CONFIRMATION_ITERATIONS = 50
    STOP_CONFIRMATION_SLEEP = 0.1
    MAX_REPORT_RETRY_ATTEMPTS = 3
    ACCOUNT_STATUS_CACHE_DURATION = 300
    ACCOUNT_STATUS_CHECK_CACHE_DURATION = 30
    MAX_DISPLAYED_ACCOUNTS = 5  # Maximum number of accounts to show in summaries
    MAX_DISPLAYED_LOGS = 5  # Maximum number of recent logs to display
    
    @classmethod
    def ensure_directories(cls):
        """Ensure all required directories exist"""
        for directory in [cls.SESSIONS_DIR, cls.UPLOADS_DIR, cls.MEDIA_DIR, 
                         cls.RESULTS_DIR, cls.LOGS_DIR]:
            os.makedirs(directory, exist_ok=True)
    
    @classmethod
    def get_proxy_dict(cls):
        """Get proxy configuration"""
        if not cls.PROXY_ENABLED:
            return None
        proxy = {
            'proxy_type': cls.PROXY_TYPE,
            'addr': cls.PROXY_HOST,
            'port': cls.PROXY_PORT
        }
        if cls.PROXY_USERNAME:
            proxy['username'] = cls.PROXY_USERNAME
        if cls.PROXY_PASSWORD:
            proxy['password'] = cls.PROXY_PASSWORD
        return proxy
    
    @classmethod
    def validate(cls):
        """Validate configuration"""
        if not cls.BOT_TOKEN:
            raise ValueError("BOT_TOKEN is required")
        if not cls.ADMIN_USER_ID:
            raise ValueError("ADMIN_USER_ID is required")
        if not cls.API_ID or not cls.API_HASH:
            raise ValueError("API_ID and API_HASH are required")


# ============================================================================
# 常量定义
# ============================================================================
# Error message truncation lengths for target.last_error
ERROR_MESSAGE_SHORT_LENGTH = 50  # For short error previews (e.g., user not found errors)
ERROR_MESSAGE_LONG_LENGTH = 100  # For detailed error messages (e.g., full exception text)

# Default values for force send mode
DEFAULT_CONSECUTIVE_FAILURE_LIMIT = 30  # Default consecutive failures before switching account
DEFAULT_ERROR_MESSAGE = "未知错误"  # Default error message when error is not set


# ============================================================================
# 枚举类型
# ============================================================================
class AccountStatus(enum.Enum):
    """Account status"""
    ACTIVE = "active"
    BANNED = "banned"
    LIMITED = "limited"
    INACTIVE = "inactive"


class TaskStatus(enum.Enum):
    """Task status"""
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPED = "stopped"
    COMPLETED = "completed"
    FAILED = "failed"


class MessageFormat(enum.Enum):
    """Message format"""
    PLAIN = "plain"
    MARKDOWN = "markdown"
    HTML = "html"


class MediaType(enum.Enum):
    """Media type"""
    TEXT = "text"
    IMAGE = "image"
    VIDEO = "video"
    VOICE = "voice"
    DOCUMENT = "document"
    FORWARD = "forward"


class SendMethod(enum.Enum):
    """Send method"""
    DIRECT = "direct"  # 直接发送
    POSTBOT = "postbot"  # post代码（使用@postbot配置）
    CHANNEL_FORWARD = "channel_forward"  # 频道转发
    CHANNEL_FORWARD_HIDDEN = "channel_forward_hidden"  # 隐藏转发来源


class FloodWaitStrategy(enum.Enum):
    """FloodWait handling strategy"""
    STOP_TASK = "stop_task"  # 停止任务
    SWITCH_ACCOUNT = "switch_account"  # 切换账号
    CONTINUE_WAIT = "continue_wait"  # 继续等待


class MessageMode(enum.Enum):
    """Message sending mode"""
    NORMAL = "normal"  # 普通模式
    EDIT = "edit"  # 编辑模式
    REPLY = "reply"  # 回复模式


# ============================================================================
# 常量
# ============================================================================
# UI labels mapping
SEND_METHOD_LABELS = {
    SendMethod.DIRECT: '📤 直接发送',
    SendMethod.POSTBOT: '🤖 Post代码',
    SendMethod.CHANNEL_FORWARD: '📢 频道转发',
    SendMethod.CHANNEL_FORWARD_HIDDEN: '🔒 隐藏转发来源'
}

MEDIA_TYPE_LABELS = {
    MediaType.TEXT: '📝 纯文本',
    MediaType.IMAGE: '🖼️ 图片',
    MediaType.VIDEO: '🎥 视频',
    MediaType.DOCUMENT: '📄 文档',
    MediaType.FORWARD: '📡 转发'
}

# FloodWait strategy mappings
FLOOD_STRATEGY_FULL_TO_SHORT = {
    'switch_account': 'switch',
    'continue_wait': 'wait',
    'stop_task': 'stop'
}

FLOOD_STRATEGY_SHORT_TO_FULL = {
    'switch': 'switch_account',
    'wait': 'continue_wait',
    'stop': 'stop_task'
}

FLOOD_STRATEGY_DISPLAY = {
    'switch_account': '🔄 切换账号',
    'continue_wait': '⏳ 继续等待',
    'stop_task': '⛔ 停止任务'
}

FLOOD_STRATEGY_DISPLAY_SHORT = {
    'switch': '切换账号',
    'wait': '继续等待',
    'stop': '停止任务'
}


# ============================================================================
# 辅助函数
# ============================================================================
async def safe_answer_query(query, text="", show_alert=False, timeout=5.0):
    """
    安全地回答 callback query，避免超时错误
    
    Args:
        query: CallbackQuery 对象
        text: 回答文本
        show_alert: 是否显示警告框
        timeout: 超时时间（秒）
    """
    if query is None:
        logger.warning("safe_answer_query called with None query, skipping")
        return
    
    try:
        await asyncio.wait_for(
            query.answer(text, show_alert=show_alert),
            timeout=timeout
        )
    except asyncio.TimeoutError:
        query_id = getattr(query, 'id', 'unknown')
        logger.warning(f"Query answer timeout after {timeout}s: query_id={query_id}")
    except telegram_error.BadRequest as e:
        # Query already answered or expired
        logger.warning(f"Query BadRequest (likely expired): {e}")
    except AttributeError as e:
        logger.error(f"Query object missing required attributes: {e}")
    except Exception as e:
        logger.error(f"Unexpected error answering query: {e}")


# Global cache for account spambot status checks (thread-safe)
# Format: {account_id: {'status': 'active/limited/banned', 'checked_at': datetime}}
account_status_cache = {}
account_status_cache_lock = threading.Lock()


async def check_account_real_status(account_manager, account_id):
    """
    实时检查账户状态（通过 @spambot）
    带有5分钟缓存避免频繁查询
    
    Args:
        account_manager: AccountManager 实例
        account_id: 账户ID
    
    Returns:
        str: 'active', 'limited', 'banned', or 'unknown'
    """
    account_id_str = str(account_id)
    
    # 检查缓存（线程安全）
    with account_status_cache_lock:
        if account_id_str in account_status_cache:
            cached = account_status_cache[account_id_str]
            cache_age = (datetime.now(timezone.utc) - cached['checked_at']).total_seconds()
            if cache_age < Config.ACCOUNT_STATUS_CACHE_DURATION:
                logger.debug(f"Using cached status for account {account_id}: {cached['status']}")
                return cached['status']
    
    client = None
    try:
        # 获取客户端 - 带超时保护
        client = await asyncio.wait_for(
            account_manager.get_client(account_id),
            timeout=10.0
        )
        
        # 查询 @spambot - 整个操作带超时保护
        async def query_spambot():
            spambot = await client.get_entity('spambot')
            await client.send_message(spambot, '/start')
            await asyncio.sleep(2)
            return await client.get_messages(spambot, limit=1)
        
        messages = await asyncio.wait_for(query_spambot(), timeout=15.0)
        
        if not messages:
            logger.warning(f"No response from @spambot for account {account_id}")
            return 'unknown'
        
        response = messages[0].text.lower()
        logger.info(f"@spambot response for account {account_id}: {response[:100]}...")
        
        # 分类状态（使用与 check_all_accounts_status 相同的逻辑）
        status = 'active'
        if any(keyword in response for keyword in ['banned', 'ban', 'spam', 'block', '封禁', '禁止']):
            status = 'banned'
        elif any(keyword in response for keyword in ['限制', 'limit', 'restrict', 'frozen', '冻结']):
            status = 'limited'
        
        # 更新缓存（线程安全）
        with account_status_cache_lock:
            account_status_cache[account_id_str] = {
                'status': status,
                'checked_at': datetime.now(timezone.utc)
            }
        
        # 更新数据库状态
        if status == 'banned':
            account_manager.accounts_col.update_one(
                {'_id': ObjectId(account_id)},
                {'$set': {'status': AccountStatus.BANNED.value, 'updated_at': datetime.now(timezone.utc)}}
            )
        elif status == 'limited':
            account_manager.accounts_col.update_one(
                {'_id': ObjectId(account_id)},
                {'$set': {'status': AccountStatus.LIMITED.value, 'updated_at': datetime.now(timezone.utc)}}
            )
        
        return status
        
    except asyncio.TimeoutError:
        logger.error(f"Timeout checking account {account_id} with @spambot")
        return 'unknown'
    except Exception as e:
        logger.error(f"Error checking account {account_id} with @spambot: {e}", exc_info=True)
        return 'unknown'
    finally:
        # Ensure any pending operations are properly handled
        # Note: We don't disconnect the client as it's cached and managed by account_manager
        if client:
            try:
                # Give a moment for any pending operations to complete
                await asyncio.sleep(0.1)
            except Exception:
                pass


async def should_stop_task_due_to_accounts(db_instance, task_id):
    """
    检查是否应该因为没有可用账户而停止任务
    
    Args:
        db_instance: MongoDB database instance
        task_id: 任务ID
    
    Returns:
        tuple: (should_stop: bool, reason: str)
    """
    # 统计可用账户（只统计 messaging 类型的账户）
    active_count = db_instance[Account.COLLECTION_NAME].count_documents({
        'status': AccountStatus.ACTIVE.value,
        'account_type': 'messaging'
    })
    
    if active_count == 0:
        # 没有可用账户，应该停止任务
        reason = "所有账户均无法使用（封禁/受限/冻结）"
        logger.warning(f"Task {task_id}: {reason}")
        
        # 更新任务状态
        db_instance[Task.COLLECTION_NAME].update_one(
            {'_id': ObjectId(task_id)},
            {
                '$set': {
                    'status': TaskStatus.STOPPED.value,
                    'completed_at': datetime.now(timezone.utc),
                    'updated_at': datetime.now(timezone.utc)
                }
            }
        )
        
        return True, reason
    
    return False, ""


# ============================================================================
# 数据库模型
# ============================================================================
class Account:
    """Telegram account model - MongoDB document"""
    COLLECTION_NAME = 'accounts'
    
    def __init__(self, phone, session_name, status=None, api_id=None, api_hash=None,
                 messages_sent_today=0, total_messages_sent=0, last_used=None,
                 daily_limit=50, created_at=None, updated_at=None, proxy_id=None, 
                 account_type='messaging', _id=None):
        self._id = _id
        self.phone = phone
        self.session_name = session_name
        self.status = status or AccountStatus.ACTIVE.value
        self.api_id = api_id
        self.api_hash = api_hash
        self.messages_sent_today = messages_sent_today
        self.total_messages_sent = total_messages_sent
        self.last_used = last_used
        self.daily_limit = daily_limit
        self.proxy_id = proxy_id  # Reference to Proxy document
        self.account_type = account_type  # 'messaging' or 'collection'
        self.created_at = created_at or datetime.utcnow()
        self.updated_at = updated_at or datetime.utcnow()
    
    def to_dict(self):
        """Convert to dictionary for MongoDB"""
        doc = {
            'phone': self.phone,
            'session_name': self.session_name,
            'status': self.status,
            'api_id': self.api_id,
            'api_hash': self.api_hash,
            'messages_sent_today': self.messages_sent_today,
            'total_messages_sent': self.total_messages_sent,
            'last_used': self.last_used,
            'daily_limit': self.daily_limit,
            'proxy_id': self.proxy_id,
            'account_type': self.account_type,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }
        if self._id:
            doc['_id'] = self._id
        return doc
    
    @classmethod
    def from_dict(cls, doc):
        """Create instance from MongoDB document"""
        if not doc:
            return None
        return cls(
            phone=doc.get('phone'),
            session_name=doc.get('session_name'),
            status=doc.get('status'),
            api_id=doc.get('api_id'),
            api_hash=doc.get('api_hash'),
            messages_sent_today=doc.get('messages_sent_today', 0),
            total_messages_sent=doc.get('total_messages_sent', 0),
            last_used=doc.get('last_used'),
            daily_limit=doc.get('daily_limit', 50),
            proxy_id=doc.get('proxy_id'),
            account_type=doc.get('account_type', 'messaging'),
            created_at=doc.get('created_at'),
            updated_at=doc.get('updated_at'),
            _id=doc.get('_id')
        )


class Task:
    """Task model - MongoDB document"""
    COLLECTION_NAME = 'tasks'
    
    def __init__(self, name, message_text, status=None, message_format=None, 
                 media_type=None, media_path=None, send_method=None, postbot_code=None,
                 channel_link=None, min_interval=30, max_interval=120, account_id=None,
                 total_targets=0, sent_count=0, failed_count=0, created_at=None,
                 started_at=None, completed_at=None, updated_at=None, _id=None,
                 thread_count=1, pin_message=False, delete_dialog=False, 
                 repeat_send=False, ignore_bidirectional_limit=0,
                 # New fields for edit mode
                 message_mode='normal', edit_delay_min=5, edit_delay_max=15, edit_content=None,
                 # New fields for reply mode
                 reply_timeout=300, reply_keywords=None, reply_default=None,
                 # New fields for batch pause
                 batch_pause_count=0, batch_pause_min=0, batch_pause_max=5,
                 # New field for FloodWait strategy
                 flood_wait_strategy='switch_account',
                 # New fields for voice call
                 voice_call_enabled=False, voice_call_duration=10, 
                 voice_call_wait_after=3, voice_call_send_if_failed=True,
                 # Other new fields
                 thread_start_interval=1, auto_switch_dead_account=True,
                 # New fields for retry and limits
                 daily_limit=50, retry_count=3, retry_interval=60, force_private_mode=False):
        self._id = _id
        self.name = name
        self.status = status or TaskStatus.PENDING.value
        self.message_text = message_text
        self.message_format = message_format or MessageFormat.PLAIN.value
        self.media_type = media_type or MediaType.TEXT.value
        self.media_path = media_path
        self.send_method = send_method or SendMethod.DIRECT.value
        self.postbot_code = postbot_code
        self.channel_link = channel_link
        self.min_interval = min_interval
        self.max_interval = max_interval
        self.account_id = account_id
        self.total_targets = total_targets
        self.sent_count = sent_count
        self.failed_count = failed_count
        self.created_at = created_at or datetime.utcnow()
        self.started_at = started_at
        self.completed_at = completed_at
        self.updated_at = updated_at or datetime.utcnow()
        # New configuration options
        self.thread_count = thread_count
        self.pin_message = pin_message
        self.delete_dialog = delete_dialog
        self.repeat_send = repeat_send
        self.ignore_bidirectional_limit = ignore_bidirectional_limit
        # Edit mode fields
        self.message_mode = message_mode
        self.edit_delay_min = edit_delay_min
        self.edit_delay_max = edit_delay_max
        self.edit_content = edit_content
        # Reply mode fields
        self.reply_timeout = reply_timeout
        self.reply_keywords = reply_keywords or {}
        self.reply_default = reply_default
        # Batch pause fields
        self.batch_pause_count = batch_pause_count
        self.batch_pause_min = batch_pause_min
        self.batch_pause_max = batch_pause_max
        # FloodWait strategy
        self.flood_wait_strategy = flood_wait_strategy
        # Voice call fields
        self.voice_call_enabled = voice_call_enabled
        self.voice_call_duration = voice_call_duration
        self.voice_call_wait_after = voice_call_wait_after
        self.voice_call_send_if_failed = voice_call_send_if_failed
        # Other fields
        self.thread_start_interval = thread_start_interval
        self.auto_switch_dead_account = auto_switch_dead_account
        # Retry and limit fields
        self.daily_limit = daily_limit
        self.retry_count = retry_count
        self.retry_interval = retry_interval
        self.force_private_mode = force_private_mode
    
    def to_dict(self):
        """Convert to dictionary for MongoDB"""
        doc = {
            'name': self.name,
            'status': self.status,
            'message_text': self.message_text,
            'message_format': self.message_format,
            'media_type': self.media_type,
            'media_path': self.media_path,
            'send_method': self.send_method,
            'postbot_code': self.postbot_code,
            'channel_link': self.channel_link,
            'min_interval': self.min_interval,
            'max_interval': self.max_interval,
            'account_id': self.account_id,
            'total_targets': self.total_targets,
            'sent_count': self.sent_count,
            'failed_count': self.failed_count,
            'created_at': self.created_at,
            'started_at': self.started_at,
            'completed_at': self.completed_at,
            'updated_at': self.updated_at,
            'thread_count': self.thread_count,
            'pin_message': self.pin_message,
            'delete_dialog': self.delete_dialog,
            'repeat_send': self.repeat_send,
            'ignore_bidirectional_limit': self.ignore_bidirectional_limit,
            # Edit mode fields
            'message_mode': self.message_mode,
            'edit_delay_min': self.edit_delay_min,
            'edit_delay_max': self.edit_delay_max,
            'edit_content': self.edit_content,
            # Reply mode fields
            'reply_timeout': self.reply_timeout,
            'reply_keywords': self.reply_keywords,
            'reply_default': self.reply_default,
            # Batch pause fields
            'batch_pause_count': self.batch_pause_count,
            'batch_pause_min': self.batch_pause_min,
            'batch_pause_max': self.batch_pause_max,
            # FloodWait strategy
            'flood_wait_strategy': self.flood_wait_strategy,
            # Voice call fields
            'voice_call_enabled': self.voice_call_enabled,
            'voice_call_duration': self.voice_call_duration,
            'voice_call_wait_after': self.voice_call_wait_after,
            'voice_call_send_if_failed': self.voice_call_send_if_failed,
            # Other fields
            'thread_start_interval': self.thread_start_interval,
            'auto_switch_dead_account': self.auto_switch_dead_account,
            # Retry and limit fields
            'daily_limit': self.daily_limit,
            'retry_count': self.retry_count,
            'retry_interval': self.retry_interval,
            'force_private_mode': self.force_private_mode
        }
        if self._id:
            doc['_id'] = self._id
        return doc
    
    @classmethod
    def from_dict(cls, doc):
        """Create instance from MongoDB document"""
        if not doc:
            return None
        return cls(
            name=doc.get('name'),
            message_text=doc.get('message_text'),
            status=doc.get('status'),
            message_format=doc.get('message_format'),
            media_type=doc.get('media_type'),
            media_path=doc.get('media_path'),
            send_method=doc.get('send_method'),
            postbot_code=doc.get('postbot_code'),
            channel_link=doc.get('channel_link'),
            min_interval=doc.get('min_interval', 30),
            max_interval=doc.get('max_interval', 120),
            account_id=doc.get('account_id'),
            total_targets=doc.get('total_targets', 0),
            sent_count=doc.get('sent_count', 0),
            failed_count=doc.get('failed_count', 0),
            created_at=doc.get('created_at'),
            started_at=doc.get('started_at'),
            completed_at=doc.get('completed_at'),
            updated_at=doc.get('updated_at'),
            _id=doc.get('_id'),
            thread_count=doc.get('thread_count', 1),
            pin_message=doc.get('pin_message', False),
            delete_dialog=doc.get('delete_dialog', False),
            repeat_send=doc.get('repeat_send', False),
            ignore_bidirectional_limit=doc.get('ignore_bidirectional_limit', 0),
            # Edit mode fields
            message_mode=doc.get('message_mode', 'normal'),
            edit_delay_min=doc.get('edit_delay_min', 5),
            edit_delay_max=doc.get('edit_delay_max', 15),
            edit_content=doc.get('edit_content'),
            # Reply mode fields
            reply_timeout=doc.get('reply_timeout', 300),
            reply_keywords=doc.get('reply_keywords', {}),
            reply_default=doc.get('reply_default'),
            # Batch pause fields
            batch_pause_count=doc.get('batch_pause_count', 0),
            batch_pause_min=doc.get('batch_pause_min', 0),
            batch_pause_max=doc.get('batch_pause_max', 5),
            # FloodWait strategy
            flood_wait_strategy=doc.get('flood_wait_strategy', 'switch_account'),
            # Voice call fields
            voice_call_enabled=doc.get('voice_call_enabled', False),
            voice_call_duration=doc.get('voice_call_duration', 10),
            voice_call_wait_after=doc.get('voice_call_wait_after', 3),
            voice_call_send_if_failed=doc.get('voice_call_send_if_failed', True),
            # Other fields
            thread_start_interval=doc.get('thread_start_interval', 1),
            auto_switch_dead_account=doc.get('auto_switch_dead_account', True),
            # Retry and limit fields
            daily_limit=doc.get('daily_limit', 50),
            retry_count=doc.get('retry_count', 3),
            retry_interval=doc.get('retry_interval', 60),
            force_private_mode=doc.get('force_private_mode', False)
        )


class Target:
    """Target user model - MongoDB document"""
    COLLECTION_NAME = 'targets'
    
    def __init__(self, task_id, username=None, user_id=None, first_name=None,
                 last_name=None, is_sent=False, is_valid=True, error_message=None,
                 created_at=None, sent_at=None, _id=None,
                 failed_accounts=None, last_error=None, retry_count=0, 
                 last_account_id=None, updated_at=None):
        self._id = _id
        self.task_id = task_id
        self.username = username
        self.user_id = user_id
        self.first_name = first_name
        self.last_name = last_name
        self.is_sent = is_sent
        self.is_valid = is_valid
        self.error_message = error_message
        self.created_at = created_at or datetime.utcnow()
        self.sent_at = sent_at
        # New fields for force send mode
        self.failed_accounts = failed_accounts or []
        self.last_error = last_error
        self.retry_count = retry_count
        self.last_account_id = last_account_id
        self.updated_at = updated_at or datetime.utcnow()
    
    def to_dict(self):
        """Convert to dictionary for MongoDB"""
        doc = {
            'task_id': self.task_id,
            'username': self.username,
            'user_id': self.user_id,
            'first_name': self.first_name,
            'last_name': self.last_name,
            'is_sent': self.is_sent,
            'is_valid': self.is_valid,
            'error_message': self.error_message,
            'created_at': self.created_at,
            'sent_at': self.sent_at,
            'failed_accounts': self.failed_accounts,
            'last_error': self.last_error,
            'retry_count': self.retry_count,
            'last_account_id': self.last_account_id,
            'updated_at': self.updated_at
        }
        if self._id:
            doc['_id'] = self._id
        return doc
    
    @classmethod
    def from_dict(cls, doc):
        """Create instance from MongoDB document"""
        if not doc:
            return None
        return cls(
            task_id=doc.get('task_id'),
            username=doc.get('username'),
            user_id=doc.get('user_id'),
            first_name=doc.get('first_name'),
            last_name=doc.get('last_name'),
            is_sent=doc.get('is_sent', False),
            is_valid=doc.get('is_valid', True),
            error_message=doc.get('error_message'),
            created_at=doc.get('created_at'),
            sent_at=doc.get('sent_at'),
            _id=doc.get('_id'),
            failed_accounts=doc.get('failed_accounts', []),
            last_error=doc.get('last_error'),
            retry_count=doc.get('retry_count', 0),
            last_account_id=doc.get('last_account_id'),
            updated_at=doc.get('updated_at')
        )


class MessageLog:
    """Message log model - MongoDB document"""
    COLLECTION_NAME = 'message_logs'
    
    def __init__(self, task_id, account_id, target_id, message_text,
                 success=False, error_message=None, sent_at=None, _id=None):
        self._id = _id
        self.task_id = task_id
        self.account_id = account_id
        self.target_id = target_id
        self.message_text = message_text
        self.success = success
        self.error_message = error_message
        self.sent_at = sent_at or datetime.utcnow()
    
    def to_dict(self):
        """Convert to dictionary for MongoDB"""
        doc = {
            'task_id': self.task_id,
            'account_id': self.account_id,
            'target_id': self.target_id,
            'message_text': self.message_text,
            'success': self.success,
            'error_message': self.error_message,
            'sent_at': self.sent_at
        }
        if self._id:
            doc['_id'] = self._id
        return doc
    
    @classmethod
    def from_dict(cls, doc):
        """Create instance from MongoDB document"""
        if not doc:
            return None
        return cls(
            task_id=doc.get('task_id'),
            account_id=doc.get('account_id'),
            target_id=doc.get('target_id'),
            message_text=doc.get('message_text'),
            success=doc.get('success', False),
            error_message=doc.get('error_message'),
            sent_at=doc.get('sent_at'),
            _id=doc.get('_id')
        )


class Proxy:
    """Proxy model - MongoDB document"""
    COLLECTION_NAME = 'proxies'
    
    def __init__(self, proxy_type, host, port, username=None, password=None,
                 is_active=True, success_count=0, fail_count=0, last_used=None,
                 created_at=None, updated_at=None, _id=None):
        self._id = _id
        self.proxy_type = proxy_type  # 'socks5', 'http', 'https'
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.is_active = is_active
        self.success_count = success_count
        self.fail_count = fail_count
        self.last_used = last_used
        self.created_at = created_at or datetime.utcnow()
        self.updated_at = updated_at or datetime.utcnow()
    
    def to_dict(self):
        """Convert to dictionary for MongoDB"""
        doc = {
            'proxy_type': self.proxy_type,
            'host': self.host,
            'port': self.port,
            'username': self.username,
            'password': self.password,
            'is_active': self.is_active,
            'success_count': self.success_count,
            'fail_count': self.fail_count,
            'last_used': self.last_used,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }
        if self._id:
            doc['_id'] = self._id
        return doc
    
    @classmethod
    def from_dict(cls, doc):
        """Create instance from MongoDB document"""
        if not doc:
            return None
        return cls(
            proxy_type=doc.get('proxy_type'),
            host=doc.get('host'),
            port=doc.get('port'),
            username=doc.get('username'),
            password=doc.get('password'),
            is_active=doc.get('is_active', True),
            success_count=doc.get('success_count', 0),
            fail_count=doc.get('fail_count', 0),
            last_used=doc.get('last_used'),
            created_at=doc.get('created_at'),
            updated_at=doc.get('updated_at'),
            _id=doc.get('_id')
        )
    
    def get_proxy_dict(self):
        """Get proxy configuration for Telethon"""
        proxy = {
            'proxy_type': self.proxy_type,
            'addr': self.host,
            'port': self.port
        }
        if self.username:
            proxy['username'] = self.username
        if self.password:
            proxy['password'] = self.password
        return proxy



def init_db(mongodb_uri, database_name):
    """Initialize MongoDB database"""
    client = MongoClient(mongodb_uri)
    db = client[database_name]
    
    # Create indexes for better performance
    db[Account.COLLECTION_NAME].create_index('phone', unique=True)
    db[Account.COLLECTION_NAME].create_index('session_name', unique=True)
    db[Account.COLLECTION_NAME].create_index('status')
    db[Account.COLLECTION_NAME].create_index('proxy_id')
    db[Account.COLLECTION_NAME].create_index('account_type')
    
    db[Task.COLLECTION_NAME].create_index('status')
    db[Task.COLLECTION_NAME].create_index('account_id')
    
    db[Target.COLLECTION_NAME].create_index('task_id')
    db[Target.COLLECTION_NAME].create_index('is_sent')
    db[Target.COLLECTION_NAME].create_index([('task_id', 1), ('is_sent', 1)])
    
    db[MessageLog.COLLECTION_NAME].create_index('task_id')
    db[MessageLog.COLLECTION_NAME].create_index('account_id')
    db[MessageLog.COLLECTION_NAME].create_index('sent_at')
    
    db[Proxy.COLLECTION_NAME].create_index('is_active')
    db[Proxy.COLLECTION_NAME].create_index([('host', 1), ('port', 1)])
    
    # Initialize collection indexes
    init_collection_indexes(db)
    
    return db


def get_db_client(mongodb_uri, database_name):
    """Get MongoDB database client"""
    client = MongoClient(mongodb_uri)
    return client[database_name]


# ============================================================================
# 代理管理函数
# ============================================================================
def parse_proxy_line(line):
    """
    Parse proxy line from multiple formats:
    - host:port:username:password (4-part colon-separated, supports domain names like f01a4db3d3952561.abcproxy.vip:4950:user:pass)
    - socks5://IP:port:username:password (protocol prefix with auth)
    - socks5://user:pass@host:port (ABCProxy URL format)
    - host:port (simple format without auth)
    
    Returns Proxy object or None if invalid
    """
    line = line.strip()
    if not line or line.startswith('#'):
        return None
    
    try:
        # ABCProxy format: socks5://user:pass@host:port or http://user:pass@host:port
        if '://' in line and '@' in line:
            # Extract protocol
            protocol, rest = line.split('://', 1)
            proxy_type = protocol.lower()
            
            # Extract auth and host
            auth_part, host_part = rest.split('@', 1)
            username, password = auth_part.split(':', 1)
            
            # Extract host and port
            if ':' in host_part:
                host, port = host_part.rsplit(':', 1)
                port = int(port)
            else:
                return None
            
            return Proxy(
                proxy_type=proxy_type,
                host=host,
                port=port,
                username=username,
                password=password
            )
        
        # Protocol prefix format: socks5://IP:端口:用户名:密码
        elif '://' in line:
            protocol, rest = line.split('://', 1)
            proxy_type = protocol.lower()
            parts = rest.split(':')
            
            if len(parts) == 4:
                # With auth
                host, port, username, password = parts
                return Proxy(
                    proxy_type=proxy_type,
                    host=host,
                    port=int(port),
                    username=username,
                    password=password
                )
            elif len(parts) == 2:
                # Without auth
                host, port = parts
                return Proxy(
                    proxy_type=proxy_type,
                    host=host,
                    port=int(port)
                )
        
        # Standard format: IP:端口:用户名:密码 or IP:端口
        else:
            parts = line.split(':')
            if len(parts) == 4:
                # With auth
                host, port, username, password = parts
                return Proxy(
                    proxy_type='socks5',  # Default to socks5
                    host=host,
                    port=int(port),
                    username=username,
                    password=password
                )
            elif len(parts) == 2:
                # Without auth
                host, port = parts
                return Proxy(
                    proxy_type='socks5',  # Default to socks5
                    host=host,
                    port=int(port)
                )
    except Exception as e:
        logger.warning(f"Failed to parse proxy line: {line}, error: {e}")
        return None
    
    return None


async def test_proxy(db, proxy_id):
    """Test proxy connection using a temporary Telegram client"""
    try:
        proxy_doc = db[Proxy.COLLECTION_NAME].find_one({'_id': ObjectId(proxy_id)})
        if not proxy_doc:
            return False, "Proxy not found"
        
        proxy = Proxy.from_dict(proxy_doc)
        proxy_dict = proxy.get_proxy_dict()
        
        # Create temporary client to test proxy
        test_session = os.path.join(Config.SESSIONS_DIR, f"test_proxy_{proxy_id}")
        client = TelegramClient(test_session, Config.API_ID, Config.API_HASH, proxy=proxy_dict)
        
        try:
            await client.connect()
            # If we can connect, proxy is working
            success = client.is_connected()
            await client.disconnect()
            
            # Clean up test session - wrapped in try-except to prevent failures
            try:
                if os.path.exists(f"{test_session}.session"):
                    os.remove(f"{test_session}.session")
            except Exception as cleanup_error:
                logger.warning(f"Failed to cleanup test session: {cleanup_error}")
            
            # Update proxy statistics
            if success:
                db[Proxy.COLLECTION_NAME].update_one(
                    {'_id': ObjectId(proxy_id)},
                    {
                        '$inc': {'success_count': 1},
                        '$set': {'last_used': datetime.utcnow(), 'updated_at': datetime.utcnow()}
                    }
                )
                return True, "Connection successful"
            else:
                # Connection failed - automatically delete the proxy
                logger.warning(f"❌ Proxy {proxy.host}:{proxy.port} failed test, deleting...")
                
                # Remove proxy from accounts that are using it
                proxy_oid = ObjectId(proxy_id)
                db[Account.COLLECTION_NAME].update_many(
                    {'$or': [{'proxy_id': proxy_oid}, {'proxy_id': str(proxy_id)}]},
                    {'$set': {'proxy_id': None}}
                )
                
                # Delete the proxy
                db[Proxy.COLLECTION_NAME].delete_one({'_id': proxy_oid})
                logger.info(f"🗑️ Deleted unavailable proxy: {proxy.host}:{proxy.port}")
                
                return False, "Connection failed - proxy deleted"
                
        except Exception as e:
            logger.error(f"Proxy test error: {e}")
            
            # Test failed - automatically delete the proxy
            logger.warning(f"❌ Proxy {proxy.host}:{proxy.port} test error, deleting...")
            
            # Remove proxy from accounts that are using it
            proxy_oid = ObjectId(proxy_id)
            db[Account.COLLECTION_NAME].update_many(
                {'$or': [{'proxy_id': proxy_oid}, {'proxy_id': str(proxy_id)}]},
                {'$set': {'proxy_id': None}}
            )
            
            # Delete the proxy
            db[Proxy.COLLECTION_NAME].delete_one({'_id': proxy_oid})
            logger.info(f"🗑️ Deleted unavailable proxy: {proxy.host}:{proxy.port}")
            
            return False, f"Error: {str(e)} - proxy deleted"
            
    except Exception as e:
        logger.error(f"Proxy test failed: {e}", exc_info=True)
        return False, str(e)



def get_next_available_proxy(db):
    """
    Get next available proxy from pool using round-robin strategy.
    Returns Proxy object or None if no proxies available.
    """
    try:
        # Get all active proxies, sorted by usage count (least used first)
        active_proxies = list(db[Proxy.COLLECTION_NAME].find(
            {'is_active': True}
        ).sort('success_count', 1).limit(1))
        
        if not active_proxies:
            logger.warning("No active proxies available in pool")
            return None
        
        # Return the least used proxy
        return Proxy.from_dict(active_proxies[0])
    except Exception as e:
        logger.error(f"Failed to get proxy from pool: {e}", exc_info=True)
        return None


def assign_proxies_to_accounts(db):
    """
    DEPRECATED: Manual proxy assignment is no longer used.
    Proxies are now automatically assigned during account operations.
    This function is kept for backward compatibility but does nothing.
    """
    logger.warning("Manual proxy assignment is deprecated. Proxies are auto-assigned during operations.")
    return 0


# ============================================================================
# 代理管理类
# ============================================================================
class ProxyManager:
    """Manage proxy health scoring and selection"""
    
    def __init__(self, db):
        self.db = db
        self.proxies_col = db[Proxy.COLLECTION_NAME]
    
    def get_best_proxy(self):
        """Get best proxy based on success rate and recency"""
        try:
            # Get all active proxies
            proxies = list(self.proxies_col.find({'is_active': True}))
            
            if not proxies:
                return None
            
            # Score proxies
            scored_proxies = []
            for proxy_doc in proxies:
                proxy = Proxy.from_dict(proxy_doc)
                score = self._calculate_proxy_score(proxy)
                scored_proxies.append((score, proxy))
            
            # Sort by score (highest first)
            scored_proxies.sort(key=lambda x: x[0], reverse=True)
            
            # Return best proxy
            if scored_proxies:
                return scored_proxies[0][1]
            
            return None
            
        except Exception as e:
            logger.error(f"ProxyManager: Error getting best proxy: {e}")
            return None
    
    def _calculate_proxy_score(self, proxy):
        """Calculate proxy health score (0-100)"""
        total_attempts = proxy.success_count + proxy.fail_count
        
        # No attempts yet, give neutral score
        if total_attempts == 0:
            return 50
        
        # Calculate success rate (0-100)
        success_rate = (proxy.success_count / total_attempts) * 100
        
        # Time decay: prefer recently used proxies
        if proxy.updated_at:
            age_seconds = (datetime.utcnow() - proxy.updated_at).total_seconds()
            age_hours = age_seconds / 3600
            # Decay factor: 1.0 for fresh, 0.5 for 24h old, 0.1 for week old
            time_factor = max(0.1, 1.0 - (age_hours / 168))  # 168 hours = 1 week
        else:
            time_factor = 0.5
        
        # Combined score
        score = success_rate * time_factor
        
        return score
    
    def record_proxy_result(self, proxy_id, success):
        """Record proxy operation result and auto-disable if needed"""
        try:
            if success:
                self.proxies_col.update_one(
                    {'_id': ObjectId(proxy_id)},
                    {
                        '$inc': {'success_count': 1},
                        '$set': {'updated_at': datetime.utcnow()}
                    }
                )
            else:
                self.proxies_col.update_one(
                    {'_id': ObjectId(proxy_id)},
                    {
                        '$inc': {'fail_count': 1},
                        '$set': {'updated_at': datetime.utcnow()}
                    }
                )
                
                # Check if should disable proxy
                proxy_doc = self.proxies_col.find_one({'_id': ObjectId(proxy_id)})
                if proxy_doc:
                    proxy = Proxy.from_dict(proxy_doc)
                    total = proxy.success_count + proxy.fail_count
                    
                    # Disable if failure rate > 80% and at least 10 attempts
                    if total >= 10:
                        failure_rate = (proxy.fail_count / total) * 100
                        if failure_rate > 80:
                            self.proxies_col.update_one(
                                {'_id': ObjectId(proxy_id)},
                                {'$set': {'is_active': False, 'updated_at': datetime.utcnow()}}
                            )
                            logger.warning(f"ProxyManager: Disabled proxy {proxy.host}:{proxy.port} due to {failure_rate:.1f}% failure rate")
                            
        except Exception as e:
            logger.error(f"ProxyManager: Error recording proxy result: {e}")


# ============================================================================
# 消息格式化类
# ============================================================================
class MessageFormatter:
    """Format and personalize messages"""
    
    @staticmethod
    def personalize(message_text, user_info):
        """Personalize message with user information"""
        if not user_info:
            return message_text
        
        replacements = {
            '{name}': user_info.get('name', ''),
            '{first_name}': user_info.get('first_name', ''),
            '{last_name}': user_info.get('last_name', ''),
            '{full_name}': user_info.get('full_name', ''),
            '{username}': user_info.get('username', '')
        }
        
        personalized = message_text
        for placeholder, value in replacements.items():
            if value:
                personalized = personalized.replace(placeholder, value)
        return personalized
    
    @staticmethod
    def extract_user_info(user):
        """Extract user information"""
        info = {}
        info['first_name'] = getattr(user, 'first_name', '') or ''
        info['last_name'] = getattr(user, 'last_name', '') or ''
        info['username'] = f"@{user.username}" if getattr(user, 'username', None) else ''
        
        full_name_parts = []
        if info['first_name']:
            full_name_parts.append(info['first_name'])
        if info['last_name']:
            full_name_parts.append(info['last_name'])
        info['full_name'] = ' '.join(full_name_parts)
        info['name'] = info['username'].replace('@', '') if info['username'] else info['first_name']
        
        return info
    
    @staticmethod
    def get_parse_mode(message_format):
        """Get Telethon parse mode"""
        if message_format == MessageFormat.MARKDOWN:
            return 'md'
        elif message_format == MessageFormat.HTML:
            return 'html'
        return None


# ============================================================================
# Display Formatting Helpers
# ============================================================================
def mask_phone_number(phone: str) -> str:
    """Mask phone number for privacy, showing only last few digits"""
    if not phone or len(phone) < Config.PHONE_MASK_VISIBLE_DIGITS:
        return "****"
    return f"****{phone[-Config.PHONE_MASK_VISIBLE_DIGITS:]}"


def format_log_entry(log: dict, max_target_len: int = None, max_msg_len: int = None) -> tuple:
    """Format log entry for display
    
    Args:
        log: Log dictionary with time, target, status, message fields
        max_target_len: Maximum length for target display
        max_msg_len: Maximum length for message display
        
    Returns:
        Tuple of (time_str, status_emoji, target, message)
    """
    if max_target_len is None:
        max_target_len = Config.MAX_TARGET_DISPLAY_LENGTH
    if max_msg_len is None:
        max_msg_len = Config.MAX_MESSAGE_DISPLAY_LENGTH
    
    time_str = log['time'].strftime('%H:%M:%S') if isinstance(log['time'], datetime) else str(log['time'])
    status_emoji = {'success': '✅', 'failed': '❌', 'skipped': '⏸️'}.get(log['status'], '❓')
    target = log['target'][:max_target_len] if log['target'] else 'unknown'
    message = log['message'][:max_msg_len] if log['message'] else ''
    
    return time_str, status_emoji, target, message


# ============================================================================
# 编辑模式和回复模式类
# ============================================================================
class EditMode:
    """Handle edit mode functionality for messages"""
    
    def __init__(self, task, account_manager):
        self.task = task
        self.account_manager = account_manager
        self.sent_messages = {}  # {target_id: message_obj}
    
    async def send_and_schedule_edit(self, client, entity, target_id, initial_message, edit_content):
        """Send initial message and schedule edit"""
        try:
            # Send initial message
            sent_message = await client.send_message(entity, initial_message)
            
            # Store message for editing
            self.sent_messages[target_id] = sent_message
            
            # Wait random delay
            delay = random.randint(self.task.edit_delay_min, self.task.edit_delay_max)
            logger.info(f"EditMode: Scheduled edit in {delay} seconds for target {target_id}")
            await asyncio.sleep(delay)
            
            # Edit message
            await client.edit_message(entity, sent_message, edit_content)
            logger.info(f"EditMode: Message edited successfully for target {target_id}")
            
            return True
        except Exception as e:
            logger.error(f"EditMode: Failed to edit message for target {target_id}: {e}")
            return False


class ReplyMode:
    """Handle reply mode functionality for auto-replies"""
    
    def __init__(self, task, account_manager):
        self.task = task
        self.account_manager = account_manager
        self.monitoring_tasks = {}  # {target_id: asyncio.Task}
    
    async def monitor_and_reply(self, client, entity, target_id, stop_event):
        """Monitor for user replies and respond accordingly"""
        try:
            # Get initial message count
            initial_messages = await client.get_messages(entity, limit=1)
            last_message_id = initial_messages[0].id if initial_messages else 0
            
            start_time = datetime.utcnow()
            timeout = timedelta(seconds=self.task.reply_timeout)
            
            while (datetime.utcnow() - start_time) < timeout:
                if stop_event.is_set():
                    logger.info(f"ReplyMode: Stop event detected for target {target_id}")
                    break
                
                # Check for new messages
                await asyncio.sleep(2)  # Check every 2 seconds
                new_messages = await client.get_messages(entity, min_id=last_message_id, limit=10)
                
                for msg in reversed(new_messages):
                    if msg.out:  # Skip our own messages
                        continue
                    
                    # Check if message matches any keyword
                    message_text = msg.message.lower() if msg.message else ""
                    reply_sent = False
                    
                    for keyword, reply_text in self.task.reply_keywords.items():
                        if keyword.lower() in message_text:
                            await client.send_message(entity, reply_text)
                            logger.info(f"ReplyMode: Sent keyword reply for '{keyword}' to target {target_id}")
                            reply_sent = True
                            break
                    
                    # Send default reply if no keyword matched
                    if not reply_sent and self.task.reply_default:
                        await client.send_message(entity, self.task.reply_default)
                        logger.info(f"ReplyMode: Sent default reply to target {target_id}")
                    
                    last_message_id = msg.id
            
            logger.info(f"ReplyMode: Monitoring ended for target {target_id}")
            return True
            
        except Exception as e:
            logger.error(f"ReplyMode: Error monitoring target {target_id}: {e}")
            return False
    
    def start_monitoring(self, client, entity, target_id, stop_event):
        """Start monitoring task in background"""
        task = asyncio.create_task(self.monitor_and_reply(client, entity, target_id, stop_event))
        self.monitoring_tasks[target_id] = task
        return task
    
    async def stop_all_monitoring(self):
        """Stop all monitoring tasks"""
        for target_id, task in self.monitoring_tasks.items():
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self.monitoring_tasks.clear()


# ============================================================================
# 账户管理类
# ============================================================================
class AccountManager:
    """Manage Telegram accounts"""
    
    def __init__(self, db):
        self.db = db
        self.accounts_col = db[Account.COLLECTION_NAME]
        self.clients = {}
        self.client_locks = {}  # Locks for preventing concurrent client creation
    
    async def send_code_request(self, phone, api_id=None, api_hash=None):
        """Send code to phone"""
        api_id = api_id or Config.API_ID
        api_hash = api_hash or Config.API_HASH
        
        session_name = f"session_{phone.replace('+', '')}"
        session_path = os.path.join(Config.SESSIONS_DIR, session_name)
        proxy = Config.get_proxy_dict()
        client = TelegramClient(session_path, api_id, api_hash, proxy=proxy)
        
        try:
            await client.connect()
            result = await client.send_code_request(phone)
            return {
                'status': 'success',
                'phone': phone,
                'client': client,
                'phone_code_hash': result.phone_code_hash
            }
        except Exception as e:
            logger.error(f"Error sending code: {e}")
            if client.is_connected():
                await client.disconnect()
            raise
    
    async def verify_code(self, phone, code, phone_code_hash, client, password=None):
        """Verify phone code"""
        try:
            await client.sign_in(phone, code, phone_code_hash=phone_code_hash)
        except SessionPasswordNeededError:
            if not password:
                return {'status': 'password_required', 'client': client}
            await client.sign_in(password=password)
        except PhoneCodeInvalidError:
            raise ValueError("Invalid code")
        
        me = await client.get_me()
        session_name = f"session_{phone.replace('+', '')}"
        account = Account(
            phone=phone,
            session_name=session_name,
            api_id=Config.API_ID,
            api_hash=Config.API_HASH,
            status=AccountStatus.ACTIVE.value
        )
        result = self.accounts_col.insert_one(account.to_dict())
        account._id = result.inserted_id
        self.clients[str(account._id)] = client
        
        return {'status': 'success', 'account': account, 'user': me}
    
    async def import_session_zip(self, zip_path, api_id=None, api_hash=None, account_type='messaging'):
        """Import sessions from zip"""
        logger.info(f"Starting session import from: {zip_path}")
        api_id = api_id or Config.API_ID
        api_hash = api_hash or Config.API_HASH
        imported = []
        temp_dir = os.path.join(Config.UPLOADS_DIR, 'temp')
        os.makedirs(temp_dir, exist_ok=True)
        logger.info(f"Created temporary directory: {temp_dir}")
        
        try:
            logger.info(f"Extracting zip file...")
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
            logger.info(f"Zip file extracted successfully")
            
            session_files = []
            for root, dirs, files in os.walk(temp_dir):
                for file in files:
                    if file.endswith('.session'):
                        session_files.append(os.path.join(root, file))
            
            logger.info(f"Found {len(session_files)} session files")
            
            for idx, session_path in enumerate(session_files, 1):
                logger.info(f"Verifying session {idx}/{len(session_files)}: {os.path.basename(session_path)}")
                result = await self._verify_session(session_path, api_id, api_hash, account_type)
                if result:
                    imported.append(result)
                    logger.info(f"Session verified successfully: {result['account'].phone}")
                else:
                    logger.warning(f"Session verification failed: {os.path.basename(session_path)}")
            
            logger.info(f"Import completed: {len(imported)}/{len(session_files)} sessions imported successfully")
            return imported
        finally:
            logger.info(f"Cleaning up temporary directory: {temp_dir}")
            shutil.rmtree(temp_dir, ignore_errors=True)
    
    async def _verify_session(self, session_path, api_id, api_hash, account_type='messaging'):
        """Verify session file"""
        logger.info(f"Connecting to Telegram with session: {os.path.basename(session_path)}")
        proxy = Config.get_proxy_dict()
        client = TelegramClient(session_path, api_id, api_hash, proxy=proxy)
        
        try:
            await client.connect()
            logger.info(f"Connected successfully, checking authorization...")
            
            if not await client.is_user_authorized():
                logger.warning(f"Session not authorized: {os.path.basename(session_path)}")
                return None
            
            me = await client.get_me()
            phone = me.phone if me.phone else f"user_{me.id}"
            logger.info(f"User info retrieved: {me.first_name} ({phone})")
            
            session_name = os.path.basename(session_path).replace('.session', '')
            new_path = os.path.join(Config.SESSIONS_DIR, f"{session_name}.session")
            shutil.copy2(session_path, new_path)
            logger.info(f"Session file copied to: {new_path}")
            
            # 确保状态设置为 ACTIVE
            account = Account(
                phone=phone,
                session_name=session_name,
                api_id=str(api_id),
                api_hash=api_hash,
                status=AccountStatus.ACTIVE.value,  # 明确设置为 ACTIVE
                account_type=account_type  # 设置账户类型
            )
            result = self.accounts_col.insert_one(account.to_dict())
            account._id = result.inserted_id
            # Mask phone number in logs for privacy (show only last 4 digits)
            masked_phone = f"***{phone[-4:]}" if phone and len(phone) >= 4 else "***"
            logger.info(f"Account saved to database: {masked_phone} with status: {account.status}, type: {account.account_type}")
            
            # 验证状态
            saved_account = self.accounts_col.find_one({'_id': result.inserted_id})
            if saved_account['status'] != AccountStatus.ACTIVE.value:
                logger.warning(f"Account {phone} status is not active after save: {saved_account['status']}")
            
            await client.disconnect()
            
            return {'account': account, 'user': me}
        except TypeNotFoundError as e:
            # Session file corrupted or incompatible Telethon version
            logger.error(
                f"Session file corrupted or incompatible: {os.path.basename(session_path)}\n"
                f"Error: {e}\n"
                f"This account needs to be re-logged in. Skipping..."
            )
            if client.is_connected():
                await client.disconnect()
            return None
        except Exception as e:
            logger.error(f"Error verifying session {os.path.basename(session_path)}: {e}", exc_info=True)
            if client.is_connected():
                await client.disconnect()
            return None
    
    async def get_client(self, account_id):
        """Get client for account with automatic proxy assignment and lock protection"""
        account_id_str = str(account_id)
        
        # Check if already connected (fast path, no lock needed)
        if account_id_str in self.clients and self.clients[account_id_str].is_connected():
            return self.clients[account_id_str]
        
        # Create lock for this account if doesn't exist
        if account_id_str not in self.client_locks:
            self.client_locks[account_id_str] = asyncio.Lock()
        
        # Acquire lock to prevent concurrent client creation
        async with self.client_locks[account_id_str]:
            # Double-check if another coroutine already created the client
            if account_id_str in self.clients and self.clients[account_id_str].is_connected():
                return self.clients[account_id_str]
            
            account_doc = self.accounts_col.find_one({'_id': ObjectId(account_id)})
        if not account_doc:
            raise ValueError(f"Account {account_id} not found")
        
        account = Account.from_dict(account_doc)
        session_path = os.path.join(Config.SESSIONS_DIR, account.session_name)
        
        # Auto-assign proxy from pool if not already assigned
        proxy = None
        proxy_obj = None
        
        if account.proxy_id:
            # Account already has a proxy assigned, verify it's still active
            try:
                proxy_id = account.proxy_id if isinstance(account.proxy_id, ObjectId) else ObjectId(account.proxy_id)
                proxy_doc = self.db[Proxy.COLLECTION_NAME].find_one({
                    '_id': proxy_id,
                    'is_active': True
                })
                if proxy_doc:
                    proxy_obj = Proxy.from_dict(proxy_doc)
                    proxy = proxy_obj.get_proxy_dict()
                    logger.info(f"Using assigned proxy for account {account.phone}: {proxy_obj.host}:{proxy_obj.port}")
                else:
                    logger.warning(f"Assigned proxy {account.proxy_id} not active, will get new one")
                    account.proxy_id = None  # Clear inactive proxy
            except Exception as e:
                logger.warning(f"Failed to load assigned proxy: {e}")
                account.proxy_id = None
        
        # If no valid proxy assigned, get one from pool
        if not proxy:
            proxy_obj = get_next_available_proxy(self.db)
            if proxy_obj:
                proxy = proxy_obj.get_proxy_dict()
                # Save proxy assignment to account
                self.accounts_col.update_one(
                    {'_id': ObjectId(account_id)},
                    {'$set': {'proxy_id': proxy_obj._id, 'updated_at': datetime.utcnow()}}
                )
                logger.info(f"Auto-assigned proxy to account {account.phone}: {proxy_obj.host}:{proxy_obj.port}")
            else:
                logger.warning(f"No proxies available in pool, will try without proxy")
        
        # Try to connect with proxy (if available)
        client = None
        connection_timeout = 30  # 30 seconds timeout
        
        if proxy:
            try:
                logger.info(f"Attempting connection with proxy for account {account.phone}")
                client = TelegramClient(session_path, int(account.api_id), account.api_hash, proxy=proxy)
                
                # Connect with timeout
                await asyncio.wait_for(client.connect(), timeout=connection_timeout)
                
                if await client.is_user_authorized():
                    logger.info(f"✅ Successfully connected with proxy: {proxy_obj.host}:{proxy_obj.port}")
                    # Update proxy success count
                    if proxy_obj:
                        self.db[Proxy.COLLECTION_NAME].update_one(
                            {'_id': proxy_obj._id},
                            {
                                '$inc': {'success_count': 1},
                                '$set': {'last_used': datetime.utcnow(), 'updated_at': datetime.utcnow()}
                            }
                        )
                    self.clients[account_id_str] = client
                    return client
                else:
                    logger.warning(f"Account not authorized with proxy, will try without proxy")
                    if client.is_connected():
                        await client.disconnect()
                    client = None
                    
            except asyncio.TimeoutError:
                logger.warning(f"⏱️ Proxy connection timeout after {connection_timeout}s, falling back to local")
                if proxy_obj:
                    # Update proxy fail count
                    self.db[Proxy.COLLECTION_NAME].update_one(
                        {'_id': proxy_obj._id},
                        {'$inc': {'fail_count': 1}, '$set': {'updated_at': datetime.utcnow()}}
                    )
                    # Check if should auto-delete after 3 failures
                    updated_proxy = self.db[Proxy.COLLECTION_NAME].find_one({'_id': proxy_obj._id})
                    if updated_proxy and updated_proxy.get('fail_count', 0) >= 3:
                        # Remove proxy from all accounts using it
                        self.db[Account.COLLECTION_NAME].update_many(
                            {'$or': [{'proxy_id': proxy_obj._id}, {'proxy_id': str(proxy_obj._id)}]},
                            {'$set': {'proxy_id': None}}
                        )
                        # Delete the proxy
                        self.db[Proxy.COLLECTION_NAME].delete_one({'_id': proxy_obj._id})
                        logger.warning(f"🗑️ Proxy {proxy_obj.host}:{proxy_obj.port} auto-deleted after 3 failures")
                if client and client.is_connected():
                    await client.disconnect()
                client = None
                
            except Exception as e:
                logger.warning(f"Proxy connection failed: {e}, falling back to local")
                if proxy_obj:
                    self.db[Proxy.COLLECTION_NAME].update_one(
                        {'_id': proxy_obj._id},
                        {'$inc': {'fail_count': 1}, '$set': {'updated_at': datetime.utcnow()}}
                    )
                    # Check if should auto-delete after 3 failures
                    updated_proxy = self.db[Proxy.COLLECTION_NAME].find_one({'_id': proxy_obj._id})
                    if updated_proxy and updated_proxy.get('fail_count', 0) >= 3:
                        # Remove proxy from all accounts using it
                        self.db[Account.COLLECTION_NAME].update_many(
                            {'$or': [{'proxy_id': proxy_obj._id}, {'proxy_id': str(proxy_obj._id)}]},
                            {'$set': {'proxy_id': None}}
                        )
                        # Delete the proxy
                        self.db[Proxy.COLLECTION_NAME].delete_one({'_id': proxy_obj._id})
                        logger.warning(f"🗑️ Proxy {proxy_obj.host}:{proxy_obj.port} auto-deleted after 3 failures")
                if client and client.is_connected():
                    await client.disconnect()
                client = None
        
        # Fallback: Connect without proxy (local)
        if not client:
            logger.info(f"🏠 Connecting locally (no proxy) for account {account.phone}")
            try:
                client = TelegramClient(session_path, int(account.api_id), account.api_hash, proxy=None)
                await client.connect()
                
                if not await client.is_user_authorized():
                    self.accounts_col.update_one(
                        {'_id': ObjectId(account_id)},
                        {'$set': {'status': AccountStatus.INACTIVE.value, 'updated_at': datetime.utcnow()}}
                    )
                    raise ValueError(f"Account {account_id} not authorized")
            except TypeNotFoundError as e:
                logger.error(
                    f"Session file corrupted or incompatible for account {account.phone}\n"
                    f"Error: {e}\n"
                    f"This account needs to be re-logged in."
                )
                self.accounts_col.update_one(
                    {'_id': ObjectId(account_id)},
                    {'$set': {'status': AccountStatus.INACTIVE.value, 'updated_at': datetime.utcnow()}}
                )
                raise ValueError(f"Session corrupted for account {account_id}, please re-login")
        
        self.clients[account_id_str] = client
        return client
    
    def _update_account_status(self, account_id, phone, new_status, reason, verify=False):
        """
        Helper method to update account status with logging and optional verification.
        
        Args:
            account_id: Account ID
            phone: Phone number (for logging)
            new_status: New status value (e.g., AccountStatus.ACTIVE.value)
            reason: Reason for status change (for logging)
            verify: Whether to verify the update (default: False for performance)
        """
        # Select emoji based on status type
        if new_status == AccountStatus.ACTIVE.value:
            status_emoji = '✅'
        elif new_status == AccountStatus.BANNED.value:
            status_emoji = '🚫'
        elif new_status == AccountStatus.LIMITED.value:
            status_emoji = '⚠️'
        elif new_status == AccountStatus.INACTIVE.value:
            status_emoji = '❌'
        else:
            status_emoji = '❓'
            
        logger.info(f"{status_emoji} Account {phone}: {reason}, updating status to {new_status}")
        
        self.accounts_col.update_one(
            {'_id': ObjectId(account_id)},
            {'$set': {'status': new_status, 'updated_at': datetime.utcnow()}}
        )
        
        # Optional verification (can be disabled for performance in production)
        if verify or logger.isEnabledFor(logging.DEBUG):
            updated_doc = self.accounts_col.find_one({'_id': ObjectId(account_id)})
            if updated_doc and 'status' in updated_doc:
                logger.debug(f"{status_emoji} Database verified: {phone} status = {updated_doc['status']}")
            else:
                logger.warning(f"{status_emoji} Database verification failed: document not found or missing status field")
    
    async def check_account_status(self, account_id):
        """
        Check account status by attempting to connect and get user info.
        
        Logic:
        - If get_me() succeeds → Account is ACTIVE (working)
        - If get_me() fails → Account is BANNED/INACTIVE (not working)
        
        Returns:
            bool: True if account is active, False if banned/inactive
        """
        account_doc = self.accounts_col.find_one({'_id': ObjectId(account_id)})
        if not account_doc:
            logger.error(f"Account {account_id} not found in database")
            return False
            
        account = Account.from_dict(account_doc)
        logger.info(f"Checking status for account {account.phone} (current status: {account.status})")
        
        try:
            # Try to get client and user info
            client = await self.get_client(account_id)
            me = await client.get_me()
            
            if me and me.id:
                # ✅ SUCCESS: Account can be accessed → Mark as ACTIVE
                self._update_account_status(
                    account_id, account.phone, AccountStatus.ACTIVE.value,
                    f"get_me() succeeded (user_id: {me.id})"
                )
                return True
            else:
                # ❌ FAILURE: get_me() returned None → Mark as BANNED
                self._update_account_status(
                    account_id, account.phone, AccountStatus.BANNED.value,
                    "get_me() returned None"
                )
                return False
                
        except Exception as e:
            # ❌ EXCEPTION: Cannot access account → Mark as BANNED
            self._update_account_status(
                account_id, account.phone, AccountStatus.BANNED.value,
                f"check failed with error: {e}"
            )
            return False
    
    def get_active_accounts(self):
        """Get active accounts"""
        docs = self.accounts_col.find({'status': AccountStatus.ACTIVE.value})
        return [Account.from_dict(doc) for doc in docs]
    
    async def disconnect_client(self, account_id):
        """Disconnect a specific client"""
        account_id_str = str(account_id)
        if account_id_str in self.clients:
            client = self.clients[account_id_str]
            try:
                if client.is_connected():
                    await client.disconnect()
                    logger.info(f"Disconnected client for account {account_id}")
            except Exception as e:
                logger.error(f"Error disconnecting client for account {account_id}: {e}")
            finally:
                del self.clients[account_id_str]
                if account_id_str in self.client_locks:
                    del self.client_locks[account_id_str]
    
    async def disconnect_all(self):
        """Disconnect all clients"""
        for account_id, client in list(self.clients.items()):
            try:
                if client.is_connected():
                    await client.disconnect()
            except Exception as e:
                logger.error(f"Error disconnecting client {account_id}: {e}")
        self.clients.clear()
        self.client_locks.clear()


# ============================================================================
# 任务管理类
# ============================================================================
class TaskManager:
    """任务管理器 - 管理所有私信任务的执行"""
    
    def __init__(self, db, account_manager, bot_application=None):
        self.db = db
        self.tasks_col = db[Task.COLLECTION_NAME]
        self.targets_col = db[Target.COLLECTION_NAME]
        self.logs_col = db[MessageLog.COLLECTION_NAME]
        self.account_manager = account_manager
        self.running_tasks = {}  # {task_id: {'asyncio_task': asyncio.Task, 'stop_event': asyncio.Event, 'started_at': datetime}}
        self.stop_flags = {}  # Keep for backward compatibility
        self.report_sent = set()  # Track which tasks have sent completion reports
        self.report_retry_count = {}  # Track report send retry attempts {task_id: count}
        self.bot_application = bot_application  # 用于发送完成报告
        self._account_check_cache = {}  # Cache for check_and_stop_if_no_accounts {task_id: {'result': bool, 'checked_at': datetime}}
        self.recent_logs = {}  # {task_id: [{'time': datetime, 'target': str, 'status': str, 'message': str, 'account': str}, ...]}
        self.stop_events = {}  # {task_id: asyncio.Event} - for reply monitoring
        self.current_account_info = {}  # {task_id: {'phone': str, 'sent_today': int, 'daily_limit': int}}
    
    def create_task(self, name, message_text, message_format, media_type=MediaType.TEXT,
                   media_path=None, send_method=SendMethod.DIRECT, postbot_code=None, 
                   channel_link=None, min_interval=30, max_interval=120):
        """Create new task"""
        task = Task(
            name=name,
            message_text=message_text,
            message_format=message_format.value if isinstance(message_format, enum.Enum) else message_format,
            media_type=media_type.value if isinstance(media_type, enum.Enum) else media_type,
            media_path=media_path,
            send_method=send_method.value if isinstance(send_method, enum.Enum) else send_method,
            postbot_code=postbot_code,
            channel_link=channel_link,
            min_interval=min_interval,
            max_interval=max_interval,
            status=TaskStatus.PENDING.value
        )
        result = self.tasks_col.insert_one(task.to_dict())
        task._id = result.inserted_id
        return task
    
    def add_targets(self, task_id, target_list):
        """Add targets to task"""
        task_doc = self.tasks_col.find_one({'_id': ObjectId(task_id)})
        if not task_doc:
            raise ValueError(f"Task {task_id} not found")
        
        unique_targets = set()
        for target in target_list:
            target = str(target).strip()
            if target.startswith('@'):
                target = target[1:]
            unique_targets.add(target)
        
        added_count = 0
        for target_str in unique_targets:
            if target_str.isdigit():
                target = Target(task_id=str(task_id), user_id=target_str)
            else:
                target = Target(task_id=str(task_id), username=target_str)
            self.targets_col.insert_one(target.to_dict())
            added_count += 1
        
        self.tasks_col.update_one(
            {'_id': ObjectId(task_id)},
            {'$set': {'total_targets': added_count, 'updated_at': datetime.utcnow()}}
        )
        return added_count
    
    def parse_target_file(self, file_content):
        """Parse targets from file"""
        lines = file_content.decode('utf-8').split('\n')
        targets = []
        for line in lines:
            line = line.strip()
            if line and not line.startswith('#'):
                targets.append(line)
        return targets
    
    async def check_phone_numbers(self, phone_numbers, account_id):
        """Check if phone numbers are registered on Telegram"""
        client = await self.account_manager.get_client(str(account_id))
        
        registered = []
        unregistered = []
        
        for phone in phone_numbers:
            try:
                # Try to get entity by phone number
                entity = await client.get_entity(phone)
                registered.append(phone)
                logger.info(f"Phone {phone} is registered on Telegram")
            except Exception as e:
                unregistered.append(phone)
                logger.info(f"Phone {phone} is not registered: {e}")
        
        return {
            'registered': registered,
            'unregistered': unregistered,
            'total': len(phone_numbers),
            'registered_count': len(registered),
            'unregistered_count': len(unregistered)
        }
    
    async def start_task(self, task_id):
        """Start task with dual stop mechanism"""
        task_doc = self.tasks_col.find_one({'_id': ObjectId(task_id)})
        if not task_doc:
            raise ValueError(f"Task {task_id} not found")
        
        task = Task.from_dict(task_doc)
        if task.status == TaskStatus.RUNNING.value:
            raise ValueError("Task already running")
        
        self.tasks_col.update_one(
            {'_id': ObjectId(task_id)},
            {'$set': {
                'status': TaskStatus.RUNNING.value,
                'started_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            }}
        )
        
        # Create stop event for immediate stopping
        stop_event = asyncio.Event()
        self.stop_flags[str(task_id)] = False  # Keep for backward compatibility
        
        # Create and store asyncio task with stop event
        asyncio_task = asyncio.create_task(self._execute_task(str(task_id), stop_event))
        self.running_tasks[str(task_id)] = {
            'asyncio_task': asyncio_task,
            'stop_event': stop_event,
            'started_at': datetime.utcnow()
        }
        return asyncio_task
    
    async def stop_task(self, task_id):
        """Stop task immediately with graceful + force cancellation (improved version)"""
        task_id_str = str(task_id)
        
        if task_id_str not in self.running_tasks:
            logger.warning(f"Task {task_id} not in running_tasks")
            # Even if not in running list, update database status
            self.tasks_col.update_one(
                {'_id': ObjectId(task_id)},
                {'$set': {
                    'status': TaskStatus.STOPPED.value,
                    'completed_at': datetime.utcnow(),
                    'updated_at': datetime.utcnow()
                }}
            )
            return
        
        task_info = self.running_tasks[task_id_str]
        
        logger.info(f"Task {task_id}: Initiating stop sequence...")
        
        # Validate task_info structure
        if not isinstance(task_info, dict):
            logger.error(f"Task {task_id}: Invalid task_info structure, expected dict (old format detected)")
            asyncio_task = task_info
        else:
            asyncio_task = task_info.get('asyncio_task')
        
        # 1. Set stop event (highest priority)
        if isinstance(task_info, dict) and 'stop_event' in task_info:
            task_info['stop_event'].set()
            logger.info(f"Task {task_id}: ✓ Stop event set")
        
        # 2. Set memory stop flag (backward compatibility)
        self.stop_flags[task_id_str] = True
        logger.info(f"Task {task_id}: ✓ Stop flag set")
        
        # 3. Update database status immediately
        self.tasks_col.update_one(
            {'_id': ObjectId(task_id)},
            {'$set': {
                'status': TaskStatus.STOPPED.value,
                'completed_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            }}
        )
        logger.info(f"Task {task_id}: ✓ Database status updated to STOPPED")
        
        # 4. Wait for graceful stop (reduced timeout to 3 seconds)
        try:
            await asyncio.wait_for(asyncio_task, timeout=3.0)
            logger.info(f"Task {task_id}: ✓ Stopped gracefully within 3s")
        except asyncio.TimeoutError:
            logger.warning(f"Task {task_id}: Timeout after 3s, forcing cancellation...")
            
            # 5. Force cancel the task
            asyncio_task.cancel()
            try:
                await asyncio_task
            except asyncio.CancelledError:
                logger.info(f"Task {task_id}: ✓ Cancelled successfully")
            except Exception as e:
                logger.error(f"Task {task_id}: Error during cancellation: {e}")
        
        # 6. Clean up running tasks record
        if task_id_str in self.running_tasks:
            del self.running_tasks[task_id_str]
            logger.info(f"Task {task_id}: ✓ Removed from running_tasks")
        
        if task_id_str in self.stop_flags:
            del self.stop_flags[task_id_str]
            logger.info(f"Task {task_id}: ✓ Removed stop_flag")
        
        logger.info(f"Task {task_id}: ✅ Stop sequence completed")
    
    def delete_task(self, task_id):
        """Delete task and all associated data"""
        task_id_str = str(task_id)
        
        # Check if task is running
        if task_id_str in self.running_tasks:
            raise ValueError("Cannot delete a running task. Please stop it first.")
        
        # Delete associated targets
        self.targets_col.delete_many({'task_id': task_id_str})
        
        # Delete associated message logs
        self.logs_col.delete_many({'task_id': task_id_str})
        
        # Delete the task itself
        result = self.tasks_col.delete_one({'_id': ObjectId(task_id)})
        
        if result.deleted_count == 0:
            raise ValueError(f"Task {task_id} not found")
        
        logger.info(f"Task {task_id} and all associated data deleted successfully")
        return True
    
    async def _sleep_with_stop_check(self, seconds, stop_event, task_id=None):
        """可中断的睡眠 - 每秒检查停止信号，每5秒检查数据库"""
        check_db_every = 5  # Check database every 5 seconds to reduce load
        for i in range(int(seconds)):
            if stop_event.is_set():
                logger.debug(f"Sleep interrupted by stop signal after {i}s")
                return True  # Return True if interrupted
            
            # Check database status less frequently for performance
            if task_id and i % check_db_every == 0:
                task_doc = self.tasks_col.find_one({'_id': ObjectId(task_id)})
                if task_doc and task_doc.get('status') == TaskStatus.STOPPED.value:
                    logger.debug(f"Sleep interrupted by database STOPPED status after {i}s")
                    return True
            
            await asyncio.sleep(1)
        
        # Handle remaining fractional seconds
        remaining = seconds - int(seconds)
        if remaining > 0 and not stop_event.is_set():
            await asyncio.sleep(remaining)
        
        return stop_event.is_set()  # Return True if stopped during remaining time
    
    async def _send_message_with_stop_check(self, task, target, account, stop_event):
        """发送消息（带停止检查）"""
        # Check before sending
        if stop_event.is_set():
            logger.debug("Send cancelled: stop signal detected before send")
            return False
        
        try:
            # Execute actual send
            success = await self._send_message_with_mode(task, target, account)
            return success
        except asyncio.CancelledError:
            logger.warning("Send message cancelled by task cancellation")
            raise
        except Exception as e:
            logger.error(f"Send message error: {e}")
            return False

    async def _execute_task(self, task_id, stop_event):
        """执行任务 - 支持重复发送模式和正常模式，使用双重停止机制"""
        task_doc = self.tasks_col.find_one({'_id': ObjectId(task_id)})
        task = Task.from_dict(task_doc)
        
        logger.info("=" * 80)
        logger.info("开始执行任务")
        logger.info(f"任务ID: {task_id}")
        logger.info(f"任务名称: {task.name}")
        logger.info(f"发送方式: {task.send_method}")
        logger.info(f"线程数配置: {task.thread_count}")
        logger.info(f"重复发送模式: {task.repeat_send}")
        logger.info("=" * 80)
        
        # 启动进度监控任务
        progress_task = asyncio.create_task(self._monitor_progress(task_id))
        logger.info("进度监控任务已启动")
        
        try:
            # Priority 1: Check stop event
            if stop_event.is_set():
                logger.info(f"Task {task_id}: Stop event detected before start")
                return
            
            # Priority 2: Check database status
            task_doc = self.tasks_col.find_one({'_id': ObjectId(task_id)})
            if not task_doc:
                logger.info(f"Task {task_id}: Task not found in database")
                return
            
            task = Task.from_dict(task_doc)
            if task.status != TaskStatus.RUNNING.value:
                logger.info(f"Task {task_id}: Status is {task.status}, not RUNNING")
                return
            
            # 获取待发送目标
            target_docs = self.targets_col.find({
                'task_id': task_id,
                'is_sent': False,
                'is_valid': True
            })
            targets = [Target.from_dict(doc) for doc in target_docs]
            
            logger.info(f"找到 {len(targets)} 个待发送目标")
            
            if not targets:
                logger.info("没有待发送目标，标记任务为已完成")
                self.tasks_col.update_one(
                    {'_id': ObjectId(task_id)},
                    {'$set': {
                        'status': TaskStatus.COMPLETED.value,
                        'completed_at': datetime.utcnow(),
                        'updated_at': datetime.utcnow()
                    }}
                )
                # 自动生成并发送完成报告
                logger.info("开始生成完成报告...")
                await self._send_completion_reports(task_id)
                return
            
            # 获取活跃账户
            accounts = self.account_manager.get_active_accounts()
            logger.info(f"活跃账户数量: {len(accounts)}")
            
            if not accounts:
                # 检查是否有任何账户
                all_accounts_count = self.db[Account.COLLECTION_NAME].count_documents({})
                logger.error(f"没有活跃账户可用！总账户数: {all_accounts_count}")
                
                if all_accounts_count == 0:
                    error_msg = "No accounts found. Please add accounts first."
                    logger.error(f"Task {task_id}: {error_msg}")
                    raise ValueError("❌ 没有找到任何账户！\n\n请先在【账户管理】中添加账户。")
                else:
                    # 有账户但都不是 active 状态
                    inactive_accounts = self.db[Account.COLLECTION_NAME].count_documents({'status': {'$ne': AccountStatus.ACTIVE.value}})
                    error_msg = f"Found {all_accounts_count} accounts, but none are active. {inactive_accounts} accounts are inactive/banned/limited."
                    logger.error(f"Task {task_id}: {error_msg}")
                    
                    # 获取账户状态统计
                    status_stats = {}
                    for status in AccountStatus:
                        count = self.db[Account.COLLECTION_NAME].count_documents({'status': status.value})
                        if count > 0:
                            status_stats[status.value] = count
                    
                    stats_text = "\n".join([f"  • {status}: {count}" for status, count in status_stats.items()])
                    raise ValueError(f"❌ 没有可用的活跃账户！\n\n账户状态统计：\n{stats_text}\n\n请检查账户状态或添加新账户。")
            
            # 根据任务模式选择不同的执行逻辑
            if task.force_private_mode:
                # 强制私信模式：连续失败计数
                await self._execute_force_send_mode(task_id, task, targets, accounts, stop_event)
            elif task.repeat_send:
                # 重复发送模式：所有账号轮流给所有用户发送
                await self._execute_repeat_send_mode(task_id, task, targets, accounts, stop_event)
            else:
                # 正常模式：每个用户按顺序尝试账号
                await self._execute_normal_mode(task_id, task, targets, accounts, stop_event)
            
            # Check if stopped before generating report
            if stop_event.is_set():
                logger.info(f"Task {task_id}: Stopped, skipping final completion")
                return
            
            # 获取最终任务状态
            task_doc = self.tasks_col.find_one({'_id': ObjectId(task_id)})
            task = Task.from_dict(task_doc)
            
            logger.info("=" * 80)
            logger.info("任务执行完成")
            logger.info(f"发送成功: {task.sent_count}")
            logger.info(f"发送失败: {task.failed_count}")
            logger.info(f"总计: {task.total_targets}")
            logger.info("=" * 80)
            
            # 更新任务状态为已完成
            self.tasks_col.update_one(
                {'_id': ObjectId(task_id)},
                {'$set': {
                    'status': TaskStatus.COMPLETED.value,
                    'completed_at': datetime.utcnow(),
                    'updated_at': datetime.utcnow()
                }}
            )
            
            # 自动生成并发送完成报告
            logger.info("开始生成并发送完成报告...")
            await self._send_completion_reports(task_id)
            
        except Exception as e:
            logger.error("=" * 80)
            logger.error(f"任务执行出错: {task_id}")
            logger.error(f"错误信息: {str(e)}")
            logger.error("=" * 80)
            logger.error("详细错误堆栈:", exc_info=True)
            
            self.tasks_col.update_one(
                {'_id': ObjectId(task_id)},
                {'$set': {'status': TaskStatus.FAILED.value, 'updated_at': datetime.utcnow()}}
            )
        finally:
            # 取消进度监控
            logger.info("正在停止进度监控...")
            progress_task.cancel()
            try:
                await progress_task
            except asyncio.CancelledError:
                pass
            
            if task_id in self.running_tasks:
                del self.running_tasks[task_id]
            if task_id in self.stop_flags:
                del self.stop_flags[task_id]
            logger.info(f"任务 {task_id}: 清理完成")
    
    async def _execute_repeat_send_mode(self, task_id, task, targets, accounts, stop_event):
        """执行重复发送模式：所有账号轮流给所有用户发送消息"""
        logger.info("=" * 80)
        logger.info("执行模式：重复发送")
        logger.info(f"目标用户数: {len(targets)}")
        logger.info(f"可用账号数: {len(accounts)}")
        logger.info(f"线程数: {task.thread_count}")
        logger.info("=" * 80)
        
        # 将账号分批，每批使用 thread_count 个账号
        batch_size = task.thread_count
        account_batches = [accounts[i:i + batch_size] for i in range(0, len(accounts), batch_size)]
        
        logger.info(f"账号分批: {len(account_batches)} 批，每批 {batch_size} 个账号")
        
        # 每批账号给所有用户发送
        for batch_index, account_batch in enumerate(account_batches):
            # Check stop event first
            if stop_event.is_set():
                logger.info("检测到停止事件，终止任务")
                break
            
            if self.stop_flags.get(task_id, False):
                logger.info("检测到停止标志，终止任务")
                break
            
            # Check database status
            task_doc = self.tasks_col.find_one({'_id': ObjectId(task_id)})
            if task_doc:
                task_status = Task.from_dict(task_doc).status
                if task_status != TaskStatus.RUNNING.value:
                    logger.info(f"任务状态变更为 {task_status}，停止执行")
                    break
            
            # 每10轮检查账号
            if batch_index > 0 and batch_index % Config.ACCOUNT_CHECK_LOOP_INTERVAL == 0:
                if await self.check_and_stop_if_no_accounts(task_id):
                    logger.info("所有账号不可用，任务已停止")
                    break
            
            logger.info("=" * 80)
            logger.info(f"第 {batch_index + 1}/{len(account_batches)} 轮")
            logger.info(f"使用账号: {[acc.phone for acc in account_batch]}")
            logger.info("=" * 80)
            
            # 每个账号并发发送给所有用户
            async def send_to_all_targets(account):
                """单个账号发送给所有目标"""
                logger.info(f"账号 {account.phone} 开始给所有用户发送")
                
                for target_idx, target in enumerate(targets):
                    # Check stop event
                    if stop_event.is_set():
                        logger.info(f"账号 {account.phone}: 检测到停止事件")
                        break
                    
                    if self.stop_flags.get(task_id, False):
                        logger.info(f"账号 {account.phone}: 检测到停止标志")
                        break
                    
                    # 检查每日限额
                    account_doc = self.db[Account.COLLECTION_NAME].find_one({'_id': account._id})
                    if account_doc:
                        account = Account.from_dict(account_doc)
                        if account.messages_sent_today >= account.daily_limit:
                            logger.warning(f"账号 {account.phone} 达到每日限额")
                            break
                        
                        # 重置每日计数器
                        if account.last_used and account.last_used.date() < datetime.utcnow().date():
                            self.db[Account.COLLECTION_NAME].update_one(
                                {'_id': account._id},
                                {'$set': {'messages_sent_today': 0, 'updated_at': datetime.utcnow()}}
                            )
                            account.messages_sent_today = 0
                    
                    # 发送消息 - Use stop-aware wrapper
                    logger.info(f"账号 {account.phone} -> 用户 {target.username or target.user_id} ({target_idx + 1}/{len(targets)})")
                    success = await self._send_message_with_stop_check(task, target, account, stop_event)
                    
                    if success:
                        self.tasks_col.update_one(
                            {'_id': ObjectId(task_id)},
                            {'$inc': {'sent_count': 1}, '$set': {'updated_at': datetime.utcnow()}}
                        )
                        self.db[Account.COLLECTION_NAME].update_one(
                            {'_id': account._id},
                            {
                                '$inc': {'messages_sent_today': 1, 'total_messages_sent': 1},
                                '$set': {'last_used': datetime.utcnow(), 'updated_at': datetime.utcnow()}
                            }
                        )
                        logger.info(f"✅ 发送成功")
                    else:
                        self.tasks_col.update_one(
                            {'_id': ObjectId(task_id)},
                            {'$inc': {'failed_count': 1}, '$set': {'updated_at': datetime.utcnow()}}
                        )
                        logger.warning(f"❌ 发送失败")
                    
                    # 更新账户最后使用时间
                    self.db[Account.COLLECTION_NAME].update_one(
                        {'_id': account._id},
                        {'$set': {'last_used': datetime.utcnow(), 'updated_at': datetime.utcnow()}}
                    )
                    
                    # 消息间隔
                    delay = random.randint(task.min_interval, task.max_interval)
                    logger.debug(f"等待 {delay} 秒...")
                    
                    # ✅ Use interruptible sleep for message interval
                    interrupted = await self._sleep_with_stop_check(delay, stop_event, task_id)
                    if interrupted:
                        logger.info(f"账号 {account.phone}: Stop signal during message interval")
                        break
                
                logger.info(f"账号 {account.phone} 完成所有发送")
            
            # 并发执行当前批次的所有账号，支持线程启动间隔
            concurrent_tasks = []
            for acc_idx, acc in enumerate(account_batch):
                # Apply thread start interval (except for first account)
                if acc_idx > 0 and task.thread_start_interval > 0:
                    logger.info(f"账号 {acc.phone}: 等待 {task.thread_start_interval} 秒后启动")
                    await asyncio.sleep(task.thread_start_interval)
                
                concurrent_tasks.append(send_to_all_targets(acc))
            
            await asyncio.gather(*concurrent_tasks, return_exceptions=True)
            
            logger.info(f"第 {batch_index + 1} 轮完成")
    
    async def _execute_normal_mode(self, task_id, task, targets, accounts, stop_event):
        """执行正常模式：每个用户按顺序尝试账号，直到成功或无账号可用"""
        logger.info("=" * 80)
        logger.info("执行模式：正常模式")
        logger.info(f"目标用户数: {len(targets)}")
        logger.info(f"可用账号数: {len(accounts)}")
        logger.info(f"线程数: {task.thread_count}")
        logger.info("=" * 80)
        
        # 使用线程数配置确定并发执行
        thread_count = min(task.thread_count, len(accounts))
        
        # 将目标分批处理，每批由一个账号处理
        batch_size = max(1, len(targets) // thread_count)
        batches = [targets[i:i + batch_size] for i in range(0, len(targets), batch_size)]
        logger.info(f"目标分批: {len(batches)} 批，每批约 {batch_size} 个目标")
        
        # 为每个批次创建并发任务
        concurrent_tasks = []
        for batch_idx, batch in enumerate(batches[:thread_count]):
            account = accounts[batch_idx % len(accounts)]
            logger.info(f"批次 {batch_idx + 1}: 分配账户 {account.phone}，处理 {len(batch)} 个目标")
            
            # Apply thread start interval (except for first batch)
            if batch_idx > 0 and task.thread_start_interval > 0:
                logger.info(f"批次 {batch_idx + 1}: 等待 {task.thread_start_interval} 秒后启动")
                await asyncio.sleep(task.thread_start_interval)
            
            concurrent_tasks.append(
                self._process_batch_normal_mode(task_id, task, batch, accounts, batch_idx, stop_event)
            )
        
        # 并发执行所有批次
        logger.info("=" * 80)
        logger.info(f"开始并发执行 {len(concurrent_tasks)} 个批次...")
        logger.info("=" * 80)
        await asyncio.gather(*concurrent_tasks, return_exceptions=True)
    
    async def _process_batch_normal_mode(self, task_id, task, targets, all_accounts, batch_idx, stop_event):
        """处理一批目标 - 正常模式：失败时尝试下一个账号"""
        logger.info(f"[批次 {batch_idx}] 开始处理 {len(targets)} 个目标")
        
        account_pool = all_accounts.copy()
        account_index = 0
        loop_count = 0
        consecutive_failures = 0
        
        for idx, target in enumerate(targets):
            # Priority 1: Check stop event
            if stop_event.is_set():
                logger.info(f"[批次 {batch_idx}] 检测到停止事件，停止执行")
                break
            
            # Priority 2: Check stop flag (backward compatibility)
            if self.stop_flags.get(task_id, False):
                logger.info(f"[批次 {batch_idx}] 检测到停止标志，停止执行")
                break
            
            # Priority 3: Check database status
            task_doc = self.tasks_col.find_one({'_id': ObjectId(task_id)})
            if task_doc:
                task_status = Task.from_dict(task_doc).status
                if task_status != TaskStatus.RUNNING.value:
                    logger.info(f"[批次 {batch_idx}] 任务状态变更为 {task_status}，停止执行")
                    break
            
            # 每10次循环检查账号
            loop_count += 1
            if loop_count % Config.ACCOUNT_CHECK_LOOP_INTERVAL == 0:
                if await self.check_and_stop_if_no_accounts(task_id):
                    logger.info(f"[批次 {batch_idx}] 所有账号不可用，任务已停止")
                    break
            
            logger.info(f"[批次 {batch_idx}] 处理目标 {idx + 1}/{len(targets)}: {target.username or target.user_id}")
            
            success = False
            attempts = 0
            max_attempts = len(account_pool)
            
            # 尝试多个账号直到成功
            while not success and attempts < max_attempts:
                account = account_pool[account_index % len(account_pool)]
                
                # 检查每日限额
                account_doc = self.db[Account.COLLECTION_NAME].find_one({'_id': account._id})
                if account_doc:
                    account = Account.from_dict(account_doc)
                    if account.messages_sent_today >= account.daily_limit:
                        logger.warning(f"[批次 {batch_idx}] 账户 {account.phone} 达到每日限额，尝试下一个账户")
                        account_index += 1
                        attempts += 1
                        continue
                    
                    # 重置每日计数器
                    if account.last_used and account.last_used.date() < datetime.utcnow().date():
                        self.db[Account.COLLECTION_NAME].update_one(
                            {'_id': account._id},
                            {'$set': {'messages_sent_today': 0, 'updated_at': datetime.utcnow()}}
                        )
                        account.messages_sent_today = 0
                
                # 发送消息 - Use stop-aware wrapper
                logger.info(f"[批次 {batch_idx}] 使用账户 {account.phone} 尝试发送")
                
                # Update current account info
                self._update_current_account(task_id, account)
                
                success = await self._send_message_with_stop_check(task, target, account, stop_event)
                
                if not success:
                    logger.warning(f"[批次 {batch_idx}] 账户 {account.phone} 发送失败，尝试下一个账户")
                    account_index += 1
                    attempts += 1
                else:
                    # 发送成功 - 重置连续失败计数
                    consecutive_failures = 0
                    self.tasks_col.update_one(
                        {'_id': ObjectId(task_id)},
                        {'$inc': {'sent_count': 1}, '$set': {'updated_at': datetime.utcnow()}}
                    )
                    self.db[Account.COLLECTION_NAME].update_one(
                        {'_id': account._id},
                        {
                            '$inc': {'messages_sent_today': 1, 'total_messages_sent': 1},
                            '$set': {'last_used': datetime.utcnow(), 'updated_at': datetime.utcnow()}
                        }
                    )
                    logger.info(f"[批次 {batch_idx}] ✅ 发送成功")
                    
                    # Batch pause mechanism (if configured)
                    if task.batch_pause_count > 0:
                        # Get current sent count
                        task_doc = self.tasks_col.find_one({'_id': ObjectId(task_id)})
                        if task_doc:
                            current_sent = task_doc.get('sent_count', 0)
                            if current_sent > 0 and current_sent % task.batch_pause_count == 0:
                                pause_delay = random.randint(task.batch_pause_min, task.batch_pause_max)
                                logger.info(f"[批次 {batch_idx}] 🛑 批次停顿: 已发送 {current_sent} 条，停顿 {pause_delay} 秒")
                                
                                # ✅ Use interruptible sleep during batch pause
                                interrupted = await self._sleep_with_stop_check(pause_delay, stop_event, task_id)
                                if interrupted:
                                    logger.info(f"[批次 {batch_idx}] Stop signal during batch pause")
                                    break
                    
                    # 消息间隔 - ✅ Use interruptible sleep
                    delay = random.randint(task.min_interval, task.max_interval)
                    interrupted = await self._sleep_with_stop_check(delay, stop_event, task_id)
                    if interrupted:
                        logger.info(f"[批次 {batch_idx}] Stop signal during message interval")
                        break
                    
                    break
            
            # 如果所有账号都尝试过仍然失败
            if not success:
                consecutive_failures += 1
                self.tasks_col.update_one(
                    {'_id': ObjectId(task_id)},
                    {'$inc': {'failed_count': 1}, '$set': {'updated_at': datetime.utcnow()}}
                )
                logger.warning(f"[批次 {batch_idx}] ❌ 所有账户尝试后仍然失败: {target.username or target.user_id}")
                
                # 检查连续失败次数
                if consecutive_failures >= Config.CONSECUTIVE_FAILURES_THRESHOLD:
                    logger.warning(f"[批次 {batch_idx}] 连续失败 {consecutive_failures} 次，检查账号可用性")
                    # 检查账号可用性（无论是否有成功发送）
                    if await self.check_and_stop_if_no_accounts(task_id):
                        logger.info(f"[批次 {batch_idx}] 所有账号不可用，任务已停止")
                        break
            
            # ✅ Check stop signal after each target processing
            if stop_event.is_set():
                logger.info(f"[批次 {batch_idx}] Stop signal detected after target {idx + 1}")
                break
        
        logger.info(f"[批次 {batch_idx}] 批次处理完成")
    
    async def _execute_force_send_mode(self, task_id, task, targets, accounts, stop_event):
        """执行强制私信模式：多账号并发，连续失败后查询 @spambot 判断账号状态"""
        # 使用 ignore_bidirectional_limit 作为连续失败上限
        consecutive_limit = task.ignore_bidirectional_limit if task.ignore_bidirectional_limit > 0 else DEFAULT_CONSECUTIVE_FAILURE_LIMIT
        
        logger.info("=" * 80)
        logger.info("执行模式：强制私信模式（多账号并发）")
        logger.info(f"目标用户数: {len(targets)}")
        logger.info(f"可用账号数: {len(accounts)}")
        logger.info(f"线程数: {task.thread_count}")
        logger.info(f"连续失败上限: {consecutive_limit}次")
        logger.info("=" * 80)
        
        # 将账号分批，每批使用 thread_count 个账号并发执行
        batch_size = task.thread_count
        account_batches = [accounts[i:i + batch_size] for i in range(0, len(accounts), batch_size)]
        
        logger.info(f"账号分批: {len(account_batches)} 批，每批 {batch_size} 个账号并发")
        
        for batch_index, account_batch in enumerate(account_batches):
            # Check stop event
            if stop_event.is_set():
                logger.info(f"Task {task_id}: Stop signal received")
                break
            
            # Check database status
            task_doc = self.tasks_col.find_one({'_id': ObjectId(task_id)})
            if task_doc:
                task_status = Task.from_dict(task_doc).status
                if task_status != TaskStatus.RUNNING.value:
                    logger.info(f"Task {task_id}: Status is {task_status}, not RUNNING")
                    break
            
            logger.info("=" * 80)
            logger.info(f"第 {batch_index + 1}/{len(account_batches)} 批账号开始工作")
            logger.info(f"使用账号: {[acc.phone for acc in account_batch]}")
            logger.info("=" * 80)
            
            # 为每个账号创建并发任务
            concurrent_tasks = []
            for acc_idx, acc in enumerate(account_batch):
                # Apply thread start interval (except for first account)
                if acc_idx > 0 and task.thread_start_interval > 0:
                    logger.info(f"账号 {acc.phone}: 等待 {task.thread_start_interval} 秒后启动")
                    await asyncio.sleep(task.thread_start_interval)
                
                concurrent_tasks.append(
                    self._process_account_force_mode(task_id, task, targets, acc, consecutive_limit, stop_event)
                )
            
            # 并发执行当前批次的所有账号
            await asyncio.gather(*concurrent_tasks, return_exceptions=True)
            
            logger.info(f"第 {batch_index + 1}/{len(account_batches)} 批账号完成工作")
        
        logger.info(f"Task {task_id}: Force send mode completed")
    
    async def _process_account_force_mode(self, task_id, task, targets, account, consecutive_limit, stop_event):
        """处理单个账号的强制私信任务"""
        consecutive_failures = 0  # 连续失败计数器
        
        logger.info(f"📱 账号 {account.phone} 开始工作")
        
        # Update current account info
        self._update_current_account(task_id, account)
        
        # 获取该账号应该发送的目标列表
        available_targets = self._get_available_targets_for_account(
            task_id,
            str(account._id),
            targets
        )
        
        if not available_targets:
            logger.info(f"账号 {account.phone} 没有可用目标，跳过")
            return
        
        logger.info(f"账号 {account.phone} 有 {len(available_targets)} 个可用目标")
        
        for idx, target in enumerate(available_targets):
            # Check stop signal
            if stop_event.is_set():
                logger.info(f"账号 {account.phone}: Stop signal detected")
                break
            
            # Check daily limit
            account_doc = self.db[Account.COLLECTION_NAME].find_one({'_id': account._id})
            if account_doc:
                account = Account.from_dict(account_doc)
                if account.messages_sent_today >= account.daily_limit:
                    logger.warning(f"账号 {account.phone} 达到每日限额")
                    break
                
                # Reset daily counter if needed
                if account.last_used and account.last_used.date() < datetime.utcnow().date():
                    self.db[Account.COLLECTION_NAME].update_one(
                        {'_id': account._id},
                        {'$set': {'messages_sent_today': 0, 'updated_at': datetime.utcnow()}}
                    )
                    account.messages_sent_today = 0
            
            # 发送消息
            logger.info(f"[{idx+1}/{len(available_targets)}] 账号 {account.phone} -> {target.username or target.user_id}")
            success = await self._send_message_with_stop_check(task, target, account, stop_event)
            
            if success:
                # ✅ 成功 → 计数器归零
                consecutive_failures = 0
                logger.info(
                    f"✅ [{idx+1}/{len(available_targets)}] "
                    f"账号 {account.phone} 成功发送给 {target.username or target.user_id}，"
                    f"连续失败计数归零"
                )
                
                # 更新目标状态
                self.targets_col.update_one(
                    {'_id': target._id},
                    {'$set': {
                        'is_sent': True,
                        'sent_at': datetime.utcnow(),
                        'last_account_id': str(account._id),
                        'updated_at': datetime.utcnow()
                    }}
                )
                
                # 更新任务计数
                self.tasks_col.update_one(
                    {'_id': ObjectId(task_id)},
                    {'$inc': {'sent_count': 1}, '$set': {'updated_at': datetime.utcnow()}}
                )
                
                # 更新账号统计
                self.db[Account.COLLECTION_NAME].update_one(
                    {'_id': account._id},
                    {
                        '$inc': {'messages_sent_today': 1, 'total_messages_sent': 1},
                        '$set': {'last_used': datetime.utcnow(), 'updated_at': datetime.utcnow()}
                    }
                )
                
            else:
                # ❌ 失败 → 计数器+1
                consecutive_failures += 1
                logger.warning(
                    f"❌ [{idx+1}/{len(available_targets)}] "
                    f"账号 {account.phone} 发送失败给 {target.username or target.user_id}，"
                    f"连续失败: {consecutive_failures}/{consecutive_limit}"
                )
                
                # 更新目标失败记录
                self.targets_col.update_one(
                    {'_id': target._id},
                    {
                        '$addToSet': {'failed_accounts': str(account._id)},
                        '$set': {
                            'last_error': getattr(target, 'last_error', DEFAULT_ERROR_MESSAGE),
                            'last_account_id': str(account._id),
                            'updated_at': datetime.utcnow()
                        },
                        '$inc': {'retry_count': 1}
                    }
                )
                
                # 更新任务失败计数
                self.tasks_col.update_one(
                    {'_id': ObjectId(task_id)},
                    {'$inc': {'failed_count': 1}, '$set': {'updated_at': datetime.utcnow()}}
                )
                
                # 检查是否达到连续失败上限
                if consecutive_failures >= consecutive_limit:
                    logger.warning(
                        f"🔍 账号 {account.phone} 连续失败 {consecutive_failures} 次，查询 @spambot 状态..."
                    )
                    
                    # 主动查询 @spambot 状态
                    spambot_status = await check_account_real_status(self.account_manager, str(account._id))
                    
                    if spambot_status == 'active':
                        # @spambot 说没有限制，重置计数器继续
                        consecutive_failures = 0
                        logger.info(
                            f"✅ @spambot 确认账号 {account.phone} 状态正常（no limits），"
                            f"重置连续失败计数，继续发送"
                        )
                    elif spambot_status in ['limited', 'banned']:
                        # @spambot 确认账号受限或被禁，停止该账号
                        logger.error(
                            f"🛑 @spambot 确认账号 {account.phone} 状态为 {spambot_status}，停用该账号"
                        )
                        
                        # 标记账号状态
                        status_value = AccountStatus.BANNED.value if spambot_status == 'banned' else AccountStatus.LIMITED.value
                        self.db[Account.COLLECTION_NAME].update_one(
                            {'_id': account._id},
                            {'$set': {
                                'status': status_value,
                                'updated_at': datetime.utcnow()
                            }}
                        )
                        
                        break  # 跳出循环，停止该账号
                    else:
                        # 状态未知，保守起见继续尝试但记录警告
                        logger.warning(
                            f"⚠️ 账号 {account.phone} 的 @spambot 状态未知，继续尝试"
                        )
            
            # 消息间隔
            delay = random.randint(task.min_interval, task.max_interval)
            interrupted = await self._sleep_with_stop_check(delay, stop_event, task_id)
            if interrupted:
                logger.info(f"账号 {account.phone}: Stop signal during interval")
                break
        
        logger.info(f"✅ 账号 {account.phone} 完成工作")
    
    def _get_available_targets_for_account(self, task_id, account_id, targets):
        """获取账号可用的目标列表（优先未尝试的）"""
        
        # 优先级1：从未被任何账号尝试过的目标
        never_tried = []
        # 优先级2：被其他账号失败但当前账号未尝试的目标
        failed_by_others = []
        
        for t in targets:
            if t.is_sent:
                continue
            
            failed_accounts = getattr(t, 'failed_accounts', [])
            
            if not failed_accounts:
                # 从未被任何账号尝试过
                never_tried.append(t)
            elif account_id not in failed_accounts:
                # 其他账号失败但当前账号未尝试
                failed_by_others.append(t)
        
        # 合并列表（优先级排序）
        available = never_tried + failed_by_others
        
        logger.info(
            f"账号 {account_id[-8:]} 可用目标分布：\n"
            f"  - 从未尝试: {len(never_tried)}\n"
            f"  - 其他账号失败: {len(failed_by_others)}\n"
            f"  - 总计: {len(available)}"
        )
        
        return available
    
    async def _process_batch(self, task_id, task, targets, account, batch_idx):
        """处理一批目标 - 使用单个账户"""
        logger.info(f"[批次 {batch_idx}] 开始处理 {len(targets)} 个目标，使用账户: {account.phone}")
        
        for idx, target in enumerate(targets):
            # 检查停止标志
            if self.stop_flags.get(task_id, False):
                logger.info(f"[批次 {batch_idx}] 检测到停止标志，停止执行")
                break
            
            logger.info(f"[批次 {batch_idx}] 处理目标 {idx + 1}/{len(targets)}: {target.username or target.user_id}")
            
            # 检查每日限额
            account_doc = self.db[Account.COLLECTION_NAME].find_one({'_id': account._id})
            if account_doc:
                account = Account.from_dict(account_doc)
                if account.messages_sent_today >= account.daily_limit:
                    logger.warning(f"[批次 {batch_idx}] 账户 {account.phone} 达到每日限额，停止批次")
                    break
                
                # 重置每日计数器（如果需要）
                if account.last_used and account.last_used.date() < datetime.utcnow().date():
                    logger.info(f"[批次 {batch_idx}] 重置账户 {account.phone} 的每日计数器")
                    self.db[Account.COLLECTION_NAME].update_one(
                        {'_id': account._id},
                        {'$set': {'messages_sent_today': 0, 'updated_at': datetime.utcnow()}}
                    )
                    account.messages_sent_today = 0
            
            # 发送消息
            logger.info(f"[批次 {batch_idx}] 正在发送消息到目标: {target.username or target.user_id}")
            success = await self._send_message_with_mode(task, target, account)
            
            if success:
                # 更新成功计数
                self.tasks_col.update_one(
                    {'_id': ObjectId(task_id)},
                    {'$inc': {'sent_count': 1}, '$set': {'updated_at': datetime.utcnow()}}
                )
                self.db[Account.COLLECTION_NAME].update_one(
                    {'_id': account._id},
                    {
                        '$inc': {'messages_sent_today': 1, 'total_messages_sent': 1},
                        '$set': {'last_used': datetime.utcnow(), 'updated_at': datetime.utcnow()}
                    }
                )
                logger.info(f"[批次 {batch_idx}] ✅ 发送成功: {target.username or target.user_id}")
            else:
                # 更新失败计数
                self.tasks_col.update_one(
                    {'_id': ObjectId(task_id)},
                    {'$inc': {'failed_count': 1}, '$set': {'updated_at': datetime.utcnow()}}
                )
                logger.warning(f"[批次 {batch_idx}] ❌ 发送失败: {target.username or target.user_id}")
            
            # 更新账户最后使用时间
            self.db[Account.COLLECTION_NAME].update_one(
                {'_id': account._id},
                {'$set': {'last_used': datetime.utcnow(), 'updated_at': datetime.utcnow()}}
            )
            
            # 消息间隔延迟
            delay = random.randint(task.min_interval, task.max_interval)
            logger.info(f"[批次 {batch_idx}] 等待 {delay} 秒后发送下一条消息...")
            await asyncio.sleep(delay)
        
        logger.info(f"[批次 {batch_idx}] 批次处理完成")
    
    async def _monitor_progress(self, task_id):
        """监控和更新任务进度 - 使用30-60秒随机间隔"""
        try:
            while True:
                # Use random interval between 30-60 seconds
                interval = random.randint(30, 60)
                await asyncio.sleep(interval)
                # 进度在 _process_batch 中自动更新
                # 这里只是保持监控任务活跃
                logger.debug(f"任务 {task_id}: 进度监控心跳 (下次检查间隔: {interval}秒)")
        except asyncio.CancelledError:
            logger.info(f"Task {task_id}: Progress monitor cancelled")
            raise
    
    async def check_accounts_availability(self):
        """Check if any account is available - optimized with find_one"""
        # Use find_one instead of count_documents for better performance
        available = self.db[Account.COLLECTION_NAME].find_one({
            'status': AccountStatus.ACTIVE.value
        })
        return available is not None
    
    async def check_and_stop_if_no_accounts(self, task_id):
        """Check accounts and stop task if all unavailable - with detailed reason and 30s cache"""
        # Check cache (30 seconds)
        task_id_str = str(task_id)
        if task_id_str in self._account_check_cache:
            cached = self._account_check_cache[task_id_str]
            cache_age = (datetime.utcnow() - cached['checked_at']).total_seconds()
            if cache_age < Config.ACCOUNT_STATUS_CHECK_CACHE_DURATION:
                logger.debug(f"Task {task_id}: Using cached account check result")
                return cached['result']
        
        if not await self.check_accounts_availability():
            logger.error(f"Task {task_id}: All accounts unavailable")
            
            # 获取账户状态统计
            total_accounts = self.db[Account.COLLECTION_NAME].count_documents({})
            banned_count = self.db[Account.COLLECTION_NAME].count_documents({
                'status': AccountStatus.BANNED.value
            })
            limited_count = self.db[Account.COLLECTION_NAME].count_documents({
                'status': AccountStatus.LIMITED.value
            })
            inactive_count = self.db[Account.COLLECTION_NAME].count_documents({
                'status': AccountStatus.INACTIVE.value
            })
            
            # 构建详细的停止原因
            reason_parts = []
            if banned_count > 0:
                reason_parts.append(f"封禁: {banned_count}")
            if limited_count > 0:
                reason_parts.append(f"受限: {limited_count}")
            if inactive_count > 0:
                reason_parts.append(f"未激活: {inactive_count}")
            
            detailed_reason = f"所有账号均无法发送消息 (总计: {total_accounts}, {', '.join(reason_parts)})"
            
            # 标记任务失败
            self.tasks_col.update_one(
                {'_id': ObjectId(task_id)},
                {
                    '$set': {
                        'status': TaskStatus.FAILED.value,
                        'completed_at': datetime.utcnow(),
                        'error_message': detailed_reason
                    }
                }
            )
            
            # 发送通知到管理员（如果bot_application可用）
            if self.bot_application:
                try:
                    await self.bot_application.bot.send_message(
                        Config.ADMIN_USER_ID,
                        f"❌ <b>任务自动停止</b>\n\n"
                        f"原因：{detailed_reason}\n\n"
                        f"📊 账户状态详情：\n"
                        f"• 总账户数: {total_accounts}\n"
                        f"• 🚫 封禁: {banned_count}\n"
                        f"• ⚠️ 受限: {limited_count}\n"
                        f"• ❄️ 未激活: {inactive_count}\n\n"
                        f"💡 建议：\n"
                        f"1. 使用 '检查账户状态' 功能查询 @spambot\n"
                        f"2. 添加新的可用账户\n"
                        f"3. 等待受限账户恢复",
                        parse_mode='HTML'
                    )
                except Exception as e:
                    logger.error(f"Failed to send admin notification: {e}")
            
            # 生成报告
            await self._send_completion_reports(task_id)
            
            # Cache result
            self._account_check_cache[task_id_str] = {
                'result': True,
                'checked_at': datetime.utcnow()
            }
            
            return True
        
        # Cache result
        self._account_check_cache[task_id_str] = {
            'result': False,
            'checked_at': datetime.utcnow()
        }
        
        return False
    
    async def generate_failed_targets_report(self, task_id):
        """生成失败用户报告（按失败原因分类）"""
        
        # 查询所有失败的目标（有重试但未成功）
        failed_targets = list(self.targets_col.find({
            'task_id': str(task_id),
            'is_sent': False,
            'retry_count': {'$gt': 0}  # 至少被尝试过一次
        }))
        
        if not failed_targets:
            return "✅ 没有失败的用户"
        
        # 按失败原因分类
        failed_by_reason = {}
        for target in failed_targets:
            reason = target.get('last_error', DEFAULT_ERROR_MESSAGE)
            if reason not in failed_by_reason:
                failed_by_reason[reason] = []
            failed_by_reason[reason].append(target)
        
        # 生成报告
        report_lines = [
            f"❌ <b>失败用户报告</b>",
            f"",
            f"总计失败: {len(failed_targets)} 个用户",
            f""
        ]
        
        for reason, targets_list in failed_by_reason.items():
            report_lines.append(f"<b>{reason}</b>: {len(targets_list)}个")
            
            # 列出用户名（最多显示5个）
            usernames = [t.get('username', t.get('user_id', 'Unknown')) for t in targets_list[:5]]
            report_lines.append(f"  用户: {', '.join(usernames)}")
            
            if len(targets_list) > 5:
                report_lines.append(f"  ... 还有 {len(targets_list) - 5} 个")
            
            report_lines.append("")
        
        return "\n".join(report_lines)
    
    async def export_failed_targets_csv(self, task_id):
        """导出失败用户列表为CSV"""
        import io
        import csv
        
        failed_targets = list(self.targets_col.find({
            'task_id': str(task_id),
            'is_sent': False,
            'retry_count': {'$gt': 0}
        }))
        
        if not failed_targets:
            return None
        
        # 使用StringIO和CSV writer来正确处理转义
        output = io.StringIO()
        writer = csv.writer(output)
        
        # 写入标题
        writer.writerow(['用户名', '用户ID', '失败原因', '尝试次数', '失败账号数'])
        
        # 写入数据
        for target in failed_targets:
            username = target.get('username', '')
            user_id = target.get('user_id', '')
            last_error = target.get('last_error', DEFAULT_ERROR_MESSAGE)
            retry_count = target.get('retry_count', 0)
            failed_accounts_count = len(target.get('failed_accounts', []))
            
            writer.writerow([username, user_id, last_error, retry_count, failed_accounts_count])
        
        # 创建文件对象 (using utf-8-sig encoding for Excel compatibility - adds BOM)
        csv_content = output.getvalue()
        file = io.BytesIO(csv_content.encode('utf-8-sig'))
        file.name = f"failed_targets_{task_id}.csv"
        
        return file
    
    async def _send_completion_reports(self, task_id):
        """生成并自动发送完成报告 - 任务完成后自动执行，防止重复发送"""
        # Prevent duplicate reports
        if task_id in self.report_sent:
            logger.info(f"任务 {task_id}: 报告已发送，跳过重复发送")
            return
        
        # Check retry limit
        retry_count = self.report_retry_count.get(task_id, 0)
        if retry_count >= Config.MAX_REPORT_RETRY_ATTEMPTS:
            logger.error(f"任务 {task_id}: 达到最大重试次数 ({Config.MAX_REPORT_RETRY_ATTEMPTS})，停止发送报告")
            return
        
        self.report_sent.add(task_id)
        
        try:
            logger.info(f"========================================")
            logger.info(f"任务完成 - 开始生成报告")
            logger.info(f"任务ID: {task_id}")
            logger.info(f"尝试次数: {retry_count + 1}/{Config.MAX_REPORT_RETRY_ATTEMPTS}")
            logger.info(f"========================================")
            
            # Get task info for message count
            task_doc = self.tasks_col.find_one({'_id': ObjectId(task_id)})
            if not task_doc:
                logger.warning(f"任务 {task_id}: 任务不存在")
                return
            task = Task.from_dict(task_doc)
            
            results = self.export_task_results(task_id)
            if not results:
                logger.warning(f"任务 {task_id}: 无结果可导出")
                return
            
            # 生成时间戳
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            logger.info(f"报告时间戳: {timestamp}")
            
            # 生成4个报告文件: 成功/失败/剩余用户列表 + 运行日志
            success_file = os.path.join(Config.RESULTS_DIR, f"发送成功的用户名_{task_id}_{timestamp}.txt")
            failed_file = os.path.join(Config.RESULTS_DIR, f"发送失败的用户名_{task_id}_{timestamp}.txt")
            remaining_file = os.path.join(Config.RESULTS_DIR, f"剩余未发送的用户名_{task_id}_{timestamp}.txt")
            log_file = os.path.join(Config.RESULTS_DIR, f"任务运行日志_{task_id}_{timestamp}.txt")
            
            # 写入成功用户列表
            logger.info(f"生成成功用户列表: {len(results['success_targets'])} 个用户")
            with open(success_file, 'w', encoding='utf-8') as f:
                f.write(f"任务完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"总成功数: {len(results['success_targets'])}\n")
                f.write("=" * 50 + "\n\n")
                for t in results['success_targets']:
                    f.write(f"{t.username or t.user_id}\n")
            
            # 写入失败用户列表
            logger.info(f"生成失败用户列表: {len(results['failed_targets'])} 个用户")
            with open(failed_file, 'w', encoding='utf-8') as f:
                f.write(f"任务完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"总失败数: {len(results['failed_targets'])}\n")
                f.write("=" * 50 + "\n\n")
                for t in results['failed_targets']:
                    f.write(f"{t.username or t.user_id}: {t.error_message or '未知错误'}\n")
            
            # 写入剩余未发送用户列表
            logger.info(f"生成剩余用户列表: {len(results['remaining_targets'])} 个用户")
            with open(remaining_file, 'w', encoding='utf-8') as f:
                f.write(f"任务完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"总剩余数: {len(results['remaining_targets'])}\n")
                f.write(f"说明: 这些用户尚未发送，可用于下次任务\n")
                f.write("=" * 50 + "\n\n")
                for t in results['remaining_targets']:
                    f.write(f"{t.username or t.user_id}\n")
            
            # 写入运行日志 - 详细版本
            logger.info(f"生成运行日志: {len(results['logs'])} 条记录")
            with open(log_file, 'w', encoding='utf-8') as f:
                f.write(f"任务运行日志\n")
                f.write(f"任务ID: {task_id}\n")
                f.write(f"完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("=" * 50 + "\n\n")
                
                # 预先批量获取所有账户和目标信息（避免N+1查询）
                unique_account_ids = list(set([log.account_id for log in results['logs'] if log.account_id]))
                unique_target_ids = list(set([log.target_id for log in results['logs'] if log.target_id]))
                
                # 批量查询账户信息 - 安全转换ObjectId
                valid_account_ids = []
                for aid in unique_account_ids:
                    if aid and isinstance(aid, str) and len(aid) == 24:  # MongoDB ObjectId是24位十六进制字符串
                        try:
                            valid_account_ids.append(ObjectId(aid))
                        except Exception:
                            pass
                
                account_docs = self.db[Account.COLLECTION_NAME].find({
                    '_id': {'$in': valid_account_ids}
                })
                accounts_map = {str(doc['_id']): Account.from_dict(doc) for doc in account_docs}
                
                # 批量查询目标信息 - 安全转换ObjectId
                valid_target_ids = []
                for tid in unique_target_ids:
                    if tid and isinstance(tid, str) and len(tid) == 24:
                        try:
                            valid_target_ids.append(ObjectId(tid))
                        except Exception:
                            pass
                
                target_docs = self.targets_col.find({
                    '_id': {'$in': valid_target_ids}
                })
                targets_map = {str(doc['_id']): Target.from_dict(doc) for doc in target_docs}
                
                # 统计每个账户的发送情况
                account_stats = {}
                for log in results['logs']:
                    account_id = log.account_id
                    if account_id not in account_stats:
                        # 从预加载的账户信息中获取
                        account = accounts_map.get(account_id)
                        if account:
                            account_stats[account_id] = {
                                'phone': account.phone,
                                'success': 0,
                                'failed': 0,
                                'errors': {}
                            }
                        else:
                            account_stats[account_id] = {
                                'phone': 'Unknown',
                                'success': 0,
                                'failed': 0,
                                'errors': {}
                            }
                    
                    if log.success:
                        account_stats[account_id]['success'] += 1
                    else:
                        account_stats[account_id]['failed'] += 1
                        # 分类错误原因
                        error_type = self._categorize_error(log.error_message)
                        if error_type not in account_stats[account_id]['errors']:
                            account_stats[account_id]['errors'][error_type] = 0
                        account_stats[account_id]['errors'][error_type] += 1
                
                # 写入账户统计
                f.write("📊 账户统计:\n")
                f.write("-" * 50 + "\n")
                for account_id, stats in account_stats.items():
                    f.write(f"\n📱 账户: {stats['phone']}\n")
                    f.write(f"   ✅ 已成功发送: {stats['success']}条\n")
                    f.write(f"   ❌ 发送失败: {stats['failed']}条\n")
                    if stats['errors']:
                        f.write(f"   失败原因统计:\n")
                        for error_type, count in stats['errors'].items():
                            f.write(f"      • {error_type}: {count}次\n")
                f.write("\n" + "=" * 50 + "\n\n")
                
                # 写入详细日志
                f.write("📝 详细发送记录:\n")
                f.write("-" * 50 + "\n\n")
                for log in results['logs']:
                    # 从预加载的数据中获取账户信息
                    account_id = log.account_id
                    phone = account_stats.get(account_id, {}).get('phone', 'Unknown')
                    
                    # 从预加载的数据中获取目标用户信息
                    target = targets_map.get(log.target_id)
                    target_name = "Unknown"
                    if target:
                        target_name = target.username or target.user_id or "Unknown"
                    
                    status = "✅ 成功" if log.success else "❌ 失败"
                    
                    # 格式化消息内容预览（最多50个字符），处理None情况
                    message_text = log.message_text or ""
                    message_preview = (message_text[:50] + "...") if len(message_text) > 50 else message_text
                    
                    f.write(f"[{log.sent_at}]\n")
                    f.write(f"账户: {phone}\n")
                    f.write(f"目标: {target_name}\n")
                    f.write(f"状态: {status}\n")
                    
                    if log.success:
                        f.write(f"私信内容: {message_preview}\n")
                    else:
                        error_category = self._categorize_error(log.error_message)
                        f.write(f"失败原因: {error_category}\n")
                        f.write(f"详细错误: {log.error_message}\n")
                    
                    f.write("\n")
            
            # 如果有bot_application，自动发送报告给管理员
            if self.bot_application and Config.ADMIN_USER_ID:
                logger.info(f"========================================")
                logger.info(f"自动发送报告给管理员")
                logger.info(f"管理员ID: {Config.ADMIN_USER_ID}")
                logger.info(f"========================================")
                
                # 发送完成消息
                # Calculate unique users who received messages
                unique_users = len(results['success_targets'])
                total_messages = task.sent_count  # Total messages sent (including repeat sends)
                remaining_count = len(results['remaining_targets'])
                
                # Context-aware completion message
                if remaining_count == 0:
                    status_emoji = "🎉"
                    status_msg = "任务完成，用户名已用完！"
                elif task.status == TaskStatus.STOPPED.value:
                    status_emoji = "⏸️"
                    status_msg = "任务已手动停止"
                elif task.status == TaskStatus.FAILED.value:
                    status_emoji = "❌"
                    status_msg = "任务失败"
                else:
                    status_emoji = "✅"
                    status_msg = "任务完成！"
                
                # Calculate runtime and speed
                runtime_str = "未知"
                speed_str = "0.0 条/分钟"
                if task.started_at and task.completed_at:
                    runtime = task.completed_at - task.started_at
                    hours, remainder = divmod(int(runtime.total_seconds()), 3600)
                    minutes, seconds = divmod(remainder, 60)
                    runtime_str = f"{hours}:{minutes:02d}:{seconds:02d}"
                    
                    # Calculate speed
                    if total_messages > 0 and runtime.total_seconds() > 0:
                        speed = total_messages / runtime.total_seconds() * 60  # messages per minute
                        speed_str = f"{speed:.1f} 条/分钟"
                
                # Build failure reason summary
                failure_summary = ""
                error_categories = {}
                for log in results['logs']:
                    if not log.success:
                        error_type = self._categorize_error(log.error_message)
                        error_categories[error_type] = error_categories.get(error_type, 0) + 1
                
                if error_categories:
                    failure_summary = "\n\n📋 <b>失败原因分类</b>:\n"
                    for error_type, count in sorted(error_categories.items(), key=lambda x: x[1], reverse=True):
                        failure_summary += f"• {error_type}: {count} 次\n"
                
                # Build account summary
                account_summary = ""
                if account_stats:
                    account_summary = "\n\n📱 <b>账号统计</b>:\n"
                    for account_id, stats in list(account_stats.items())[:Config.MAX_DISPLAYED_ACCOUNTS]:  # Show top N accounts
                        total = stats['success'] + stats['failed']
                        account_summary += f"• {stats['phone']}: 成功{stats['success']}/失败{stats['failed']} (共{total})\n"
                
                completion_text = (
                    f"{status_emoji} <b>{status_msg}</b>\n\n"
                    f"📊 <b>任务统计</b>:\n"
                    f"✅ 发送成功: {total_messages} 条消息\n"
                    f"📧 成功用户: {unique_users} 人\n"
                    f"❌ 发送失败: {len(results['failed_targets'])} 人\n"
                    f"⏸️ 剩余未发送: {remaining_count} 人\n\n"
                    f"⏱️ <b>时间统计</b>:\n"
                    f"• 运行时间: {runtime_str}\n"
                    f"• 平均速度: {speed_str}\n"
                    f"{account_summary}"
                    f"{failure_summary}\n\n"
                    f"📁 正在发送日志报告..."
                )
                
                try:
                    await self.bot_application.bot.send_message(
                        chat_id=Config.ADMIN_USER_ID,
                        text=completion_text,
                        parse_mode='HTML'
                    )
                    logger.info("完成消息已发送")
                except Exception as e:
                    logger.error(f"发送完成消息失败: {e}")
                
                # 发送4个文件（添加剩余用户名文件）
                files_to_send = [
                    (success_file, "发送成功的用户名.txt"),
                    (failed_file, "发送失败的用户名.txt"),
                    (remaining_file, "剩余未发送的用户名.txt"),
                    (log_file, "任务运行日志.txt")
                ]
                
                for file_path, filename in files_to_send:
                    try:
                        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
                            logger.info(f"发送文件: {filename}")
                            with open(file_path, 'rb') as f:
                                await self.bot_application.bot.send_document(
                                    chat_id=Config.ADMIN_USER_ID,
                                    document=f,
                                    filename=filename,
                                    caption=f"📄 {filename}"
                                )
                            logger.info(f"文件发送成功: {filename}")
                        else:
                            logger.warning(f"文件为空或不存在: {filename}")
                    except Exception as e:
                        logger.error(f"发送文件失败 {filename}: {e}")
                
                # 发送失败用户报告（如果启用了强制私信模式）
                task_doc = self.tasks_col.find_one({'_id': ObjectId(task_id)})
                if task_doc:
                    task = Task.from_dict(task_doc)
                    if task.force_private_mode:
                        try:
                            # 导出失败用户CSV
                            logger.info("导出失败用户CSV...")
                            csv_file = await self.export_failed_targets_csv(task_id)
                            if csv_file:
                                await self.bot_application.bot.send_document(
                                    chat_id=Config.ADMIN_USER_ID,
                                    document=csv_file,
                                    caption=f"📄 失败用户列表详情",
                                    filename=csv_file.name
                                )
                                logger.info("失败用户CSV已发送")
                        except Exception as e:
                            logger.error(f"发送失败报告出错: {e}")
                
                logger.info("========================================")
                logger.info("所有报告文件已发送完成")
                logger.info("========================================")
            else:
                logger.info("未配置bot_application或ADMIN_USER_ID，报告文件已生成但未自动发送")
            
        except Exception as e:
            logger.error(f"任务 {task_id}: 生成完成报告出错: {e}", exc_info=True)
            # Remove from report_sent and increment retry count
            self.report_sent.discard(task_id)
            self.report_retry_count[task_id] = retry_count + 1
            logger.info(f"任务 {task_id}: 报告发送失败，将在下次尝试 (剩余重试: {Config.Config.MAX_REPORT_RETRY_ATTEMPTS - self.report_retry_count[task_id]})")
    
    async def _send_with_voice_call(self, task, target, account):
        """Send message with voice call"""
        try:
            client = await self.account_manager.get_client(str(account._id))
            recipient = int(target.user_id) if target.user_id else target.username
            
            # Get entity
            entity = await client.get_entity(recipient)
            
            # Make voice call
            logger.info(f"VoiceCall: Initiating call to {recipient}")
            try:
                call = await client.call(entity, duration=task.voice_call_duration)
                logger.info(f"VoiceCall: Call initiated successfully, waiting {task.voice_call_wait_after}s")
                await asyncio.sleep(task.voice_call_wait_after)
                
                # Send message after call
                return await self._send_message(task, target, account)
                
            except Exception as call_error:
                logger.warning(f"VoiceCall: Failed to call {recipient}: {call_error}")
                
                # Send message anyway if configured
                if task.voice_call_send_if_failed:
                    logger.info(f"VoiceCall: Sending message despite call failure")
                    return await self._send_message(task, target, account)
                else:
                    return False
                    
        except Exception as e:
            logger.error(f"VoiceCall: Error in voice call flow: {e}")
            # Try to send message anyway
            if task.voice_call_send_if_failed:
                return await self._send_message(task, target, account)
            return False
    
    async def _send_message_with_mode(self, task, target, account):
        """Send message with appropriate mode (voice call, edit, reply, or normal)"""
        # Check if voice call is enabled
        if getattr(task, 'voice_call_enabled', False):
            return await self._send_with_voice_call(task, target, account)
        
        # Check if edit mode is enabled
        message_mode = getattr(task, 'message_mode', 'normal')
        if message_mode == 'edit' and getattr(task, 'edit_content', None):
            return await self._send_message_with_edit(task, target, account)
        
        # Normal send
        success = await self._send_message(task, target, account)
        
        # Start reply monitoring if configured and send was successful
        if success:
            reply_keywords = getattr(task, 'reply_keywords', None)
            reply_default = getattr(task, 'reply_default', None)
            if reply_keywords or reply_default:
                await self._start_reply_monitoring(task, target, account)
        
        return success
    
    async def _send_message_with_edit(self, task, target, account):
        """Send message in edit mode - send initial message then edit it"""
        try:
            logger.info(f"使用编辑模式发送消息给 {target.username or target.user_id}")
            client = await self.account_manager.get_client(str(account._id))
            
            # 确定接收者
            recipient = int(target.user_id) if target.user_id else target.username
            entity = await client.get_entity(recipient)
            
            # 提取用户信息用于消息个性化
            user_info = MessageFormatter.extract_user_info(entity)
            
            # 个性化消息内容
            initial_message = MessageFormatter.personalize(task.message_text, user_info)
            edit_content = MessageFormatter.personalize(task.edit_content, user_info)
            parse_mode = MessageFormatter.get_parse_mode(task.message_format)
            
            # Create EditMode instance
            edit_mode = EditMode(task, self.account_manager)
            
            # Send and schedule edit
            success = await edit_mode.send_and_schedule_edit(
                client, entity, str(target._id), initial_message, edit_content
            )
            
            if success:
                self.targets_col.update_one(
                    {'_id': target._id},
                    {'$set': {'is_sent': True, 'sent_at': datetime.utcnow()}}
                )
                self._log_message(str(task._id), str(account._id), str(target._id), initial_message, True, None)
                logger.info(f"编辑模式消息发送成功: {recipient}")
            else:
                self._log_message(str(task._id), str(account._id), str(target._id), initial_message, False, "Edit failed")
            
            return success
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"编辑模式发送失败: {e}")
            target.last_error = f"编辑模式失败: {error_msg[:100]}"
            self._log_message(str(task._id), str(account._id), str(target._id), task.message_text, False, error_msg)
            return False
    
    async def _start_reply_monitoring(self, task, target, account):
        """Start reply monitoring for a target after successful send"""
        try:
            logger.info(f"启动回复监听: {target.username or target.user_id}")
            client = await self.account_manager.get_client(str(account._id))
            
            # 确定接收者
            recipient = int(target.user_id) if target.user_id else target.username
            entity = await client.get_entity(recipient)
            
            # Create ReplyMode instance and start monitoring
            # Note: We need a stop event for this task
            task_id = str(task._id)
            if task_id not in self.stop_events:
                self.stop_events[task_id] = asyncio.Event()
            
            reply_mode = ReplyMode(task, self.account_manager)
            reply_mode.start_monitoring(client, entity, str(target._id), self.stop_events[task_id])
            
            logger.info(f"回复监听已启动: {target.username or target.user_id}")
            
        except Exception as e:
            logger.error(f"启动回复监听失败: {e}")
    
    async def _send_message(self, task, target, account):
        """发送消息 - 支持所有发送方式，包含重试机制"""
        retry_count = getattr(task, 'retry_count', 0)
        retry_interval = getattr(task, 'retry_interval', 5)
        
        for attempt in range(retry_count + 1):
            if attempt > 0:
                logger.info(f"重试发送 (第{attempt}/{retry_count}次): {target.username or target.user_id}")
                await asyncio.sleep(retry_interval)
            
            success = await self._do_send_message(task, target, account)
            if success:
                return True
        
        # All retries failed
        logger.warning(f"所有重试均失败: {target.username or target.user_id}")
        return False
    
    async def _do_send_message(self, task, target, account):
        """实际发送消息的内部方法"""
        try:
            # 获取账户的Telegram客户端
            logger.info(f"使用账户 {account.phone} 发送消息")
            client = await self.account_manager.get_client(str(account._id))
            
            # 确定接收者（用户ID或用户名）
            recipient = int(target.user_id) if target.user_id else target.username
            logger.info(f"目标接收者: {recipient}")
            
            # 获取目标用户实体
            try:
                logger.info(f"正在获取用户实体: {recipient}")
                entity = await client.get_entity(recipient)
                logger.info(f"用户实体获取成功")
            except Exception as e:
                error_msg = str(e)
                logger.error(f"获取用户实体失败 {recipient}: {e}")
                
                # Set target.last_error
                if "No user has" in error_msg or "user not found" in error_msg.lower():
                    target.last_error = f"用户不存在: {error_msg[:ERROR_MESSAGE_SHORT_LENGTH]}"
                else:
                    target.last_error = f"无法获取用户信息: {error_msg[:ERROR_MESSAGE_LONG_LENGTH]}"
                
                self.targets_col.update_one(
                    {'_id': target._id},
                    {'$set': {'is_valid': False, 'error_message': str(e)}}
                )
                self._log_message(str(task._id), str(account._id), str(target._id), task.message_text, False, str(e))
                
                # Add to recent logs
                self._add_recent_log(str(task._id), {
                    'time': datetime.utcnow(),
                    'target': target.username or str(target.user_id),
                    'status': 'failed',
                    'message': target.last_error,
                    'account': account.phone
                })
                
                return False
            
            # 提取用户信息用于消息个性化
            user_info = MessageFormatter.extract_user_info(entity)
            logger.info(f"用户信息: {user_info.get('first_name', '')} {user_info.get('last_name', '')}")
            
            self.targets_col.update_one(
                {'_id': target._id},
                {'$set': {
                    'first_name': user_info.get('first_name', ''),
                    'last_name': user_info.get('last_name', '')
                }}
            )
            
            # 个性化消息内容
            personalized = MessageFormatter.personalize(task.message_text, user_info)
            parse_mode = MessageFormatter.get_parse_mode(task.message_format)
            sent_message = None
            
            # 根据不同的发送方式处理
            if task.send_method == SendMethod.POSTBOT.value:
                # Post代码发送 - 通过 @postbot 的内联模式
                logger.info(f"使用Post代码发送，代码: {task.postbot_code}")
                try:
                    # 获取 @postbot 实体
                    logger.info("正在连接 @postbot...")
                    postbot = await client.get_entity('postbot')
                    
                    # 使用内联查询获取 post 内容
                    logger.info(f"查询 @postbot 内联结果: {task.postbot_code}")
                    results = await client.inline_query(postbot, task.postbot_code)
                    
                    if not results:
                        logger.error("@postbot 内联查询无结果")
                        raise ValueError(f"Post代码 {task.postbot_code} 无效或已过期")
                    
                    # 发送第一个内联结果给目标用户
                    logger.info(f"找到 {len(results)} 个内联结果，发送第一个...")
                    sent_message = await results[0].click(entity)
                    logger.info("Post 内容发送成功")
                        
                except Exception as e:
                    logger.error(f"通过 @postbot 发送失败: {e}")
                    raise
            
            elif task.send_method in [SendMethod.CHANNEL_FORWARD.value, SendMethod.CHANNEL_FORWARD_HIDDEN.value]:
                # 频道转发
                logger.info(f"频道转发模式: {task.send_method}")
                logger.info(f"频道链接: {task.channel_link}")
                try:
                    # Parse channel link: https://t.me/channel_name/message_id
                    match = re.match(r'https://t\.me/([^/]+)/(\d+)', task.channel_link)
                    if not match:
                        raise ValueError(f"Invalid channel link format: {task.channel_link}")
                    
                    channel_username = match.group(1)
                    message_id = int(match.group(2))
                    
                    # Get channel entity
                    channel = await client.get_entity(channel_username)
                    # Get specific message
                    message = await client.get_messages(channel, ids=message_id)
                    
                    if not message:
                        raise ValueError(f"Message {message_id} not found in channel {channel_username}")
                    
                    # Forward message
                    if task.send_method == SendMethod.CHANNEL_FORWARD_HIDDEN.value:
                        # Forward without source
                        sent_message = await client.send_message(entity, message.message, file=message.media)
                    else:
                        # Forward with source
                        sent_message = await client.forward_messages(entity, message, channel)
                except Exception as e:
                    logger.error(f"Failed to forward from channel: {e}")
                    raise
            
            else:
                # 直接发送 (DIRECT method)
                if task.media_type == MediaType.TEXT.value:
                    sent_message = await client.send_message(entity, personalized, parse_mode=parse_mode)
                elif task.media_type in [MediaType.IMAGE.value, MediaType.VIDEO.value, MediaType.DOCUMENT.value]:
                    sent_message = await client.send_file(entity, task.media_path, caption=personalized, parse_mode=parse_mode)
                elif task.media_type == MediaType.VOICE.value:
                    sent_message = await client.send_file(entity, task.media_path, voice_note=True, caption=personalized, parse_mode=parse_mode)
            
            # Pin message if configured
            if task.pin_message and sent_message:
                try:
                    await client.pin_message(entity, sent_message)
                    logger.info(f"Message pinned for {recipient}")
                except Exception as e:
                    logger.warning(f"Failed to pin message for {recipient}: {e}")
            
            # Delete dialog if configured
            if task.delete_dialog:
                try:
                    await client.delete_dialog(entity)
                    logger.info(f"Dialog deleted for {recipient}")
                except Exception as e:
                    logger.warning(f"Failed to delete dialog for {recipient}: {e}")
            
            self.targets_col.update_one(
                {'_id': target._id},
                {'$set': {'is_sent': True, 'sent_at': datetime.utcnow()}}
            )
            
            self._log_message(str(task._id), str(account._id), str(target._id), personalized, True, None)
            
            # Add to recent logs
            self._add_recent_log(str(task._id), {
                'time': datetime.utcnow(),
                'target': target.username or str(target.user_id),
                'status': 'success',
                'message': '发送成功',
                'account': account.phone
            })
            
            logger.info(f"Message sent to {recipient}")
            return True
            
        except (UserPrivacyRestrictedError, UserIsBlockedError, ChatWriteForbiddenError, UserNotMutualContactError) as e:
            error_msg = f"Privacy error: {type(e).__name__}"
            if isinstance(e, UserIsBlockedError):
                target.last_error = "账户被封禁"
            elif isinstance(e, ChatWriteForbiddenError):
                target.last_error = "账户隐私限制（对方设置了隐私保护）"
            elif isinstance(e, UserPrivacyRestrictedError):
                target.last_error = "双向限制（需先添加好友）"
            elif isinstance(e, UserNotMutualContactError):
                target.last_error = "双向限制（需先添加好友）"
            else:
                target.last_error = error_msg
            
            self.targets_col.update_one(
                {'_id': target._id},
                {'$set': {'error_message': error_msg}}
            )
            self._log_message(str(task._id), str(account._id), str(target._id), task.message_text, False, error_msg)
            
            # Add to recent logs
            self._add_recent_log(str(task._id), {
                'time': datetime.utcnow(),
                'target': target.username or str(target.user_id),
                'status': 'failed',
                'message': target.last_error,
                'account': account.phone
            })
            
            return False
            
        except FloodWaitError as e:
            error_msg = f"FloodWait: {e.seconds}s"
            target.last_error = f"账户已被限流（需等待{e.seconds}秒）"
            logger.warning(f"Account {account.phone} hit FloodWait, checking real status...")
            
            # 实时检查账户状态
            real_status = await check_account_real_status(self.account_manager, account._id)
            if real_status == 'banned':
                self.db[Account.COLLECTION_NAME].update_one(
                    {'_id': account._id},
                    {'$set': {'status': AccountStatus.BANNED.value, 'updated_at': datetime.utcnow()}}
                )
                logger.error(f"Account {account.phone} is BANNED, marked as unavailable")
            elif real_status == 'limited':
                self.db[Account.COLLECTION_NAME].update_one(
                    {'_id': account._id},
                    {'$set': {'status': AccountStatus.LIMITED.value, 'updated_at': datetime.utcnow()}}
                )
                logger.warning(f"Account {account.phone} is LIMITED")
            else:
                # Even if status is active, still mark as limited temporarily due to FloodWait
                self.db[Account.COLLECTION_NAME].update_one(
                    {'_id': account._id},
                    {'$set': {'status': AccountStatus.LIMITED.value, 'updated_at': datetime.utcnow()}}
                )
            
            self._log_message(str(task._id), str(account._id), str(target._id), task.message_text, False, error_msg)
            
            # Add to recent logs
            self._add_recent_log(str(task._id), {
                'time': datetime.utcnow(),
                'target': target.username or str(target.user_id),
                'status': 'failed',
                'message': target.last_error,
                'account': account.phone
            })
            
            # Handle FloodWait based on strategy
            strategy = getattr(task, 'flood_wait_strategy', 'switch_account')
            
            if strategy == FloodWaitStrategy.STOP_TASK.value:
                logger.warning(f"FloodWait strategy: Stopping task")
                # Mark task as stopped
                self.tasks_col.update_one(
                    {'_id': task._id},
                    {'$set': {'status': TaskStatus.STOPPED.value, 'updated_at': datetime.utcnow()}}
                )
                return False
            elif strategy == FloodWaitStrategy.CONTINUE_WAIT.value:
                logger.info(f"FloodWait strategy: Waiting {e.seconds} seconds")
                await asyncio.sleep(e.seconds)
                return False
            else:  # SWITCH_ACCOUNT (default)
                logger.info(f"FloodWait strategy: Switching account")
                return False
            
        except PeerFloodError:
            error_msg = "PeerFlood"
            target.last_error = "账户已被限流（对方无法接收消息）"
            logger.warning(f"Account {account.phone} hit PeerFlood, checking real status...")
            
            # 实时检查账户状态
            real_status = await check_account_real_status(self.account_manager, account._id)
            if real_status == 'banned':
                self.db[Account.COLLECTION_NAME].update_one(
                    {'_id': account._id},
                    {'$set': {'status': AccountStatus.BANNED.value, 'updated_at': datetime.utcnow()}}
                )
                logger.error(f"Account {account.phone} is BANNED, marked as unavailable")
            elif real_status == 'limited':
                self.db[Account.COLLECTION_NAME].update_one(
                    {'_id': account._id},
                    {'$set': {'status': AccountStatus.LIMITED.value, 'updated_at': datetime.utcnow()}}
                )
                logger.warning(f"Account {account.phone} is LIMITED")
            else:
                # Even if status is active, still mark as limited temporarily due to PeerFlood
                self.db[Account.COLLECTION_NAME].update_one(
                    {'_id': account._id},
                    {'$set': {'status': AccountStatus.LIMITED.value, 'updated_at': datetime.utcnow()}}
                )
            
            self._log_message(str(task._id), str(account._id), str(target._id), task.message_text, False, error_msg)
            
            # Add to recent logs
            self._add_recent_log(str(task._id), {
                'time': datetime.utcnow(),
                'target': target.username or str(target.user_id),
                'status': 'failed',
                'message': target.last_error,
                'account': account.phone
            })
            
            return False
            
        except Exception as e:
            error_msg = str(e)
            error_lower = error_msg.lower()
            
            # Set target.last_error based on error message
            if "No user has" in error_msg or "user not found" in error_lower:
                target.last_error = f"用户不存在: {error_msg[:ERROR_MESSAGE_SHORT_LENGTH]}"
            elif "ALLOW_PAYMENT_REQUIRED" in error_msg:
                target.last_error = "双向限制（需先添加好友）"
            else:
                target.last_error = f"其他错误：{error_msg[:ERROR_MESSAGE_LONG_LENGTH]}"
            
            # Check for dead account indicators
            if task.auto_switch_dead_account:
                dead_keywords = ['banned', 'deleted', 'deactivated', 'terminated']
                if any(keyword in error_lower for keyword in dead_keywords):
                    logger.error(f"Dead account detected for {account.phone}: {error_msg}")
                    self.db[Account.COLLECTION_NAME].update_one(
                        {'_id': account._id},
                        {'$set': {'status': AccountStatus.BANNED.value, 'updated_at': datetime.utcnow()}}
                    )
            
            self.targets_col.update_one(
                {'_id': target._id},
                {'$set': {'error_message': error_msg}}
            )
            self._log_message(str(task._id), str(account._id), str(target._id), task.message_text, False, error_msg)
            
            # Add to recent logs
            self._add_recent_log(str(task._id), {
                'time': datetime.utcnow(),
                'target': target.username or str(target.user_id),
                'status': 'failed',
                'message': target.last_error,
                'account': account.phone
            })
            
            return False
    
    def _log_message(self, task_id, account_id, target_id, message_text, success, error_message):
        """Log message"""
        log = MessageLog(
            task_id=task_id,
            account_id=account_id,
            target_id=target_id,
            message_text=message_text,
            success=success,
            error_message=error_message
        )
        self.logs_col.insert_one(log.to_dict())
    
    def _add_recent_log(self, task_id, log_entry):
        """Add recent log entry for task"""
        if task_id not in self.recent_logs:
            self.recent_logs[task_id] = []
        
        # Add new entry
        self.recent_logs[task_id].append(log_entry)
        
        # Keep only last 20 entries
        if len(self.recent_logs[task_id]) > 20:
            self.recent_logs[task_id] = self.recent_logs[task_id][-20:]
    
    def _get_recent_logs(self, task_id, limit=None):
        """Get recent log entries for task"""
        if task_id not in self.recent_logs:
            return []
        
        # Use default limit if not specified
        if limit is None:
            limit = Config.MAX_DISPLAYED_LOGS
        
        # Return last N entries
        return self.recent_logs[task_id][-limit:] if limit else self.recent_logs[task_id]
    
    def _update_current_account(self, task_id, account):
        """Update current account information for task"""
        task_id_str = str(task_id)
        self.current_account_info[task_id_str] = {
            'phone': account.phone,
            'sent_today': account.messages_sent_today,
            'daily_limit': account.daily_limit
        }
    
    def _get_current_account(self, task_id):
        """Get current account information for task"""
        return self.current_account_info.get(str(task_id))
    
    def _get_account_stats(self, task_id):
        """Get account statistics for task"""
        stats = {}
        
        # Get all logs for this task
        logs = list(self.logs_col.find({'task_id': task_id}))
        
        for log in logs:
            account_id = log.get('account_id')
            if not account_id:
                continue
            
            if account_id not in stats:
                # Get account info
                account_doc = self.db[Account.COLLECTION_NAME].find_one({'_id': ObjectId(account_id)})
                if account_doc:
                    account = Account.from_dict(account_doc)
                    stats[account_id] = {
                        'phone': account.phone,
                        'success': 0,
                        'failed': 0,
                        'total': 0,
                        'messages_sent_today': account.messages_sent_today,
                        'daily_limit': account.daily_limit
                    }
                else:
                    stats[account_id] = {
                        'phone': 'unknown',
                        'success': 0,
                        'failed': 0,
                        'total': 0,
                        'messages_sent_today': 0,
                        'daily_limit': 50
                    }
            
            stats[account_id]['total'] += 1
            if log.get('success'):
                stats[account_id]['success'] += 1
            else:
                stats[account_id]['failed'] += 1
        
        return stats
    
    def _categorize_error(self, error_message):
        """将错误消息分类为友好的中文描述"""
        if not error_message:
            return "未知错误"
        
        error_lower = error_message.lower()
        
        # 隐私和权限相关错误
        if 'privacy' in error_lower or 'userprivacyrestricted' in error_lower:
            return "账户隐私限制（对方设置了隐私保护）"
        if 'blocked' in error_lower or 'userisblocked' in error_lower:
            return "已被对方屏蔽"
        if 'chatwriteforbidden' in error_lower:
            return "无权限发送消息"
        if 'notmutualcontact' in error_lower or 'usernotmutualcontact' in error_lower:
            return "非双向联系人（需要互相添加好友）"
        
        # 限流相关错误
        if 'flood' in error_lower:
            if 'peerflood' in error_lower:
                return "账户已被限流（发送过多消息）"
            return "操作过于频繁，已被限流"
        
        # 账户状态相关
        if 'banned' in error_lower:
            return "账户已封禁"
        if 'restricted' in error_lower:
            return "账户已受限"
        if 'deactivated' in error_lower:
            return "账户已停用"
        
        # 用户不存在或无效
        if 'notfound' in error_lower or 'invalid' in error_lower:
            return "用户不存在或已失效"
        if 'deleted' in error_lower:
            return "用户已删除账号"
        
        # 网络和连接错误
        if 'timeout' in error_lower or 'connection' in error_lower:
            return "网络连接超时"
        if 'network' in error_lower:
            return "网络错误"
        
        # Postbot 相关错误
        if 'postbot' in error_lower:
            return "Post代码无效或已过期"
        
        # 其他 - 安全处理可能的None情况
        if error_message:
            error_preview = error_message[:50] if len(error_message) > 50 else error_message
            return f"其他错误：{error_preview}"
        return "未知错误"
    
    def get_task_progress(self, task_id):
        """Get task progress"""
        task_doc = self.tasks_col.find_one({'_id': ObjectId(task_id)})
        if not task_doc:
            return None
        
        task = Task.from_dict(task_doc)
        return {
            'task_id': str(task._id),
            'name': task.name,
            'status': task.status,
            'total_targets': task.total_targets,
            'sent_count': task.sent_count,
            'failed_count': task.failed_count,
            'pending_count': task.total_targets - task.sent_count - task.failed_count,
            'progress_percent': (task.sent_count / task.total_targets * 100) if task.total_targets > 0 else 0
        }
    
    def export_task_results(self, task_id):
        """Export results including remaining targets"""
        task_doc = self.tasks_col.find_one({'_id': ObjectId(task_id)})
        if not task_doc:
            return None
        
        # Success: is_sent=True
        success_docs = self.targets_col.find({'task_id': task_id, 'is_sent': True})
        success_targets = [Target.from_dict(doc) for doc in success_docs]
        
        # Failed: is_sent=False AND has error_message
        failed_docs = self.targets_col.find({
            'task_id': task_id,
            'is_sent': False,
            'error_message': {'$ne': None, '$exists': True}
        })
        failed_targets = [Target.from_dict(doc) for doc in failed_docs]
        
        # Remaining: is_sent=False AND no error_message (or error_message doesn't exist)
        remaining_docs = self.targets_col.find({
            'task_id': task_id,
            'is_sent': False,
            '$or': [
                {'error_message': None},
                {'error_message': {'$exists': False}}
            ]
        })
        remaining_targets = [Target.from_dict(doc) for doc in remaining_docs]
        
        log_docs = self.logs_col.find({'task_id': task_id})
        logs = [MessageLog.from_dict(doc) for doc in log_docs]
        
        return {
            'success_targets': success_targets,
            'failed_targets': failed_targets,
            'remaining_targets': remaining_targets,
            'logs': logs
        }


# ============================================================================
# BOT 界面
# ============================================================================

# Conversation states
(PHONE_INPUT, CODE_INPUT, PASSWORD_INPUT, 
 MESSAGE_INPUT, FORMAT_SELECT, SEND_METHOD_SELECT, MEDIA_SELECT, MEDIA_UPLOAD,
 TARGET_INPUT, TASK_NAME_INPUT, SESSION_UPLOAD, TDATA_UPLOAD, POSTBOT_CODE_INPUT,
 CHANNEL_LINK_INPUT, PREVIEW_CONFIG,
 CONFIG_THREAD_INPUT, CONFIG_INTERVAL_MIN_INPUT, CONFIG_BIDIRECT_INPUT,
 CONFIG_EDIT_MODE_INPUT, CONFIG_REPLY_MODE_INPUT, CONFIG_BATCH_PAUSE_INPUT,
 CONFIG_VOICE_CALL_INPUT, CONFIG_DAILY_LIMIT_INPUT, CONFIG_RETRY_INPUT,
 CONFIG_THREAD_INTERVAL_INPUT, CONFIG_BATCH_COUNT_INPUT, CONFIG_BATCH_DELAY_INPUT) = range(27)

# Global managers
account_manager = None
task_manager = None
collection_manager = None
db = None


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start command with enhanced dashboard"""
    user_id = update.effective_user.id
    username = update.effective_user.username or "unknown"
    
    logger.info(f"Start command received from user {username} ({user_id})")
    
    if user_id != Config.ADMIN_USER_ID:
        logger.warning(f"Unauthorized access attempt by user {username} ({user_id})")
        await update.message.reply_text("⛔ 未授权访问")
        return
    
    logger.info(f"Authorized user {username} ({user_id}) accessing main menu")
    
    # Get quick stats
    total_accounts = db[Account.COLLECTION_NAME].count_documents({})
    active_accounts = db[Account.COLLECTION_NAME].count_documents({'status': AccountStatus.ACTIVE.value})
    total_tasks = db[Task.COLLECTION_NAME].count_documents({})
    running_tasks = db[Task.COLLECTION_NAME].count_documents({'status': TaskStatus.RUNNING.value})
    
    keyboard = [
        [InlineKeyboardButton("📢 广告私信", callback_data='menu_messaging'), InlineKeyboardButton("👥 采集用户", callback_data='menu_collection')],
        [InlineKeyboardButton("❓ 帮助", callback_data='menu_help')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Enhanced welcome message with stats
    text = (
        "🤖 <b>Telegram 私信机器人</b>\n"
        "━━━━━━━━━━━━━━━━\n\n"
        "📊 <b>系统状态</b>\n"
        f"  • 账户: {active_accounts}/{total_accounts} 可用\n"
        f"  • 任务: {running_tasks}/{total_tasks} 运行中\n\n"
        "✨ <b>核心功能</b>\n"
        "  ✅ 多账户管理\n"
        "  ✅ 富媒体消息\n"
        "  ✅ 消息个性化\n"
        "  ✅ 智能防封策略\n"
        "  ✅ 实时进度监控\n"
        "  ✅ 即时停止响应 (3秒内)\n\n"
        "💡 选择功能开始使用："
    )
    
    await update.message.reply_text(text, reply_markup=reply_markup, parse_mode='HTML')


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle button callbacks"""
    query = update.callback_query
    data = query.data
    user_id = query.from_user.id
    username = query.from_user.username or "unknown"
    
    logger.info(f"Button clicked by user {username} ({user_id}): {data}")
    
    # Immediately answer query to prevent timeout (with error handling)
    # The actual handlers will update the message content
    async def answer_query_with_logging():
        try:
            await safe_answer_query(query)
        except Exception as e:
            logger.error(f"Error answering query in background: {e}")
    
    asyncio.create_task(answer_query_with_logging())
    
    # Main menu
    if data == 'menu_messaging':
        # New messaging menu that consolidates all messaging features
        logger.info(f"User {user_id} accessing messaging menu")
        await show_messaging_menu(query)
    elif data == 'menu_accounts':
        logger.info(f"User {user_id} accessing accounts menu")
        await show_accounts_menu(query)
    elif data == 'menu_tasks':
        logger.info(f"User {user_id} accessing tasks menu")
        await show_tasks_menu(query)
    elif data == 'menu_config':
        logger.info(f"User {user_id} accessing config menu")
        await show_config(query)
    elif data == 'config_proxy':
        logger.info(f"User {user_id} accessing proxy management")
        await show_proxy_menu(query)
    elif data == 'proxy_list':
        logger.info(f"User {user_id} viewing proxy list")
        await list_proxies(query)
    elif data == 'proxy_upload':
        logger.info(f"User {user_id} initiating proxy upload")
        await query.edit_message_text(
            "📤 <b>上传代理文件</b>\n\n"
            "请上传包含代理信息的 .txt 文件\n\n"
            "支持格式:\n"
            "• IP:端口:用户名:密码\n"
            "• socks5://IP:端口:用户名:密码\n"
            "• socks5://user:pass@host:port\n"
            "• IP:端口 (无认证)\n\n"
            "每行一个代理",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 返回", callback_data='config_proxy')]])
        )
        # Set context for file upload handler
        context.user_data['waiting_for'] = 'proxy_file'
    # Removed manual proxy assignment - proxies are now auto-assigned during account operations
    elif data == 'proxy_clear':
        logger.info(f"User {user_id} clearing all proxies")
        # Delete all proxies
        delete_result = db[Proxy.COLLECTION_NAME].delete_many({})
        # Clear proxy_id from all accounts
        db[Account.COLLECTION_NAME].update_many({}, {'$set': {'proxy_id': None}})
        await query.message.reply_text(
            f"✅ 已清空所有代理\n\n删除了 {delete_result.deleted_count} 个代理",
            parse_mode='HTML'
        )
    elif data.startswith('proxy_test_'):
        if data == 'proxy_test_all':
            # Test all proxies concurrently
            logger.info(f"User {user_id} testing all proxies")
            await safe_answer_query(query, "⏳ 开始测试所有代理...", show_alert=False)
            
            # Get all proxies
            all_proxies = list(db[Proxy.COLLECTION_NAME].find())
            total_proxies = len(all_proxies)
            
            if total_proxies == 0:
                await query.message.reply_text("❌ 没有代理可测试")
                return
            
            # Send initial progress message
            progress_msg = await query.message.reply_text(
                f"⏳ <b>正在并发测试代理...</b>\n\n"
                f"进度: 0/{total_proxies} (0%)\n"
                f"✅ 成功: 0\n"
                f"❌ 失败: 0\n"
                f"🗑️ 已删除失败代理: 0",
                parse_mode='HTML'
            )
            
            # Test proxies with concurrency control (10 at a time)
            semaphore = asyncio.Semaphore(10)
            success_count = 0
            failed_count = 0
            deleted_count = 0
            tested_count = 0
            
            async def test_proxy_with_semaphore(proxy_doc):
                """Test single proxy with semaphore"""
                nonlocal success_count, failed_count, deleted_count, tested_count
                async with semaphore:
                    proxy_id = str(proxy_doc['_id'])
                    success, message = await test_proxy(db, proxy_id)
                    
                    tested_count += 1
                    if success:
                        success_count += 1
                    else:
                        failed_count += 1
                        # Check if proxy was deleted (test_proxy auto-deletes failed proxies)
                        if "deleted" in message.lower():
                            deleted_count += 1
                    
                    # Update progress every 5 proxies or on completion
                    if tested_count % 5 == 0 or tested_count == total_proxies:
                        percentage = (tested_count / total_proxies * 100) if total_proxies > 0 else 0
                        try:
                            await progress_msg.edit_text(
                                f"⏳ <b>正在并发测试代理...</b>\n\n"
                                f"进度: {tested_count}/{total_proxies} ({percentage:.1f}%)\n"
                                f"✅ 成功: {success_count}\n"
                                f"❌ 失败: {failed_count}\n"
                                f"🗑️ 已删除失败代理: {deleted_count}",
                                parse_mode='HTML'
                            )
                        except Exception as e:
                            logger.warning(f"Failed to update progress: {e}")
            
            # Test all proxies concurrently
            await asyncio.gather(*[test_proxy_with_semaphore(proxy) for proxy in all_proxies])
            
            # Get remaining proxies after auto-deletion
            remaining_proxies = db[Proxy.COLLECTION_NAME].count_documents({})
            
            # Reassign proxies to accounts that lost their proxies
            accounts_without_proxy = list(db[Account.COLLECTION_NAME].find({'proxy_id': None}))
            reassigned_count = 0
            for account_doc in accounts_without_proxy:
                proxy = get_next_available_proxy(db)
                if proxy:
                    db[Account.COLLECTION_NAME].update_one(
                        {'_id': account_doc['_id']},
                        {'$set': {'proxy_id': proxy._id}}
                    )
                    reassigned_count += 1
            
            # Delete progress message and show final result
            try:
                await progress_msg.delete()
            except:
                pass
            
            await query.message.reply_text(
                f"✅ <b>代理测试完成！</b>\n\n"
                f"📊 <b>测试结果：</b>\n"
                f"✅ 测试成功: {success_count} 个\n"
                f"❌ 测试失败: {failed_count} 个\n"
                f"🗑️ 已自动删除失败代理: {deleted_count} 个\n"
                f"📦 剩余可用代理: {remaining_proxies} 个\n"
                f"🔄 已重新分配代理: {reassigned_count} 个账户",
                parse_mode='HTML'
            )
        else:
            # Test single proxy
            proxy_id = data.split('_')[2]
            logger.info(f"User {user_id} testing proxy {proxy_id}")
            await safe_answer_query(query, "⏳ 正在测试代理...", show_alert=False)
            success, message = await test_proxy(db, proxy_id)
            emoji = "✅" if success else "❌"
            await query.message.reply_text(f"{emoji} {message}")
    elif data.startswith('proxy_delete_'):
        proxy_id = data.split('_')[2]
        logger.info(f"User {user_id} deleting proxy {proxy_id}")
        proxy_oid = ObjectId(proxy_id)
        db[Proxy.COLLECTION_NAME].delete_one({'_id': proxy_oid})
        # Remove proxy_id from accounts using this proxy (handle both ObjectId and string)
        db[Account.COLLECTION_NAME].update_many(
            {'$or': [{'proxy_id': proxy_oid}, {'proxy_id': proxy_id}]},
            {'$set': {'proxy_id': None}}
        )
        await safe_answer_query(query, "✅ 代理已删除", show_alert=True)
        await list_proxies(query)
    elif data.startswith('proxy_toggle_'):
        proxy_id = data.split('_')[2]
        logger.info(f"User {user_id} toggling proxy {proxy_id}")
        proxy_doc = db[Proxy.COLLECTION_NAME].find_one({'_id': ObjectId(proxy_id)})
        if proxy_doc:
            new_status = not proxy_doc.get('is_active', True)
            db[Proxy.COLLECTION_NAME].update_one(
                {'_id': ObjectId(proxy_id)},
                {'$set': {'is_active': new_status, 'updated_at': datetime.utcnow()}}
            )
            status_text = "启用" if new_status else "禁用"
            await safe_answer_query(query, f"✅ 代理已{status_text}", show_alert=True)
            await list_proxies(query)
    elif data == 'menu_stats':
        logger.info(f"User {user_id} accessing stats menu")
        await show_stats(query)
    elif data == 'menu_help':
        logger.info(f"User {user_id} accessing help menu")
        await show_help(query)
    elif data == 'menu_collection':
        logger.info(f"User {user_id} accessing collection menu")
        await caiji.show_collection_menu(query)
    elif data == 'collection_accounts_menu':
        logger.info(f"User {user_id} accessing collection accounts menu")
        await caiji.show_collection_accounts_menu(query)
    elif data == 'collection_accounts_list':
        logger.info(f"User {user_id} viewing collection accounts list")
        await caiji.list_collection_accounts(query)
    elif data == 'collection_accounts_add':
        logger.info(f"User {user_id} adding collection account")
        # 显示上传界面，但标记为采集账户类型
        context.user_data['account_type'] = 'collection'
        await show_add_account_menu(query)
    elif data == 'collection_upload_account':
        logger.info(f"User {user_id} uploading account from collection menu")
        await show_add_account_menu(query)
    elif data == 'collection_list':
        logger.info(f"User {user_id} viewing collection list")
        await caiji.show_collection_list(query)
    elif data.startswith('collection_list_'):
        page = int(data.split('_')[2])
        await caiji.show_collection_list(query, page)
    elif data.startswith('collection_detail_'):
        collection_id = data.split('_')[2]
        await caiji.show_collection_detail(query, collection_id)
    elif data.startswith('collection_start_'):
        collection_id = data.split('_')[2]
        await safe_answer_query(query, "▶️ 正在启动采集任务...", show_alert=False)
        try:
            await collection_manager.start_collection(collection_id)
            await query.message.reply_text("✅ 采集任务已启动")
            await caiji.show_collection_detail(query, collection_id)
        except Exception as e:
            await query.message.reply_text(f"❌ 启动失败: {str(e)}")
    elif data.startswith('collection_stop_'):
        collection_id = data.split('_')[2]
        await safe_answer_query(query, "⏸️ 正在停止采集任务...", show_alert=False)
        try:
            await collection_manager.stop_collection(collection_id)
            await query.message.reply_text("⏸️ 采集任务已停止")
            await caiji.show_collection_detail(query, collection_id)
        except Exception as e:
            await query.message.reply_text(f"❌ 停止失败: {str(e)}")
    elif data.startswith('collection_delete_'):
        collection_id = data.split('_')[2]
        await safe_answer_query(query, "🗑️ 正在删除采集任务...", show_alert=False)
        try:
            collection_manager.delete_collection(collection_id)
            await query.message.reply_text("✅ 采集任务已删除")
            await caiji.show_collection_list(query)
        except Exception as e:
            await query.message.reply_text(f"❌ 删除失败: {str(e)}")
    elif data.startswith('collection_export_users_'):
        collection_id = data.split('_')[3]
        await safe_answer_query(query, "📥 正在导出用户列表...", show_alert=False)
        try:
            users = await collection_manager.export_collected_users(collection_id)
            if users:
                # Create CSV content
                output = io.StringIO()
                writer = csv.DictWriter(output, fieldnames=['user_id', 'username', 'first_name', 'last_name', 'tags'])
                writer.writeheader()
                writer.writerows(users)
                
                # Send as file
                file_content = output.getvalue().encode('utf-8')
                file_bytes = io.BytesIO(file_content)
                file_bytes.name = f'collected_users_{collection_id}.csv'
                await query.message.reply_document(
                    document=file_bytes,
                    filename=f'collected_users_{collection_id}.csv',
                    caption=f"✅ 已导出 {len(users)} 个用户"
                )
            else:
                await query.message.reply_text("❌ 没有用户数据")
        except Exception as e:
            await query.message.reply_text(f"❌ 导出失败: {str(e)}")
    elif data.startswith('collection_export_groups_'):
        collection_id = data.split('_')[3]
        await safe_answer_query(query, "📥 正在导出群组列表...", show_alert=False)
        try:
            groups = await collection_manager.export_collected_groups(collection_id)
            if groups:
                # Create CSV content
                output = io.StringIO()
                writer = csv.DictWriter(output, fieldnames=['group_id', 'title', 'username', 'link', 'member_count', 'is_public'])
                writer.writeheader()
                writer.writerows(groups)
                
                # Send as file
                file_content = output.getvalue().encode('utf-8')
                file_bytes = io.BytesIO(file_content)
                file_bytes.name = f'collected_groups_{collection_id}.csv'
                await query.message.reply_document(
                    document=file_bytes,
                    filename=f'collected_groups_{collection_id}.csv',
                    caption=f"✅ 已导出 {len(groups)} 个群组/频道"
                )
            else:
                await query.message.reply_text("❌ 没有群组数据")
        except Exception as e:
            await query.message.reply_text(f"❌ 导出失败: {str(e)}")
    
    # Accounts
    elif data == 'accounts_list':
        logger.info(f"User {user_id} viewing accounts list")
        await list_accounts(query)
    elif data == 'accounts_add':
        logger.info(f"User {user_id} initiating account add")
        await show_add_account_menu(query)
    elif data == 'accounts_add_session':
        logger.info(f"User {user_id} selecting session upload option")
        await show_upload_type_menu(query)
    elif data == 'accounts_check_status':
        logger.info(f"User {user_id} checking all accounts status")
        await safe_answer_query(query, "🔍 正在检查账户状态，请稍候...", show_alert=False)
        
        # Send initial progress message
        progress_msg = await query.message.reply_text(
            "⏳ <b>正在调用 @spambot 检查所有账户...</b>\n\n"
            "进度: 0/? (0%)\n"
            "✅ 无限制: 0\n"
            "⚠️ 双向限制: 0\n"
            "❄️ 冻结: 0\n"
            "🚫 封禁: 0\n\n"
            "⏱️ 预计时间: 计算中...",
            parse_mode='HTML'
        )
        
        # Track start time for ETA calculation
        start_time = datetime.utcnow()
        
        async def update_progress(current, total, stats):
            """Update progress message"""
            try:
                # Calculate progress percentage
                percentage = (current / total * 100) if total > 0 else 0
                
                # Calculate ETA
                elapsed = (datetime.utcnow() - start_time).total_seconds()
                if current > 0:
                    avg_time_per_account = elapsed / current
                    remaining_accounts = total - current
                    eta_seconds = avg_time_per_account * remaining_accounts
                    eta_minutes = int(eta_seconds / 60)
                    eta_text = f"{eta_minutes}分{int(eta_seconds % 60)}秒" if eta_minutes > 0 else f"{int(eta_seconds)}秒"
                else:
                    eta_text = "计算中..."
                
                # Update progress message
                await progress_msg.edit_text(
                    f"⏳ <b>正在调用 @spambot 检查所有账户...</b>\n\n"
                    f"进度: {current}/{total} ({percentage:.1f}%)\n"
                    f"✅ 无限制: {len(stats['unlimited'])}\n"
                    f"⚠️ 双向限制: {len(stats['limited'])}\n"
                    f"❄️ 冻结: {len(stats['restricted'])}\n"
                    f"🚫 封禁: {len(stats['banned'])}\n\n"
                    f"⏱️ 预计剩余时间: {eta_text}",
                    parse_mode='HTML'
                )
            except Exception as e:
                logger.warning(f"Failed to update progress message: {e}")
        
        try:
            status_results = await check_all_accounts_status(progress_callback=update_progress)
            
            # Delete progress message
            try:
                await progress_msg.delete()
            except:
                pass
            
            text = (
                f"✅ <b>账户状态检查完成！</b>\n\n"
                f"📊 <b>统计结果：</b>\n"
                f"✅ 无限制账号：{len(status_results['unlimited'])} 个\n"
                f"⚠️ 双向限制账号：{len(status_results['limited'])} 个\n"
                f"❄️ 冻结账号：{len(status_results['restricted'])} 个\n"
                f"🚫 封禁账号：{len(status_results['banned'])} 个\n\n"
                f"使用下方按钮导出账户文件："
            )
            
            keyboard = [
                [InlineKeyboardButton("📥 全部账户提取", callback_data='accounts_export_all')],
                [InlineKeyboardButton("⚠️ 受限账户提取", callback_data='accounts_export_limited')],
                [InlineKeyboardButton("🔙 返回", callback_data='menu_accounts')]
            ]
            
            await query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
        except Exception as e:
            logger.error(f"Error checking accounts status: {e}", exc_info=True)
            # Delete progress message on error
            try:
                await progress_msg.delete()
            except:
                pass
            await query.message.reply_text(f"❌ 检查失败：{str(e)}")
    
    elif data == 'accounts_export_all':
        logger.info(f"User {user_id} exporting all accounts")
        await safe_answer_query(query, "📥 正在导出所有账户...", show_alert=False)
        
        try:
            # Only export messaging accounts
            all_accounts = list(db[Account.COLLECTION_NAME].find({'account_type': 'messaging'}))
            account_ids = [str(acc['_id']) for acc in all_accounts]
            
            if not account_ids:
                await safe_answer_query(query, "❌ 没有账户可导出", show_alert=True)
                return
            
            # 显示准备进度
            progress_msg = await query.message.reply_text(
                "⏳ 正在准备导出...\n\n"
                f"📊 账户总数: {len(account_ids)}\n"
                f"🔌 正在断开所有活跃连接...",
                parse_mode='HTML'
            )
            
            # 断开所有活跃的 Telethon 客户端
            disconnected_count = 0
            for account_id in account_ids:
                try:
                    # 检查 account_manager 中是否有活跃客户端
                    client = account_manager.clients.get(account_id)
                    if client and client.is_connected():
                        await client.disconnect()
                        disconnected_count += 1
                        logger.info(f"Disconnected client for account {account_id}")
                except Exception as e:
                    logger.warning(f"Failed to disconnect account {account_id}: {e}")
            
            logger.info(f"Disconnected {disconnected_count} active clients before export")
            
            # 等待1秒确保所有连接完全关闭
            await asyncio.sleep(1)
            
            # 更新进度
            await progress_msg.edit_text(
                f"✅ 已断开 {disconnected_count} 个活跃连接\n\n"
                f"📦 正在生成 ZIP 文件...",
                parse_mode='HTML'
            )
            
            zip_path = await export_accounts(account_ids, 'all')
            
            # 更新进度
            await progress_msg.edit_text("📤 正在上传文件...", parse_mode='HTML')
            
            # 发送文件，添加超时处理
            try:
                with open(zip_path, 'rb') as f:
                    await asyncio.wait_for(
                        query.message.reply_document(
                            document=f,
                            filename=os.path.basename(zip_path),
                            caption=f"📥 <b>所有账户导出</b>\n\n共 {len(account_ids)} 个账户\n\n⚠️ 导出后将自动清空本地数据",
                            parse_mode='HTML'
                        ),
                        timeout=60.0  # 60秒超时
                    )
            except asyncio.TimeoutError:
                logger.error("Document upload timeout")
                await progress_msg.delete()
                await query.message.reply_text(
                    "❌ 上传超时\n\n"
                    f"文件已保存至服务器：{os.path.basename(zip_path)}\n"
                    "请联系管理员手动获取",
                    parse_mode='HTML'
                )
                return  # 不删除账户数据
            except Exception as upload_error:
                logger.error(f"Document upload failed: {upload_error}", exc_info=True)
                await progress_msg.delete()
                await query.message.reply_text(
                    f"❌ 上传失败：{str(upload_error)}\n\n"
                    f"文件已保存至服务器\n"
                    "请联系管理员",
                    parse_mode='HTML'
                )
                return
            
            # 删除进度消息
            try:
                await progress_msg.delete()
            except Exception:
                pass
            
            # Delete all accounts from database
            delete_result = db[Account.COLLECTION_NAME].delete_many({})
            logger.info(f"Deleted {delete_result.deleted_count} accounts from database")
            
            # Delete all session files
            deleted_files = 0
            for account in all_accounts:
                session_name = account.get('session_name')
                if session_name:
                    session_path = os.path.join(Config.SESSIONS_DIR, f"{session_name}.session")
                    json_path = f"{session_path}.json"
                    
                    if os.path.exists(session_path):
                        os.remove(session_path)
                        deleted_files += 1
                        logger.info(f"Deleted session file: {session_path}")
                    
                    if os.path.exists(json_path):
                        os.remove(json_path)
                        logger.info(f"Deleted json file: {json_path}")
            
            os.remove(zip_path)
            
            # Notify user
            await query.message.reply_text(
                f"✅ <b>导出完成并已清空</b>\n\n"
                f"已导出 {len(account_ids)} 个账户\n"
                f"数据库已删除 {delete_result.deleted_count} 条记录\n"
                f"本地已删除 {deleted_files} 个会话文件",
                parse_mode='HTML'
            )
        except Exception as e:
            logger.error(f"Error exporting all accounts: {e}", exc_info=True)
            await safe_answer_query(query, f"❌ 导出失败：{str(e)}", show_alert=True)
    
    elif data == 'accounts_export_limited':
        logger.info(f"User {user_id} exporting limited accounts")
        await safe_answer_query(query, "⚠️ 正在导出受限账户...", show_alert=False)
        
        try:
            # Only export limited messaging accounts
            limited_accounts = list(db[Account.COLLECTION_NAME].find({
                'status': {'$in': [AccountStatus.LIMITED.value, AccountStatus.BANNED.value, AccountStatus.INACTIVE.value]},
                'account_type': 'messaging'
            }))
            account_ids = [str(acc['_id']) for acc in limited_accounts]
            
            if not account_ids:
                await safe_answer_query(query, "✅ 没有受限账户", show_alert=True)
                return
            
            # 显示准备进度
            progress_msg = await query.message.reply_text(
                f"⏳ 正在准备导出 {len(account_ids)} 个受限账户...\n\n"
                f"🔌 正在断开连接...",
                parse_mode='HTML'
            )
            
            # 断开受限账户的客户端
            disconnected_count = 0
            for account_id in account_ids:
                try:
                    client = account_manager.clients.get(account_id)
                    if client and client.is_connected():
                        await client.disconnect()
                        disconnected_count += 1
                        logger.info(f"Disconnected limited account client {account_id}")
                except Exception as e:
                    logger.warning(f"Failed to disconnect limited account {account_id}: {e}")
            
            logger.info(f"Disconnected {disconnected_count} limited account clients")
            await asyncio.sleep(1)
            
            await progress_msg.edit_text("📦 正在生成 ZIP 文件...", parse_mode='HTML')
            
            zip_path = await export_accounts(account_ids, 'limited')
            
            await progress_msg.edit_text("📤 正在上传文件...", parse_mode='HTML')
            
            # 发送文件，添加超时处理
            try:
                with open(zip_path, 'rb') as f:
                    await asyncio.wait_for(
                        query.message.reply_document(
                            document=f,
                            filename=os.path.basename(zip_path),
                            caption=f"⚠️ <b>受限账户导出</b>\n\n共 {len(account_ids)} 个账户\n\n⚠️ 导出后将自动删除这些受限账户",
                            parse_mode='HTML'
                        ),
                        timeout=60.0
                    )
            except asyncio.TimeoutError:
                logger.error("Limited accounts document upload timeout")
                await progress_msg.delete()
                await query.message.reply_text(
                    "❌ 上传超时\n\n"
                    f"文件已保存至服务器：{os.path.basename(zip_path)}\n"
                    "请联系管理员手动获取",
                    parse_mode='HTML'
                )
                return
            except Exception as upload_error:
                logger.error(f"Limited accounts document upload failed: {upload_error}", exc_info=True)
                await progress_msg.delete()
                await query.message.reply_text(
                    f"❌ 上传失败：{str(upload_error)}\n\n"
                    f"文件已保存至服务器\n"
                    "请联系管理员",
                    parse_mode='HTML'
                )
                return
            
            # 删除进度消息
            try:
                await progress_msg.delete()
            except Exception:
                pass
            
            # Delete limited accounts from database
            limited_ids = [acc['_id'] for acc in limited_accounts]
            delete_result = db[Account.COLLECTION_NAME].delete_many({
                '_id': {'$in': limited_ids}
            })
            logger.info(f"Deleted {delete_result.deleted_count} limited accounts from database")
            
            # Delete session files for limited accounts
            deleted_files = 0
            for account in limited_accounts:
                session_name = account.get('session_name')
                if session_name:
                    session_path = os.path.join(Config.SESSIONS_DIR, f"{session_name}.session")
                    json_path = f"{session_path}.json"
                    
                    if os.path.exists(session_path):
                        os.remove(session_path)
                        deleted_files += 1
                        logger.info(f"Deleted session file: {session_path}")
                    
                    if os.path.exists(json_path):
                        os.remove(json_path)
                        logger.info(f"Deleted json file: {json_path}")
            
            os.remove(zip_path)
            
            # Get remaining accounts count
            remaining_accounts = db[Account.COLLECTION_NAME].count_documents({})
            
            # Notify user
            await query.message.reply_text(
                f"✅ <b>受限账户导出完成并已删除</b>\n\n"
                f"已导出并删除 {len(account_ids)} 个受限账户\n"
                f"数据库已删除 {delete_result.deleted_count} 条记录\n"
                f"本地已删除 {deleted_files} 个会话文件\n"
                f"剩余账户数量: {remaining_accounts} 个",
                parse_mode='HTML'
            )
        except Exception as e:
            logger.error(f"Error exporting limited accounts: {e}", exc_info=True)
            await safe_answer_query(query, f"❌ 导出失败：{str(e)}", show_alert=True)
    # Note: upload_session_file and upload_tdata_file are handled by ConversationHandler
    elif data.startswith('account_check_'):
        account_id = data.split('_')[2]
        logger.info(f"User {user_id} checking account {account_id}")
        await check_account(query, account_id)
    
    # Tasks
    elif data == 'tasks_list':
        logger.info(f"User {user_id} viewing tasks list")
        await list_tasks(query)
    # Note: tasks_create is handled by ConversationHandler
    elif data.startswith('task_detail_'):
        task_id = data.split('_')[2]
        logger.info(f"User {user_id} viewing task {task_id} detail")
        await show_task_detail(query, task_id)
    elif data.startswith('task_config_'):
        task_id = data.split('_')[2]
        logger.info(f"User {user_id} configuring task {task_id}")
        await show_task_config(query, task_id)
    elif data.startswith('cfg_toggle_'):
        # Handle toggle buttons: pin, delete, repeat (generic), dead, force (special handlers)
        parts = data.split('_')
        toggle_type = parts[2]  # pin, delete, repeat, dead, force
        task_id = parts[3] if len(parts) > 3 else parts[-1]
        
        if toggle_type == 'dead':
            # Special handling: dead account toggle has 'account' in callback data (cfg_toggle_dead_account_)
            await toggle_dead_account_switch(update, context)
        elif toggle_type == 'force':
            # Special handling: force private mode toggle has 'private' in callback data (cfg_toggle_force_private_)
            await toggle_force_private_mode(update, context)
        else:
            # Generic handling for pin, delete, repeat
            await toggle_task_config(query, task_id, toggle_type)
    
    # New configuration handlers
    elif data.startswith('cfg_thread_') and not data.startswith('cfg_thread_interval_'):
        await request_thread_config(update, context)
    elif data.startswith('cfg_interval_'):
        await request_interval_config(update, context)
    elif data.startswith('cfg_bidirect_'):
        await request_bidirect_config(update, context)
    elif data.startswith('cfg_daily_limit_'):
        await request_daily_limit_config(update, context)
    elif data.startswith('cfg_retry_'):
        await request_retry_config(update, context)
    elif data.startswith('cfg_edit_mode_'):
        await request_edit_mode_config(update, context)
    elif data.startswith('set_mode_'):
        await set_message_mode(update, context)
    elif data.startswith('cfg_reply_mode_'):
        await request_reply_mode_config(update, context)
    elif data.startswith('cfg_batch_pause_'):
        await request_batch_pause_config(update, context)
    elif data.startswith('set_batch_count_'):
        await request_batch_count_config(update, context)
    elif data.startswith('set_batch_delay_'):
        await request_batch_delay_config(update, context)
    elif data.startswith('disable_batch_pause_'):
        await disable_batch_pause(update, context)
    elif data.startswith('cfg_flood_strategy_'):
        await request_flood_strategy_config(update, context)
    elif data.startswith('set_flood_'):
        await set_flood_strategy(update, context)
    elif data.startswith('cfg_voice_call_'):
        await request_voice_call_config(update, context)
    elif data.startswith('set_voice_'):
        await set_voice_call_mode(update, context)
    elif data.startswith('toggle_voice_'):
        await toggle_voice_call(update, context)
    elif data.startswith('cfg_thread_interval_'):
        await request_thread_interval_config(update, context)
    elif data.startswith('show_config_'):
        task_id = data.split('_')[2]
        await show_config_menu_handler(update, context, task_id)
    
    elif data.startswith('cfg_cancel_'):
        return await handle_config_cancel(update, context)
    elif data.startswith('cfg_example_'):
        await show_config_example(update, context)
    elif data == 'close_example':
        # Close example message
        await query.message.delete()
    
    elif data == 'noop':
        # No operation for info-only buttons
        await safe_answer_query(query)
    elif data.startswith('task_start_'):
        task_id = data.split('_')[2]
        logger.info(f"User {user_id} starting task {task_id}")
        await start_task_handler(query, task_id, context)
    elif data.startswith('task_stop_'):
        if data.startswith('task_stop_confirm_'):
            # Confirmed stop action
            task_id = data.split('_')[3]
            logger.info(f"User {user_id} confirmed stopping task {task_id}")
            await stop_task_confirmed(query, task_id, context)
        else:
            # Show confirmation dialog
            task_id = data.split('_')[2]
            logger.info(f"User {user_id} stopping task {task_id}")
            await stop_task_handler(query, task_id, context)
    elif data.startswith('task_progress_'):
        # Handle both task_progress_refresh_ and task_progress_
        if 'refresh' in data:
            task_id = data.split('_')[3]
            logger.info(f"User {user_id} refreshing task {task_id} progress")
            await refresh_task_progress(query, task_id)
        else:
            task_id = data.split('_')[2]
            logger.info(f"User {user_id} viewing task {task_id} progress")
            await show_task_progress(query, task_id)
    elif data.startswith('task_export_'):
        task_id = data.split('_')[2]
        logger.info(f"User {user_id} exporting task {task_id} results")
        await export_results(query, task_id)
    elif data.startswith('task_delete_'):
        task_id = data.split('_')[2]
        logger.info(f"User {user_id} deleting task {task_id}")
        await delete_task_handler(query, task_id)
    
    # Format selection
    elif data.startswith('format_'):
        format_name = data.split('_')[1]
        context.user_data['message_format'] = MessageFormat[format_name.upper()]
        logger.info(f"User {user_id} selected format: {format_name}")
        # After format selection, go to media type selection
        return await select_media_type(query)
    
    # Send method selection
    elif data.startswith('sendmethod_'):
        if data == 'sendmethod_preview':
            return await show_preview(query, context)
        elif data == 'sendmethod_direct':
            context.user_data['send_method'] = SendMethod.DIRECT
            logger.info(f"User {user_id} selected send method: direct")
            # For direct send, request message input
            await query.message.reply_text(
                "📤 <b>直接发送</b>\n\n"
                "请输入消息内容：\n\n"
                "💡 可使用变量：{name}, {first_name}, {last_name}, {full_name}, {username}",
                parse_mode='HTML'
            )
            return MESSAGE_INPUT
        elif data == 'sendmethod_postbot':
            context.user_data['send_method'] = SendMethod.POSTBOT
            logger.info(f"User {user_id} selected send method: postbot")
            return await request_postbot_code(query)
        elif data == 'sendmethod_channel_forward':
            context.user_data['send_method'] = SendMethod.CHANNEL_FORWARD
            logger.info(f"User {user_id} selected send method: channel_forward")
            return await request_channel_link(query)
        elif data == 'sendmethod_channel_forward_hidden':
            context.user_data['send_method'] = SendMethod.CHANNEL_FORWARD_HIDDEN
            logger.info(f"User {user_id} selected send method: channel_forward_hidden")
            return await request_channel_link(query)
    
    # Preview continue
    elif data == 'preview_continue':
        # After preview, always go to target list
        return await request_target_list(query)
    
    # Preview back - allow user to modify configuration
    elif data == 'preview_back':
        send_method = context.user_data.get('send_method', SendMethod.DIRECT)
        logger.info(f"User {user_id} going back from preview, send_method: {send_method.value}")
        
        if send_method == SendMethod.DIRECT:
            # For direct send, go back to message input
            await query.message.reply_text(
                "📤 <b>直接发送</b>\n\n"
                "请重新输入消息内容：\n\n"
                "💡 可使用变量：{name}, {first_name}, {last_name}, {full_name}, {username}",
                parse_mode='HTML'
            )
            return MESSAGE_INPUT
        elif send_method == SendMethod.POSTBOT:
            # For postbot, go back to code input
            return await request_postbot_code(query)
        elif send_method in [SendMethod.CHANNEL_FORWARD, SendMethod.CHANNEL_FORWARD_HIDDEN]:
            # For channel forward, go back to link input
            return await request_channel_link(query)
    
    # Media selection
    elif data.startswith('media_'):
        media_name = data.split('_')[1]
        context.user_data['media_type'] = MediaType[media_name.upper()]
        logger.info(f"User {user_id} selected media type: {media_name}")
        if context.user_data['media_type'] == MediaType.TEXT:
            # Show preview before going to target list
            return await show_preview(query, context)
        else:
            return await request_media_upload(query)
    
    # Back
    elif data == 'back_main':
        logger.info(f"User {user_id} returning to main menu")
        await back_to_main(query)


async def show_messaging_menu(query):
    """Show messaging menu with all features consolidated"""
    # Get statistics
    total_accounts = db[Account.COLLECTION_NAME].count_documents({})
    active_accounts = db[Account.COLLECTION_NAME].count_documents({'status': AccountStatus.ACTIVE.value})
    total_tasks = db[Task.COLLECTION_NAME].count_documents({})
    running_tasks = db[Task.COLLECTION_NAME].count_documents({'status': TaskStatus.RUNNING.value})
    
    keyboard = [
        [InlineKeyboardButton("📱 账户管理", callback_data='menu_accounts')],
        [InlineKeyboardButton("📝 任务管理", callback_data='menu_tasks')],
        [InlineKeyboardButton("⚙️ 全局配置", callback_data='menu_config')],
        [InlineKeyboardButton("📊 统计信息", callback_data='menu_stats')],
        [InlineKeyboardButton("🔙 返回主菜单", callback_data='back_main')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    text = (
        f"📢 <b>广告私信</b>\n\n"
        f"📊 <b>快速概览：</b>\n"
        f"👥 账户：{active_accounts}/{total_accounts} 个可用\n"
        f"📋 任务：{running_tasks}/{total_tasks} 个运行中\n\n"
        f"请选择功能："
    )
    
    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='HTML')


async def show_accounts_menu(query):
    """Show enhanced accounts menu with statistics"""
    # 统计账户数量（只统计 messaging 类型）
    total_accounts = db[Account.COLLECTION_NAME].count_documents({'account_type': 'messaging'})
    active_accounts = db[Account.COLLECTION_NAME].count_documents({
        'status': AccountStatus.ACTIVE.value,
        'account_type': 'messaging'
    })
    
    keyboard = [
        [InlineKeyboardButton("📋 账号列表", callback_data='accounts_list')],
        [InlineKeyboardButton("➕ 添加账号", callback_data='accounts_add')],
        [InlineKeyboardButton("🔍 检查账户状态", callback_data='accounts_check_status')],
        [InlineKeyboardButton("🔙 返回", callback_data='menu_messaging')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    text = (
        f"📱 <b>账户管理</b>\n\n"
        f"当前状态：可用 {active_accounts}/{total_accounts} 个账号\n\n"
        f"请选择操作："
    )
    
    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='HTML')


async def show_add_account_menu(query):
    """Show add account menu"""
    keyboard = [
        [InlineKeyboardButton("📁 上传 Session 文件", callback_data='accounts_add_session')],
        [InlineKeyboardButton("🔙 返回", callback_data='menu_accounts')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    text = (
        "➕ <b>添加账户</b>\n\n"
        "上传 Session 文件：\n"
        "支持 .session、session+json、tdata 格式\n"
        "请打包为 .zip 文件上传"
    )
    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='HTML')


async def show_upload_type_menu(query):
    """Show upload type menu"""
    logger.info(f"User {query.from_user.id} requested upload type menu")
    keyboard = [
        [InlineKeyboardButton("📁 上传 Session 文件", callback_data='upload_session_file')],
        [InlineKeyboardButton("📂 上传 TData 文件", callback_data='upload_tdata_file')],
        [InlineKeyboardButton("🔙 返回", callback_data='accounts_add')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    text = (
        "📁 <b>上传文件</b>\n\n"
        "请选择上传类型：\n\n"
        "📁 <b>Session 文件</b>\n"
        "支持 .session、session+json 格式\n"
        "请打包为 .zip 文件上传\n\n"
        "📂 <b>TData 文件</b>\n"
        "Telegram Desktop 的 tdata 文件夹\n"
        "请打包为 .zip 文件上传"
    )
    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='HTML')


async def request_session_upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Request session file upload - Conversation entry point.
    
    Handles the upload_session_file callback, prompts the user to upload a .zip file
    containing session files, and transitions to SESSION_UPLOAD state.
    
    Returns:
        int: SESSION_UPLOAD state constant
    """
    query = update.callback_query
    await safe_answer_query(query)
    logger.info(f"User {query.from_user.id} requested session file upload")
    context.user_data['upload_type'] = 'session'
    await query.message.reply_text(
        "📁 <b>上传 Session 文件</b>\n\n"
        "请上传包含 Session 文件的 .zip 压缩包\n"
        "支持格式：\n"
        "- .session 文件\n"
        "- .session + .json 文件\n\n"
        "⚠️ 文件大小限制：50MB",
        parse_mode='HTML'
    )
    return SESSION_UPLOAD


async def request_tdata_upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Request TData file upload - Conversation entry point.
    
    Handles the upload_tdata_file callback, prompts the user to upload a .zip file
    containing Telegram Desktop tdata folder, and transitions to TDATA_UPLOAD state.
    
    Returns:
        int: TDATA_UPLOAD state constant
    """
    query = update.callback_query
    await safe_answer_query(query)
    logger.info(f"User {query.from_user.id} requested tdata file upload")
    context.user_data['upload_type'] = 'tdata'
    await query.message.reply_text(
        "📂 <b>上传 TData 文件</b>\n\n"
        "请上传 Telegram Desktop 的 tdata 文件夹压缩包\n"
        "格式：tdata 文件夹打包为 .zip\n\n"
        "⚠️ 文件大小限制：50MB",
        parse_mode='HTML'
    )
    return TDATA_UPLOAD


async def handle_file_upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle file upload for session or tdata"""
    upload_type = context.user_data.get('upload_type', 'session')
    account_type = context.user_data.get('account_type', 'messaging')  # 获取账户类型，默认 messaging
    # Determine which state to return based on upload type
    current_state = SESSION_UPLOAD if upload_type == 'session' else TDATA_UPLOAD
    
    logger.info(f"User {update.effective_user.id} is uploading {upload_type} file with account_type: {account_type}")
    
    if not update.message.document:
        logger.warning(f"User {update.effective_user.id} sent non-document message")
        await update.message.reply_text("❌ 请上传 .zip 文件")
        return current_state
    
    document = update.message.document
    if not document.file_name.endswith('.zip'):
        logger.warning(f"User {update.effective_user.id} uploaded non-zip file: {document.file_name}")
        await update.message.reply_text("❌ 只支持 .zip 格式文件")
        return current_state
    
    # Download file
    logger.info(f"Downloading file: {document.file_name} ({document.file_size} bytes)")
    await update.message.reply_text("⏳ 正在下载文件...")
    
    try:
        file = await document.get_file()
        zip_path = os.path.join(Config.UPLOADS_DIR, f"{update.effective_user.id}_{document.file_name}")
        await file.download_to_drive(zip_path)
        logger.info(f"File downloaded successfully: {zip_path}")
        
        await update.message.reply_text("⏳ 正在导入账户...")
        logger.info(f"Starting account import from: {zip_path}")
        
        # Import accounts - 传递账户类型
        imported = await account_manager.import_session_zip(zip_path, account_type=account_type)
        
        if not imported:
            logger.warning(f"No accounts imported from {zip_path}")
            await update.message.reply_text(
                "❌ <b>导入失败</b>\n\n"
                "未找到有效的账户文件\n"
                "请检查 .zip 文件内容",
                parse_mode='HTML'
            )
        else:
            logger.info(f"Successfully imported {len(imported)} accounts")
            accounts_info = "\n".join([
                f"• {result['user'].first_name or ''} ({result['account'].phone})"
                for result in imported
            ])
            await update.message.reply_text(
                f"✅ <b>导入成功！</b>\n\n"
                f"成功导入 {len(imported)} 个账户：\n\n"
                f"{accounts_info}\n\n"
                f"使用 /start 查看账户列表",
                parse_mode='HTML'
            )
        
        # Cleanup
        try:
            os.remove(zip_path)
            logger.info(f"Cleaned up temporary file: {zip_path}")
        except Exception as e:
            logger.error(f"Failed to cleanup file {zip_path}: {e}")
        
        context.user_data.clear()
        return ConversationHandler.END
        
    except Exception as e:
        logger.error(f"Error importing accounts: {e}", exc_info=True)
        await update.message.reply_text(
            f"❌ <b>导入失败</b>\n\n"
            f"错误：{str(e)}\n\n"
            f"请检查文件格式是否正确",
            parse_mode='HTML'
        )
        return current_state


async def check_all_accounts_status(progress_callback=None):
    """
    Check all accounts using @spambot with enhanced multi-language pattern matching
    
    Args:
        progress_callback: Optional async function to call with progress updates
                          Should accept (current, total, stats) as parameters
    """
    # Only check messaging accounts
    accounts = list(db[Account.COLLECTION_NAME].find({'account_type': 'messaging'}))
    
    # 增强版状态模式 - 支持多语言和更精确的分类
    status_patterns = {
        # 地理限制提示 - 判定为无限制（优先级最高）
        # "some phone numbers may trigger a harsh response" 是地理限制，不是双向限制
        "地理限制": [
            "some phone numbers may trigger a harsh response",
            "phone numbers may trigger",
        ],
        "无限制": [
            "good news, no limits are currently applied",
            "you're free as a bird",
            "no limits",
            "free as a bird",
            "no restrictions",
            # 新增英文关键词
            "all good",
            "account is free",
            "working fine",
            "not limited",
            # 中文关键词
            "正常",
            "没有限制",
            "一切正常",
            "无限制"
        ],
        "临时限制": [
            # 临时限制的关键指标（优先级最高）
            "account is now limited until",
            "limited until",
            "account is limited until",
            "moderators have confirmed the report",
            "users found your messages annoying",
            "will be automatically released",
            "limitations will last longer next time",
            "while the account is limited",
            # 新增临时限制关键词
            "temporarily limited",
            "temporarily restricted",
            "temporary ban",
            # 中文关键词
            "暂时限制",
            "临时限制",
            "暂时受限"
        ],
        "垃圾邮件": [
            # 真正的限制 - "actions can trigger" 表示账号行为触发了限制
            "actions can trigger a harsh response from our anti-spam systems",
            "account was limited",
            "you will not be able to send messages",
            "limited by mistake",
            "peer flood",
            "you can only",
            # 中文关键词
            "违规",
        ],
        "冻结": [
            # 永久限制的关键指标
            "permanently banned",
            "account has been frozen permanently",
            "permanently restricted",
            "account is permanently",
            "banned permanently",
            "permanent ban",
            # 原有的patterns
            "account was blocked for violations",
            "telegram terms of service",
            "blocked for violations",
            "terms of service",
            "violations of the telegram",
            "banned",
            "suspended",
            # 中文关键词
            "永久限制",
            "永久封禁",
            "永久受限"
        ],
        "等待验证": [
            "wait",
            "pending",
            "verification",
            # 中文关键词
            "等待",
            "审核中",
            "验证"
        ]
    }
    
    status_results = {
        'unlimited': [],      # 无限制
        'limited': [],        # 双向限制/临时限制
        'restricted': [],     # 受限/冻结
        'banned': []          # 封禁/死亡账户
    }
    
    def classify_status(response_text):
        """
        Classify account status based on @spambot response with priority-based matching
        Returns: (category, status_value)
        """
        # 转换为小写以便匹配（支持英文）
        response_lower = response_text.lower()
        
        # 优先级1: 地理限制（判定为无限制）
        for pattern in status_patterns["地理限制"]:
            if pattern in response_lower:
                logger.info(f"Detected geographical restriction (treated as unlimited): {pattern}")
                return ('unlimited', AccountStatus.ACTIVE.value)
        
        # 优先级2: 临时限制
        for pattern in status_patterns["临时限制"]:
            if pattern in response_lower:
                logger.info(f"Detected temporary limitation: {pattern}")
                return ('limited', AccountStatus.LIMITED.value)
        
        # 优先级3: 冻结/永久封禁
        for pattern in status_patterns["冻结"]:
            if pattern in response_lower:
                logger.info(f"Detected permanent ban/freeze: {pattern}")
                return ('banned', AccountStatus.BANNED.value)
        
        # 优先级4: 垃圾邮件限制（双向限制）
        for pattern in status_patterns["垃圾邮件"]:
            if pattern in response_lower:
                logger.info(f"Detected spam limitation: {pattern}")
                return ('limited', AccountStatus.LIMITED.value)
        
        # 优先级5: 等待验证
        for pattern in status_patterns["等待验证"]:
            if pattern in response_lower:
                logger.info(f"Detected pending verification: {pattern}")
                return ('restricted', AccountStatus.LIMITED.value)
        
        # 优先级6: 无限制（最后检查）
        for pattern in status_patterns["无限制"]:
            if pattern in response_lower:
                logger.info(f"Detected unlimited status: {pattern}")
                return ('unlimited', AccountStatus.ACTIVE.value)
        
        # 默认：无法分类，归为无限制
        logger.warning(f"Unable to classify response, defaulting to unlimited: {response_text[:100]}...")
        return ('unlimited', AccountStatus.ACTIVE.value)
    
    # Process accounts with concurrency control (10 at a time)
    semaphore = asyncio.Semaphore(10)
    total_accounts = len(accounts)
    processed_count = 0
    
    async def check_account_with_semaphore(account_doc):
        """Check single account with semaphore"""
        nonlocal processed_count
        async with semaphore:
            account = Account.from_dict(account_doc)
            try:
                client = await account_manager.get_client(str(account._id))
                
                # 向 @spambot 发送消息
                spambot = await client.get_entity('spambot')
                await client.send_message(spambot, '/start')
                await asyncio.sleep(2)
                
                # 获取 @spambot 的回复
                messages = await client.get_messages(spambot, limit=1)
                if messages:
                    response = messages[0].text
                    logger.info(f"Account {account.phone} @spambot response: {response[:100]}...")
                    
                    # 使用增强的分类系统
                    category, new_status = classify_status(response)
                    
                    logger.info(f"✅ Account {account.phone} classified as: {category} → status: {new_status}")
                    
                    status_results[category].append(account)
                    
                    # 更新数据库
                    db[Account.COLLECTION_NAME].update_one(
                        {'_id': account._id},
                        {'$set': {'status': new_status, 'updated_at': datetime.utcnow()}}
                    )
                    
                    # 验证更新 (only in debug mode for performance)
                    if logger.isEnabledFor(logging.DEBUG):
                        updated_doc = db[Account.COLLECTION_NAME].find_one({'_id': account._id})
                        if updated_doc and 'status' in updated_doc:
                            logger.debug(f"✅ Database verified: {account.phone} status = {updated_doc['status']}")
                else:
                    # 没有收到回复，可能是无法对话
                    logger.warning(f"❌ Account {account.phone}: No response from @spambot, marking as BANNED")
                    status_results['banned'].append(account)
                    db[Account.COLLECTION_NAME].update_one(
                        {'_id': account._id},
                        {'$set': {'status': AccountStatus.BANNED.value, 'updated_at': datetime.utcnow()}}
                    )
                    # 验证更新 (only in debug mode for performance)
                    if logger.isEnabledFor(logging.DEBUG):
                        updated_doc = db[Account.COLLECTION_NAME].find_one({'_id': account._id})
                        if updated_doc and 'status' in updated_doc:
                            logger.debug(f"❌ Database verified: {account.phone} status = {updated_doc['status']}")
                    
            except Exception as e:
                # 无法连接或对话的账户认为是封禁/死亡账户
                logger.error(f"❌ Failed to check account {account.phone}: {e}, marking as BANNED")
                status_results['banned'].append(account)
                db[Account.COLLECTION_NAME].update_one(
                    {'_id': account._id},
                    {'$set': {'status': AccountStatus.BANNED.value, 'updated_at': datetime.utcnow()}}
                )
                # 验证更新 (only in debug mode for performance)
                if logger.isEnabledFor(logging.DEBUG):
                    updated_doc = db[Account.COLLECTION_NAME].find_one({'_id': account._id})
                    if updated_doc and 'status' in updated_doc:
                        logger.debug(f"❌ Database verified: {account.phone} status = {updated_doc['status']}")
            finally:
                processed_count += 1
                # Report progress every 5 accounts
                if progress_callback and (processed_count % 5 == 0 or processed_count == total_accounts):
                    await progress_callback(processed_count, total_accounts, status_results)
    
    # Process all accounts concurrently
    await asyncio.gather(*[check_account_with_semaphore(acc) for acc in accounts])
    
    return status_results


async def export_accounts(account_ids, export_type='all'):
    """Export accounts as zip file with enhanced error handling"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    export_dir = os.path.join(Config.RESULTS_DIR, f"export_{timestamp}")
    os.makedirs(export_dir, exist_ok=True)
    
    exported_count = 0
    failed_count = 0
    
    for account_id in account_ids:
        try:
            account_doc = db[Account.COLLECTION_NAME].find_one({'_id': ObjectId(account_id)})
            if not account_doc:
                logger.warning(f"Account {account_id} not found in database")
                failed_count += 1
                continue
            
            account = Account.from_dict(account_doc)
            session_name = account.session_name
            session_path = os.path.join(Config.SESSIONS_DIR, f"{session_name}.session")
            
            if os.path.exists(session_path):
                # 复制 session 文件
                shutil.copy2(session_path, export_dir)
                exported_count += 1
                logger.info(f"Exported session: {session_name}")
                
                # 如果有对应的 json 文件也复制
                json_path = f"{session_path}.json"
                if os.path.exists(json_path):
                    shutil.copy2(json_path, export_dir)
                    logger.info(f"Exported json: {session_name}.json")
            else:
                logger.warning(f"Session file not found: {session_path}")
                failed_count += 1
        except Exception as e:
            logger.error(f"Failed to export account {account_id}: {e}")
            failed_count += 1
    
    logger.info(f"Export summary: {exported_count} success, {failed_count} failed")
    
    # 打包为 zip
    zip_filename = f"accounts_{export_type}_{timestamp}.zip"
    zip_path = os.path.join(Config.RESULTS_DIR, zip_filename)
    
    with zipfile.ZipFile(zip_path, 'w') as zipf:
        for root, dirs, files in os.walk(export_dir):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.basename(file_path)
                zipf.write(file_path, arcname)
    
    # 清理临时目录
    shutil.rmtree(export_dir)
    
    logger.info(f"Created zip file: {zip_path} ({os.path.getsize(zip_path)} bytes)")
    
    return zip_path


async def list_accounts(query):
    """List accounts"""
    # 只查询 messaging 类型的账户
    account_docs = db[Account.COLLECTION_NAME].find({'account_type': 'messaging'})
    accounts = [Account.from_dict(doc) for doc in account_docs]
    
    if not accounts:
        text = "📱 <b>账户列表</b>\n\n暂无广告私信账户"
        keyboard = [
            [InlineKeyboardButton("➕ 添加账户", callback_data='accounts_add')],
            [InlineKeyboardButton("🔙 返回", callback_data='menu_accounts')]
        ]
    else:
        text = f"📱 <b>账户列表</b>\n\n共 {len(accounts)} 个广告账户：\n\n"
        keyboard = []
        
        for account in accounts:
            status_emoji = {'active': '✅', 'banned': '🚫', 'limited': '⚠️', 'inactive': '❌'}.get(account.status, '❓')
            text += (
                f"{status_emoji} <b>{account.phone}</b>\n"
                f"   状态: {account.status}\n"
                f"   今日: {account.messages_sent_today}/{account.daily_limit}\n\n"
            )
            keyboard.append([InlineKeyboardButton(f"检查 {account.phone}", callback_data=f'account_check_{str(account._id)}')])
        
        keyboard.append([InlineKeyboardButton("🔙 返回", callback_data='menu_accounts')])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='HTML')


async def check_account(query, account_id):
    """Check account"""
    result = await account_manager.check_account_status(account_id)
    if result:
        await query.message.reply_text("✅ 账户正常")
    else:
        await query.message.reply_text("❌ 账户异常")


async def show_tasks_menu(query):
    """Show tasks menu"""
    keyboard = [
        [InlineKeyboardButton("📋 查看任务列表", callback_data='tasks_list')],
        [InlineKeyboardButton("➕ 创建新任务", callback_data='tasks_create')],
        [InlineKeyboardButton("🔙 返回", callback_data='menu_messaging')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    text = "📝 <b>任务管理</b>\n\n请选择操作："
    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='HTML')


async def list_tasks(query):
    """List tasks with enhanced status display"""
    task_docs = db[Task.COLLECTION_NAME].find()
    tasks = [Task.from_dict(doc) for doc in task_docs]
    
    if not tasks:
        text = "📝 <b>任务列表</b>\n\n暂无任务"
        keyboard = [
            [InlineKeyboardButton("➕ 创建新任务", callback_data='tasks_create')],
            [InlineKeyboardButton("🔙 返回", callback_data='menu_tasks')]
        ]
    else:
        # Enhanced status display with counts
        status_counts = {}
        for task in tasks:
            status_counts[task.status] = status_counts.get(task.status, 0) + 1
        
        # Format status summary with emoji
        status_emoji_map = {
            'pending': '⏳', 
            'running': '🚀', 
            'paused': '⏸️', 
            'stopped': '⏹️',
            'completed': '✅', 
            'failed': '❌'
        }
        
        status_summary = " | ".join([
            f"{status_emoji_map.get(status, '❓')}{count}"
            for status, count in sorted(status_counts.items())
        ])
        
        text = (
            f"📝 <b>任务列表</b>\n\n"
            f"📊 共 {len(tasks)} 个任务 | {status_summary}\n\n"
            f"💡 点击任务查看详情\n"
        )
        keyboard = []
        
        # Show tasks in a 2-column grid with enhanced display
        row = []
        for idx, task in enumerate(tasks):
            status_emoji = status_emoji_map.get(task.status, '❓')
            
            # Add progress indicator for running tasks
            if task.status == 'running' and task.total_targets > 0:
                progress_pct = int((task.sent_count or 0) / task.total_targets * 100)
                button_text = f"{status_emoji} {task.name} ({progress_pct}%)"
            else:
                button_text = f"{status_emoji} {task.name}"
            
            row.append(InlineKeyboardButton(button_text, callback_data=f'task_detail_{str(task._id)}'))
            
            # Create a new row after every 2 tasks
            if len(row) == 2:
                keyboard.append(row)
                row = []
        
        # Add remaining task if odd number
        if row:
            keyboard.append(row)
        
        keyboard.append([InlineKeyboardButton("➕ 创建新任务", callback_data='tasks_create')])
        keyboard.append([InlineKeyboardButton("🔙 返回", callback_data='menu_tasks')])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='HTML')


async def show_task_detail(query, task_id):
    """Show task detail with enhanced display and configuration options"""
    task_doc = db[Task.COLLECTION_NAME].find_one({'_id': ObjectId(task_id)})
    if not task_doc:
        await safe_answer_query(query, "❌ 任务不存在", show_alert=True)
        return
    
    task = Task.from_dict(task_doc)
    
    # Enhanced status emoji mapping
    status_emoji_map = {
        'pending': '⏳', 
        'running': '🚀', 
        'paused': '⏸️', 
        'stopped': '⏹️',
        'completed': '✅', 
        'failed': '❌'
    }
    status_emoji = status_emoji_map.get(task.status, '❓')
    progress = (task.sent_count / task.total_targets * 100) if task.total_targets > 0 else 0
    
    # Build progress display for running tasks
    if task.status == TaskStatus.RUNNING.value:
        # Calculate unique users who received messages (targets with sent_at set)
        unique_users_sent = db[Target.COLLECTION_NAME].count_documents({
            'task_id': str(task_id),
            'sent_at': {'$ne': None}
        })
        
        # Enhanced running task display
        progress_bar_length = 20
        filled = int(progress / 5)  # 5% per bar
        progress_bar = '█' * filled + '░' * (progress_bar_length - filled)
        
        text = (
            f"🚀 <b>正在私信中</b>\n\n"
            f"📊 进度: {task.sent_count}/{task.total_targets} ({progress:.1f}%)\n"
            f"{progress_bar}\n\n"
            f"👥 总用户数: {task.total_targets}\n"
            f"✅ 发送成功: {task.sent_count} 条消息\n"
            f"📧 成功用户: {unique_users_sent} 人\n"
            f"❌ 发送失败: {task.failed_count}\n\n"
        )
        
        # Calculate estimated time
        if task.total_targets and task.sent_count is not None and task.failed_count is not None:
            remaining = task.total_targets - task.sent_count - task.failed_count
            if remaining > 0 and task.min_interval and task.max_interval:
                avg_interval = (task.min_interval + task.max_interval) / 2
                estimated_seconds = remaining * avg_interval
                hours, remainder = divmod(int(estimated_seconds), 3600)
                minutes, seconds = divmod(remainder, 60)
                text += f"⏱️ 预计剩余: {hours}:{minutes:02d}:{seconds:02d}\n"
        
        if task.started_at:
            elapsed = datetime.utcnow() - task.started_at
            hours, remainder = divmod(int(elapsed.total_seconds()), 3600)
            minutes, seconds = divmod(remainder, 60)
            text += f"⏰ 已运行: {hours}:{minutes:02d}:{seconds:02d}\n"
        
        text += f"\n💡 <i>任务可随时停止，不会丢失进度</i>"
    else:
        # Calculate unique users who received messages for completed/paused tasks
        unique_users_sent = db[Target.COLLECTION_NAME].count_documents({
            'task_id': str(task_id),
            'sent_at': {'$ne': None}
        })
        
        # Enhanced status display with badge
        status_text_map = {
            'pending': '待执行',
            'running': '运行中',
            'paused': '已暂停',
            'stopped': '已停止',
            'completed': '已完成',
            'failed': '已失败'
        }
        status_text = status_text_map.get(task.status, '未知')
        
        text = (
            f"{status_emoji} <b>{task.name}</b>\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"📌 状态: {status_text}\n\n"
            f"📊 <b>任务统计</b>\n"
            f"  • 进度: {task.sent_count}/{task.total_targets} ({progress:.1f}%)\n"
            f"  • 成功: {task.sent_count} 条消息\n"
            f"  • 用户: {unique_users_sent} 人\n"
            f"  • 失败: {task.failed_count}\n\n"
            f"⚙️ <b>任务配置</b>\n"
            f"  • 线程数: {task.thread_count}\n"
            f"  • 间隔: {task.min_interval}-{task.max_interval}秒\n"
            f"  • 日限: {task.daily_limit}条/账号\n"
            f"  • 无视双向: {task.ignore_bidirectional_limit}次\n"
            f"  • 重复发送: {'✔️' if task.repeat_send else '❌'}\n"
            f"  • 强制私信模式: {'✔️' if task.force_private_mode else '❌'}\n"
            f"  • 置顶消息: {'✔️' if task.pin_message else '❌'}\n"
            f"  • 删除对话: {'✔️' if task.delete_dialog else '❌'}\n"
        )
        
        if task.started_at:
            elapsed = datetime.utcnow() - task.started_at
            hours, remainder = divmod(int(elapsed.total_seconds()), 3600)
            minutes, seconds = divmod(remainder, 60)
            text += f"\n⏰ 运行时长: {hours}:{minutes:02d}:{seconds:02d}\n"
    
    keyboard = []
    
    # Configuration buttons (only if not running)
    if task.status != TaskStatus.RUNNING.value:
        keyboard.append([
            InlineKeyboardButton("⚙️ 参数配置", callback_data=f'task_config_{task_id}'),
            InlineKeyboardButton("🗑️ 删除任务", callback_data=f'task_delete_{task_id}')
        ])
    
    # Start/Stop buttons
    if task.status in [TaskStatus.PENDING.value, TaskStatus.PAUSED.value]:
        keyboard.append([InlineKeyboardButton("▶️ 开始私信", callback_data=f'task_start_{task_id}')])
    elif task.status == TaskStatus.RUNNING.value:
        keyboard.append([
            InlineKeyboardButton("🔄 刷新进度", callback_data=f'task_detail_{task_id}'),
            InlineKeyboardButton("⏸️ 停止任务", callback_data=f'task_stop_{task_id}')
        ])
        
        # Start auto-refresh for running tasks if not already running
        if not hasattr(task_manager, 'refresh_tasks'):
            task_manager.refresh_tasks = {}
        
        # Only start auto-refresh if not already running for this task
        if task_id not in task_manager.refresh_tasks or task_manager.refresh_tasks[task_id].done():
            async def auto_refresh_wrapper():
                try:
                    # Wait a moment before starting refresh
                    await asyncio.sleep(2)
                    await auto_refresh_task_progress(
                        query.bot,
                        query.message.chat_id,
                        query.message.message_id,
                        task_id
                    )
                except asyncio.CancelledError:
                    logger.info(f"Auto-refresh task for task {task_id} was cancelled")
                except Exception as e:
                    logger.error(f"Auto-refresh error for task {task_id}: {e}", exc_info=True)
            
            refresh_task = asyncio.create_task(auto_refresh_wrapper())
            task_manager.refresh_tasks[task_id] = refresh_task
            logger.info(f"Started auto-refresh for running task {task_id}")
    
    # Export button for completed tasks
    if task.status == TaskStatus.COMPLETED.value:
        keyboard.append([InlineKeyboardButton("📥 导出结果", callback_data=f'task_export_{task_id}')])
    
    keyboard.append([InlineKeyboardButton("🔙 返回任务列表", callback_data='tasks_list')])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='HTML')


async def show_task_config(query, task_id):
    """Show task configuration options"""
    task_doc = db[Task.COLLECTION_NAME].find_one({'_id': ObjectId(task_id)})
    if not task_doc:
        await safe_answer_query(query, "❌ 任务不存在", show_alert=True)
        return
    
    task = Task.from_dict(task_doc)
    
    text = (
        f"⚙️ <b>配置 - {task.name}</b>\n\n"
        f"当前配置如下，点击按钮进行调整："
    )
    
    keyboard = [
        [
            InlineKeyboardButton(f"🧵 线程数: {task.thread_count}", callback_data=f'cfg_thread_{task_id}'),
            InlineKeyboardButton(f"⏱️ 间隔: {task.min_interval}-{task.max_interval}s", callback_data=f'cfg_interval_{task_id}')
        ],
        [InlineKeyboardButton(f"🔄 无视双向: {task.ignore_bidirectional_limit}次", callback_data=f'cfg_bidirect_{task_id}')],
        [
            InlineKeyboardButton(f"{'✔️' if task.pin_message else '❌'} 置顶消息", callback_data=f'cfg_toggle_pin_{task_id}'),
            InlineKeyboardButton(f"{'✔️' if task.delete_dialog else '❌'} 删除对话", callback_data=f'cfg_toggle_delete_{task_id}')
        ],
        [InlineKeyboardButton(f"{'✔️' if task.repeat_send else '❌'} 重复发送", callback_data=f'cfg_toggle_repeat_{task_id}')],
        [
            InlineKeyboardButton(f"✏️ 编辑模式", callback_data=f'cfg_edit_mode_{task_id}'),
            InlineKeyboardButton(f"💬 回复模式", callback_data=f'cfg_reply_mode_{task_id}')
        ],
        [
            InlineKeyboardButton(f"⏸️ 批次停顿", callback_data=f'cfg_batch_pause_{task_id}'),
            InlineKeyboardButton(f"🌊 FloodWait策略", callback_data=f'cfg_flood_strategy_{task_id}')
        ],
        [
            InlineKeyboardButton(f"📞 语音拨打", callback_data=f'cfg_voice_call_{task_id}'),
            InlineKeyboardButton(f"⏲️ 线程启动间隔: {task.thread_start_interval}s", callback_data=f'cfg_thread_interval_{task_id}')
        ],
        [
            InlineKeyboardButton(f"📊 单账号日限: {task.daily_limit}条", callback_data=f'cfg_daily_limit_{task_id}'),
            InlineKeyboardButton(f"🔄 重试: {task.retry_count}次", callback_data=f'cfg_retry_{task_id}')
        ],
        [
            InlineKeyboardButton(f"{'✔️' if task.auto_switch_dead_account else '❌'} 死号自动换号", callback_data=f'cfg_toggle_dead_account_{task_id}'),
            InlineKeyboardButton(f"{'✔️' if task.force_private_mode else '❌'} 强制私信模式", callback_data=f'cfg_toggle_force_private_{task_id}')
        ],
        [InlineKeyboardButton("✅ 配置完成", callback_data=f'task_detail_{task_id}')],
        [InlineKeyboardButton("🔙 返回", callback_data=f'task_detail_{task_id}')]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Fix Bug 2: Handle "Message to edit not found" error
    try:
        await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='HTML')
    except telegram_error.BadRequest as e:
        if "Message to edit not found" in str(e) or "message to edit not found" in str(e):
            await query.message.reply_text(text, reply_markup=reply_markup, parse_mode='HTML')
        else:
            raise


async def request_thread_config(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Request thread count configuration"""
    query = update.callback_query
    await safe_answer_query(query)
    task_id = query.data.split('_')[2]
    context.user_data['config_task_id'] = task_id
    context.user_data['retry_count'] = 0
    context.user_data['current_config_type'] = 'thread'
    
    keyboard = [
        [
            InlineKeyboardButton("💡 查看示例", callback_data='cfg_example_thread'),
            InlineKeyboardButton("❌ 取消", callback_data=f'cfg_cancel_{task_id}')
        ],
        [InlineKeyboardButton("🔙 返回", callback_data=f'task_config_{task_id}')]
    ]
    
    prompt_msg = await query.message.reply_text(
        "🧵 <b>配置线程数</b>\n\n"
        "请输入要使用的账号数量（线程数）：\n\n"
        "💡 建议：1-10\n"
        "⚠️ 线程数越多，发送速度越快，但风险也越高\n\n"
        "💬 提示：可以随时点击下方按钮取消或查看示例",
        parse_mode='HTML',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    # Store prompt message ID for later deletion
    context.user_data['config_prompt_msg_id'] = prompt_msg.message_id
    return CONFIG_THREAD_INPUT


async def request_interval_config(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Request interval configuration"""
    query = update.callback_query
    await safe_answer_query(query)
    task_id = query.data.split('_')[2]
    context.user_data['config_task_id'] = task_id
    context.user_data['retry_count'] = 0
    context.user_data['current_config_type'] = 'interval'
    
    keyboard = [
        [
            InlineKeyboardButton("💡 查看示例", callback_data='cfg_example_interval'),
            InlineKeyboardButton("❌ 取消", callback_data=f'cfg_cancel_{task_id}')
        ],
        [InlineKeyboardButton("🔙 返回", callback_data=f'task_config_{task_id}')]
    ]
    
    prompt_msg = await query.message.reply_text(
        "⏱️ <b>配置发送间隔</b>\n\n"
        "请输入最小间隔和最大间隔（秒），用空格分隔：\n\n"
        "💡 格式：最小值 最大值\n"
        "💡 例如：30 120\n"
        "⚠️ 间隔越短，风险越高\n\n"
        "💬 提示：可以随时点击下方按钮取消或查看示例",
        parse_mode='HTML',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    # Store prompt message ID for later deletion
    context.user_data['config_prompt_msg_id'] = prompt_msg.message_id
    return CONFIG_INTERVAL_MIN_INPUT


async def request_bidirect_config(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Request bidirectional limit configuration"""
    query = update.callback_query
    await safe_answer_query(query)
    task_id = query.data.split('_')[2]
    context.user_data['config_task_id'] = task_id
    context.user_data['retry_count'] = 0
    context.user_data['current_config_type'] = 'bidirect'
    
    keyboard = [
        [
            InlineKeyboardButton("💡 查看示例", callback_data='cfg_example_bidirect'),
            InlineKeyboardButton("❌ 取消", callback_data=f'cfg_cancel_{task_id}')
        ],
        [InlineKeyboardButton("🔙 返回", callback_data=f'task_config_{task_id}')]
    ]
    
    prompt_msg = await query.message.reply_text(
        "🔄 <b>配置无视双向次数</b>\n\n"
        "请输入无视双向联系人限制的次数：\n\n"
        "💡 0 = 不忽略限制\n"
        "💡 1-999 = 忽略次数\n"
        "⚠️ 设置过高可能导致封号\n\n"
        "💬 提示：可以随时点击下方按钮取消或查看示例",
        parse_mode='HTML',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    # Store prompt message ID for later deletion
    context.user_data['config_prompt_msg_id'] = prompt_msg.message_id
    return CONFIG_BIDIRECT_INPUT


async def start_create_task(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Start task creation - Conversation entry point.
    
    Handles the tasks_create callback, prompts the user to input a task name,
    and transitions to TASK_NAME_INPUT state.
    
    Returns:
        int: TASK_NAME_INPUT state constant
    """
    query = update.callback_query
    await safe_answer_query(query)
    logger.info(f"User {query.from_user.id} starting task creation")
    await query.message.reply_text("➕ <b>创建新任务</b>\n\n请输入任务名称：", parse_mode='HTML')
    context.user_data['creating_task'] = True
    return TASK_NAME_INPUT


async def handle_task_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle task name"""
    context.user_data['task_name'] = update.message.text
    
    # Now go directly to send method selection
    keyboard = [
        [InlineKeyboardButton("📤 直接发送", callback_data='sendmethod_direct')],
        [InlineKeyboardButton("🤖 Post代码", callback_data='sendmethod_postbot')],
        [InlineKeyboardButton("📢 频道转发", callback_data='sendmethod_channel_forward')],
        [InlineKeyboardButton("🔒 隐藏转发来源", callback_data='sendmethod_channel_forward_hidden')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        f"✅ 任务名称: <b>{update.message.text}</b>\n\n"
        "📮 <b>请选择发送方式配置：</b>\n\n"
        "📤 <b>直接发送</b> - 请配置文本消息（可以纯文字，也可以直接发图片带文字）\n"
        "🤖 <b>Post代码</b> - 使用 @postbot 配置的图文按钮\n"
        "📢 <b>频道转发</b> - 转发频道帖子\n"
        "🔒 <b>隐藏转发来源</b> - 转发频道帖子但隐藏来源",
        parse_mode='HTML',
        reply_markup=reply_markup
    )
    return SEND_METHOD_SELECT


async def handle_message_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle message input"""
    context.user_data['message_text'] = update.message.text
    keyboard = [
        [InlineKeyboardButton("📝 纯文本", callback_data='format_plain')],
        [InlineKeyboardButton("📌 Markdown", callback_data='format_markdown')],
        [InlineKeyboardButton("🏷️ HTML", callback_data='format_html')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("✅ 消息已保存\n\n请选择格式：", reply_markup=reply_markup)
    return FORMAT_SELECT


async def select_media_type(query):
    """Select media type"""
    keyboard = [
        [InlineKeyboardButton("📝 纯文本", callback_data='media_text')],
        [InlineKeyboardButton("🖼️ 图片", callback_data='media_image')],
        [InlineKeyboardButton("🎥 视频", callback_data='media_video')],
        [InlineKeyboardButton("📄 文档", callback_data='media_document')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.message.reply_text("请选择媒体类型：", reply_markup=reply_markup)
    return MEDIA_SELECT


async def request_media_upload(query):
    """Request media upload"""
    await query.message.reply_text("请上传媒体文件：")
    return MEDIA_UPLOAD


async def handle_media_upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle media file upload"""
    user_id = update.effective_user.id
    logger.info(f"User {user_id} uploading media file")
    
    try:
        if not update.message.document and not update.message.photo and not update.message.video:
            await update.message.reply_text("❌ 请上传有效的媒体文件")
            return MEDIA_UPLOAD
        
        # Save the file
        if update.message.document:
            file = await update.message.document.get_file()
            file_ext = os.path.splitext(update.message.document.file_name)[1]
        elif update.message.photo:
            file = await update.message.photo[-1].get_file()
            file_ext = '.jpg'
        elif update.message.video:
            file = await update.message.video.get_file()
            file_ext = '.mp4'
        else:
            await update.message.reply_text("❌ 不支持的媒体类型")
            return MEDIA_UPLOAD
        
        # Save to media directory
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"media_{user_id}_{timestamp}{file_ext}"
        media_path = os.path.join(Config.MEDIA_DIR, filename)
        await file.download_to_drive(media_path)
        
        context.user_data['media_path'] = media_path
        logger.info(f"User {user_id} uploaded media to {media_path}")
        
        await update.message.reply_text("✅ 媒体文件已保存")
        
        # Show preview before going to target list
        return await show_preview_from_update(update, context)
        
    except Exception as e:
        logger.error(f"Error handling media upload for user {user_id}: {e}", exc_info=True)
        await update.message.reply_text(f"❌ 上传失败：{str(e)}")
        return MEDIA_UPLOAD


async def request_postbot_code(query):
    """Request postbot code input"""
    await query.message.reply_text(
        "🤖 <b>Post代码输入</b>\n\n"
        "请输入从 @postbot 获取的代码：\n\n"
        "💡 提示：使用 @postbot 创建图文按钮后，复制生成的代码粘贴到这里",
        parse_mode='HTML'
    )
    return POSTBOT_CODE_INPUT


async def handle_postbot_code_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle postbot code input with validation"""
    code = update.message.text.strip()
    
    # Validate postbot code format (must be like 693af80c53cb2)
    # Pattern: alphanumeric characters, minimum length defined by constant
    if not re.match(rf'^[a-zA-Z0-9]{{{Config.POSTBOT_CODE_MIN_LENGTH},}}$', code):
        await update.message.reply_text(
            "❌ <b>代码格式错误</b>\n\n"
            "Post代码格式应该类似：<code>693af80c53cb2</code>\n\n"
            "请重新输入正确的代码：",
            parse_mode='HTML'
        )
        return POSTBOT_CODE_INPUT
    
    context.user_data['postbot_code'] = code
    context.user_data['message_text'] = f"使用 @postbot 代码: {code}"
    context.user_data['message_format'] = MessageFormat.PLAIN
    context.user_data['media_type'] = MediaType.TEXT
    
    await update.message.reply_text("✅ Post代码已保存")
    
    # Show preview before going to target list
    return await show_preview_from_update(update, context)


async def request_channel_link(query):
    """Request channel link input"""
    await query.message.reply_text(
        "📢 <b>频道链接输入</b>\n\n"
        "请输入频道帖子链接：\n\n"
        "💡 格式：https://t.me/channel_name/message_id",
        parse_mode='HTML'
    )
    return CHANNEL_LINK_INPUT


async def handle_channel_link_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle channel link input"""
    link = update.message.text.strip()
    context.user_data['channel_link'] = link
    
    # Set default values for channel forward
    send_method = context.user_data.get('send_method', SendMethod.CHANNEL_FORWARD)
    if send_method == SendMethod.CHANNEL_FORWARD_HIDDEN:
        context.user_data['message_text'] = f"转发频道帖子（隐藏来源）: {link}"
    else:
        context.user_data['message_text'] = f"转发频道帖子: {link}"
    
    context.user_data['message_format'] = MessageFormat.PLAIN
    context.user_data['media_type'] = MediaType.FORWARD
    
    await update.message.reply_text("✅ 频道链接已保存")
    
    # Show preview before going to target list
    return await show_preview_from_update(update, context)


async def show_preview(query, context):
    """Show preview of configured message"""
    message_text = context.user_data.get('message_text', '')
    message_format = context.user_data.get('message_format', MessageFormat.PLAIN)
    send_method = context.user_data.get('send_method', SendMethod.DIRECT)
    media_type = context.user_data.get('media_type', MediaType.TEXT)
    
    preview_text = (
        "👁️ <b>预览配置的广告文案！</b>\n\n"
        f"📮 发送方式：{SEND_METHOD_LABELS.get(send_method, send_method.value)}\n"
        f"📝 消息格式：{message_format.value}\n"
        f"📦 媒体类型：{MEDIA_TYPE_LABELS.get(media_type, media_type.value)}\n\n"
        f"<b>消息内容：</b>\n{message_text[:200]}{'...' if len(message_text) > 200 else ''}\n\n"
        f"======下一步===\n"
        f"✅ 配置完成"
    )
    
    keyboard = [
        [InlineKeyboardButton("✅ 配置完成", callback_data='preview_continue')],
        [InlineKeyboardButton("🔙 返回修改", callback_data='preview_back')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.message.reply_text(preview_text, parse_mode='HTML', reply_markup=reply_markup)
    return PREVIEW_CONFIG


async def show_preview_from_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show preview from update message (helper for text input handlers)"""
    message_text = context.user_data.get('message_text', '')
    message_format = context.user_data.get('message_format', MessageFormat.PLAIN)
    send_method = context.user_data.get('send_method', SendMethod.DIRECT)
    media_type = context.user_data.get('media_type', MediaType.TEXT)
    
    preview_text = (
        "👁️ <b>预览配置的广告文案！</b>\n\n"
        f"📮 发送方式：{SEND_METHOD_LABELS.get(send_method, send_method.value)}\n"
        f"📝 消息格式：{message_format.value}\n"
        f"📦 媒体类型：{MEDIA_TYPE_LABELS.get(media_type, media_type.value)}\n\n"
        f"<b>消息内容：</b>\n{message_text[:200]}{'...' if len(message_text) > 200 else ''}\n\n"
        f"======下一步===\n"
        f"✅ 配置完成"
    )
    
    keyboard = [
        [InlineKeyboardButton("✅ 配置完成", callback_data='preview_continue')],
        [InlineKeyboardButton("🔙 返回修改", callback_data='preview_back')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(preview_text, parse_mode='HTML', reply_markup=reply_markup)
    return PREVIEW_CONFIG


async def request_target_list_from_update(update: Update):
    """Request target list from update (helper for text input handlers)"""
    await update.message.reply_text(
        "✅ 配置完成\n\n"
        "请发送目标列表：\n"
        "1️⃣ 直接发送（每行一个）\n"
        "2️⃣ 上传 .txt 文件\n\n"
        "格式：@username 或 用户ID"
    )
    return TARGET_INPUT


async def request_target_list(query):
    """Request target list"""
    await query.message.reply_text(
        "✅ <b>配置完成</b>\n\n"
        "<b>请发送目标列表：</b>\n"
        "1️⃣ 直接发送（每行一个）\n"
        "2️⃣ 上传 .txt 文件\n\n"
        "格式：@username（不带@也行）或 用户ID",
        parse_mode='HTML'
    )
    return TARGET_INPUT


async def handle_target_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle target input"""
    user_id = update.effective_user.id
    logger.info(f"User {user_id} submitting target input")
    
    try:
        if update.message.text:
            logger.info(f"User {user_id} sent text input")
            targets = update.message.text.strip().split('\n')
            logger.info(f"Parsed {len(targets)} targets from text")
        elif update.message.document:
            logger.info(f"User {user_id} sent document: {update.message.document.file_name}")
            file = await update.message.document.get_file()
            content = await file.download_as_bytearray()
            logger.info(f"Downloaded file: {len(content)} bytes")
            targets = task_manager.parse_target_file(bytes(content))
            logger.info(f"Parsed {len(targets)} targets from file")
        else:
            logger.warning(f"User {user_id} sent invalid input (no text or document)")
            await update.message.reply_text("❌ 无效输入\n\n请发送文本或上传 .txt 文件")
            return TARGET_INPUT
        
        if not targets:
            logger.warning(f"User {user_id} submitted empty target list")
            await update.message.reply_text("❌ 目标列表为空\n\n请添加至少一个目标")
            return TARGET_INPUT
        
        # Count original targets before deduplication
        original_count = len(targets)
        
        logger.info(f"Creating task for user {user_id}")
        task = task_manager.create_task(
            name=context.user_data['task_name'],
            message_text=context.user_data['message_text'],
            message_format=context.user_data['message_format'],
            media_type=context.user_data.get('media_type', MediaType.TEXT),
            media_path=context.user_data.get('media_path'),
            send_method=context.user_data.get('send_method', SendMethod.DIRECT),
            postbot_code=context.user_data.get('postbot_code'),
            channel_link=context.user_data.get('channel_link'),
            min_interval=Config.DEFAULT_MIN_INTERVAL,
            max_interval=Config.DEFAULT_MAX_INTERVAL
        )
        
        logger.info(f"Adding {len(targets)} targets to task {task._id}")
        added = task_manager.add_targets(task._id, targets)
        logger.info(f"Successfully added {added} targets to task {task._id}")
        
        # Calculate deduplication stats
        duplicates = original_count - added
        
        # Create quick action buttons
        keyboard = [
            [InlineKeyboardButton("📋 前往任务列表", callback_data='tasks_list')],
            [InlineKeyboardButton("⚙️ 配置任务", callback_data=f'task_config_{str(task._id)}')]
        ]
        
        await update.message.reply_text(
            f"✅ <b>任务创建成功！</b>\n\n"
            f"📝 任务名称: {task.name}\n"
            f"📊 已收到 {original_count} 个用户\n"
            f"🔄 已去重 {duplicates} 个用户\n"
            f"✅ 最终添加 {added} 个用户\n\n"
            f"<b>注意：</b>用户名发一个自动删除一个，用完代表任务结束\n\n"
            f"使用下方按钮快速访问：",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
        
        context.user_data.clear()
        return ConversationHandler.END
        
    except Exception as e:
        logger.error(f"Error handling target input for user {user_id}: {e}", exc_info=True)
        await update.message.reply_text(
            f"❌ <b>处理失败</b>\n\n"
            f"错误：{str(e)}\n\n"
            f"请重试或使用 /start 返回主菜单",
            parse_mode='HTML'
        )
        return TARGET_INPUT


async def handle_thread_config(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle thread count configuration"""
    task_id = context.user_data.get('config_task_id')
    if not task_id:
        await update.message.reply_text("❌ 配置会话已过期，请重新开始")
        return ConversationHandler.END
    
    try:
        thread_count = int(update.message.text.strip())
        if thread_count < 1 or thread_count > 50:
            # Add retry count
            retry_count = context.user_data.get('retry_count', 0) + 1
            context.user_data['retry_count'] = retry_count
            
            if retry_count >= 3:
                msg = await update.message.reply_text(
                    "❌ <b>输入错误次数过多</b>\n\n"
                    "已自动取消配置，请重新开始",
                    parse_mode='HTML'
                )
                await asyncio.sleep(2)
                try:
                    await msg.delete()
                    await update.message.delete()
                    if 'config_prompt_msg_id' in context.user_data:
                        await context.bot.delete_message(
                            chat_id=update.effective_chat.id,
                            message_id=context.user_data['config_prompt_msg_id']
                        )
                except Exception:
                    pass
                context.user_data.clear()
                return ConversationHandler.END
            
            await update.message.reply_text(
                f"❌ <b>格式错误（第{retry_count}次）</b>\n\n"
                f"线程数必须在 1-50 之间\n"
                f"还剩 {3 - retry_count} 次尝试机会",
                parse_mode='HTML'
            )
            return CONFIG_THREAD_INPUT
        
        db[Task.COLLECTION_NAME].update_one(
            {'_id': ObjectId(task_id)},
            {'$set': {'thread_count': thread_count, 'updated_at': datetime.utcnow()}}
        )
        
        msg = await update.message.reply_text(f"✅ 线程数已设置为：{thread_count}")
        # Auto-delete after configured delay
        await asyncio.sleep(Config.CONFIG_MESSAGE_DELETE_DELAY)
        try:
            # Delete confirmation message
            await msg.delete()
            # Delete user input message
            await update.message.delete()
            # Delete prompt message
            prompt_msg_id = context.user_data.get('config_prompt_msg_id')
            if prompt_msg_id:
                await context.bot.delete_message(
                    chat_id=update.effective_chat.id,
                    message_id=prompt_msg_id
                )
        except Exception as e:
            logger.warning(f"Failed to delete config message: {e}")
        
        context.user_data.clear()
        return ConversationHandler.END
        
    except ValueError:
        # Add retry count
        retry_count = context.user_data.get('retry_count', 0) + 1
        context.user_data['retry_count'] = retry_count
        
        if retry_count >= 3:
            msg = await update.message.reply_text(
                "❌ <b>输入错误次数过多</b>\n\n"
                "已自动取消配置，请重新开始",
                parse_mode='HTML'
            )
            await asyncio.sleep(2)
            try:
                await msg.delete()
                await update.message.delete()
                if 'config_prompt_msg_id' in context.user_data:
                    await context.bot.delete_message(
                        chat_id=update.effective_chat.id,
                        message_id=context.user_data['config_prompt_msg_id']
                    )
            except Exception:
                pass
            context.user_data.clear()
            return ConversationHandler.END
        
        await update.message.reply_text(
            f"❌ <b>格式错误（第{retry_count}次）</b>\n\n"
            f"请输入有效的数字\n"
            f"还剩 {3 - retry_count} 次尝试机会",
            parse_mode='HTML'
        )
        return CONFIG_THREAD_INPUT


async def handle_interval_config(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle interval configuration"""
    task_id = context.user_data.get('config_task_id')
    if not task_id:
        await update.message.reply_text("❌ 配置会话已过期，请重新开始")
        return ConversationHandler.END
    
    try:
        parts = update.message.text.strip().split()
        if len(parts) != 2:
            retry_count = context.user_data.get('retry_count', 0) + 1
            context.user_data['retry_count'] = retry_count
            
            if retry_count >= 3:
                msg = await update.message.reply_text(
                    "❌ <b>输入错误次数过多</b>\n\n"
                    "已自动取消配置，请重新开始",
                    parse_mode='HTML'
                )
                await asyncio.sleep(2)
                try:
                    await msg.delete()
                    await update.message.delete()
                    if 'config_prompt_msg_id' in context.user_data:
                        await context.bot.delete_message(
                            chat_id=update.effective_chat.id,
                            message_id=context.user_data['config_prompt_msg_id']
                        )
                except Exception:
                    pass
                context.user_data.clear()
                return ConversationHandler.END
            
            await update.message.reply_text(
                f"❌ <b>格式错误（第{retry_count}次）</b>\n\n"
                f"格式错误，请输入两个数字（用空格分隔）\n"
                f"还剩 {3 - retry_count} 次尝试机会",
                parse_mode='HTML'
            )
            return CONFIG_INTERVAL_MIN_INPUT
        
        min_interval = int(parts[0])
        max_interval = int(parts[1])
        
        if min_interval < 1 or max_interval < min_interval or max_interval > 3600:
            retry_count = context.user_data.get('retry_count', 0) + 1
            context.user_data['retry_count'] = retry_count
            
            if retry_count >= 3:
                msg = await update.message.reply_text(
                    "❌ <b>输入错误次数过多</b>\n\n"
                    "已自动取消配置，请重新开始",
                    parse_mode='HTML'
                )
                await asyncio.sleep(2)
                try:
                    await msg.delete()
                    await update.message.delete()
                    if 'config_prompt_msg_id' in context.user_data:
                        await context.bot.delete_message(
                            chat_id=update.effective_chat.id,
                            message_id=context.user_data['config_prompt_msg_id']
                        )
                except Exception:
                    pass
                context.user_data.clear()
                return ConversationHandler.END
            
            await update.message.reply_text(
                f"❌ <b>格式错误（第{retry_count}次）</b>\n\n"
                f"间隔设置不合理：最小值 ≥ 1，最大值 ≥ 最小值，最大值 ≤ 3600\n"
                f"还剩 {3 - retry_count} 次尝试机会",
                parse_mode='HTML'
            )
            return CONFIG_INTERVAL_MIN_INPUT
        
        db[Task.COLLECTION_NAME].update_one(
            {'_id': ObjectId(task_id)},
            {'$set': {
                'min_interval': min_interval,
                'max_interval': max_interval,
                'updated_at': datetime.utcnow()
            }}
        )
        
        msg = await update.message.reply_text(f"✅ 发送间隔已设置为：{min_interval}-{max_interval} 秒")
        # Auto-delete after configured delay
        await asyncio.sleep(Config.CONFIG_MESSAGE_DELETE_DELAY)
        try:
            # Delete confirmation message
            await msg.delete()
            # Delete user input message
            await update.message.delete()
            # Delete prompt message
            prompt_msg_id = context.user_data.get('config_prompt_msg_id')
            if prompt_msg_id:
                await context.bot.delete_message(
                    chat_id=update.effective_chat.id,
                    message_id=prompt_msg_id
                )
        except Exception as e:
            logger.warning(f"Failed to delete config message: {e}")
        
        context.user_data.clear()
        return ConversationHandler.END
        
    except ValueError:
        retry_count = context.user_data.get('retry_count', 0) + 1
        context.user_data['retry_count'] = retry_count
        
        if retry_count >= 3:
            msg = await update.message.reply_text(
                "❌ <b>输入错误次数过多</b>\n\n"
                "已自动取消配置，请重新开始",
                parse_mode='HTML'
            )
            await asyncio.sleep(2)
            try:
                await msg.delete()
                await update.message.delete()
                if 'config_prompt_msg_id' in context.user_data:
                    await context.bot.delete_message(
                        chat_id=update.effective_chat.id,
                        message_id=context.user_data['config_prompt_msg_id']
                    )
            except Exception:
                pass
            context.user_data.clear()
            return ConversationHandler.END
        
        await update.message.reply_text(
            f"❌ <b>格式错误（第{retry_count}次）</b>\n\n"
            f"请输入有效的数字\n"
            f"还剩 {3 - retry_count} 次尝试机会",
            parse_mode='HTML'
        )
        return CONFIG_INTERVAL_MIN_INPUT


async def handle_bidirect_config(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle bidirectional limit configuration"""
    task_id = context.user_data.get('config_task_id')
    if not task_id:
        await update.message.reply_text("❌ 配置会话已过期，请重新开始")
        return ConversationHandler.END
    
    try:
        limit = int(update.message.text.strip())
        if limit < 0 or limit > 999:
            retry_count = context.user_data.get('retry_count', 0) + 1
            context.user_data['retry_count'] = retry_count
            
            if retry_count >= 3:
                msg = await update.message.reply_text(
                    "❌ <b>输入错误次数过多</b>\n\n"
                    "已自动取消配置，请重新开始",
                    parse_mode='HTML'
                )
                await asyncio.sleep(2)
                try:
                    await msg.delete()
                    await update.message.delete()
                    if 'config_prompt_msg_id' in context.user_data:
                        await context.bot.delete_message(
                            chat_id=update.effective_chat.id,
                            message_id=context.user_data['config_prompt_msg_id']
                        )
                except Exception:
                    pass
                context.user_data.clear()
                return ConversationHandler.END
            
            await update.message.reply_text(
                f"❌ <b>格式错误（第{retry_count}次）</b>\n\n"
                f"次数必须在 0-999 之间\n"
                f"还剩 {3 - retry_count} 次尝试机会",
                parse_mode='HTML'
            )
            return CONFIG_BIDIRECT_INPUT
        
        db[Task.COLLECTION_NAME].update_one(
            {'_id': ObjectId(task_id)},
            {'$set': {'ignore_bidirectional_limit': limit, 'updated_at': datetime.utcnow()}}
        )
        
        msg = await update.message.reply_text(f"✅ 无视双向次数已设置为：{limit}")
        # Auto-delete after configured delay
        await asyncio.sleep(Config.CONFIG_MESSAGE_DELETE_DELAY)
        try:
            # Delete confirmation message
            await msg.delete()
            # Delete user input message
            await update.message.delete()
            # Delete prompt message
            prompt_msg_id = context.user_data.get('config_prompt_msg_id')
            if prompt_msg_id:
                await context.bot.delete_message(
                    chat_id=update.effective_chat.id,
                    message_id=prompt_msg_id
                )
        except Exception as e:
            logger.warning(f"Failed to delete config message: {e}")
        
        context.user_data.clear()
        return ConversationHandler.END
        
    except ValueError:
        retry_count = context.user_data.get('retry_count', 0) + 1
        context.user_data['retry_count'] = retry_count
        
        if retry_count >= 3:
            msg = await update.message.reply_text(
                "❌ <b>输入错误次数过多</b>\n\n"
                "已自动取消配置，请重新开始",
                parse_mode='HTML'
            )
            await asyncio.sleep(2)
            try:
                await msg.delete()
                await update.message.delete()
                if 'config_prompt_msg_id' in context.user_data:
                    await context.bot.delete_message(
                        chat_id=update.effective_chat.id,
                        message_id=context.user_data['config_prompt_msg_id']
                    )
            except Exception:
                pass
            context.user_data.clear()
            return ConversationHandler.END
        
        await update.message.reply_text(
            f"❌ <b>格式错误（第{retry_count}次）</b>\n\n"
            f"请输入有效的数字\n"
            f"还剩 {3 - retry_count} 次尝试机会",
            parse_mode='HTML'
        )
        return CONFIG_BIDIRECT_INPUT
        await asyncio.sleep(Config.CONFIG_MESSAGE_DELETE_DELAY)
        try:
            # Delete confirmation message
            await msg.delete()
            # Delete user input message
            await update.message.delete()
            # Delete prompt message
            prompt_msg_id = context.user_data.get('config_prompt_msg_id')
            if prompt_msg_id:
                await context.bot.delete_message(
                    chat_id=update.effective_chat.id,
                    message_id=prompt_msg_id
                )
        except Exception as e:
            logger.warning(f"Failed to delete config message: {e}")
        
        context.user_data.clear()
        return ConversationHandler.END
        
    except ValueError:
        await update.message.reply_text("❌ 请输入有效的数字：")
        return CONFIG_BIDIRECT_INPUT


# ============================================================================
# 新配置功能的回调处理器
# ============================================================================

async def request_edit_mode_config(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Request edit mode configuration"""
    query = update.callback_query
    await safe_answer_query(query)
    task_id = query.data.split('_')[3]
    context.user_data['config_task_id'] = task_id
    
    task_doc = db[Task.COLLECTION_NAME].find_one({'_id': ObjectId(task_id)})
    task = Task.from_dict(task_doc)
    
    current_mode = getattr(task, 'message_mode', 'normal')
    edit_delay_min = getattr(task, 'edit_delay_min', 5)
    edit_delay_max = getattr(task, 'edit_delay_max', 15)
    
    keyboard = [
        [InlineKeyboardButton("📤 普通模式", callback_data=f'set_mode_normal_{task_id}')],
        [InlineKeyboardButton("✏️ 编辑模式", callback_data=f'set_mode_edit_{task_id}')],
        [InlineKeyboardButton("🔙 返回", callback_data=f'show_config_{task_id}')]
    ]
    
    text = (
        f"✏️ <b>编辑模式配置</b>\n\n"
        f"当前模式: <b>{current_mode}</b>\n"
        f"编辑延迟: {edit_delay_min}-{edit_delay_max}秒\n\n"
        f"💡 编辑模式：先发送初始消息，延迟后编辑成目标内容\n"
        f"⚠️ 可用于绕过某些风控机制\n\n"
        f"请选择消息发送模式："
    )
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='HTML')


async def set_message_mode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set message mode"""
    query = update.callback_query
    await safe_answer_query(query)
    
    parts = query.data.split('_')
    mode = parts[2]  # normal or edit
    task_id = parts[3]
    
    # Map mode to display name
    mode_display = "普通" if mode == "normal" else "编辑"
    
    result = db[Task.COLLECTION_NAME].update_one(
        {'_id': ObjectId(task_id)},
        {'$set': {'message_mode': mode, 'updated_at': datetime.utcnow()}}
    )
    
    if result.modified_count > 0:
        logger.info(f"Task {task_id}: Message mode updated to {mode}")
        await safe_answer_query(query, f"✅ 已设置为{mode_display}模式")
    else:
        await safe_answer_query(query, f"✅ 已设置为{mode_display}模式（值未变更）")
    
    # Redirect back to config menu
    return await show_config_menu_handler(update, context, task_id)


async def request_reply_mode_config(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Request reply mode configuration"""
    query = update.callback_query
    await safe_answer_query(query)
    task_id = query.data.split('_')[3]
    context.user_data['config_task_id'] = task_id
    context.user_data['retry_count'] = 0
    context.user_data['current_config_type'] = 'reply'
    
    task_doc = db[Task.COLLECTION_NAME].find_one({'_id': ObjectId(task_id)})
    task = Task.from_dict(task_doc)
    
    reply_timeout = getattr(task, 'reply_timeout', 300)
    reply_keywords = getattr(task, 'reply_keywords', {})
    reply_default = getattr(task, 'reply_default', '')
    
    # Format existing keywords for display
    keywords_display = "\n".join([f"  • {k} → {v}" for k, v in reply_keywords.items()]) if reply_keywords else "  （无）"
    
    keyboard = [
        [
            InlineKeyboardButton("💡 查看示例", callback_data='cfg_example_reply'),
            InlineKeyboardButton("❌ 取消", callback_data=f'cfg_cancel_{task_id}')
        ],
        [InlineKeyboardButton("🔙 返回", callback_data=f'task_config_{task_id}')]
    ]
    
    prompt_msg = await query.message.reply_text(
        f"💬 <b>回复模式配置</b>\n\n"
        f"当前设置:\n"
        f"• 监听超时: {reply_timeout}秒\n"
        f"• 关键词数量: {len(reply_keywords)}个\n"
        f"• 默认回复: {reply_default or '（无）'}\n\n"
        f"<b>已配置的关键词:</b>\n{keywords_display}\n\n"
        f"💡 <b>配置格式:</b>\n"
        f"关键词1=回复内容1;关键词2=回复内容2;...\n\n"
        f"💡 <b>示例:</b>\n"
        f"你好=你好啊！;价格=请联系我们;帮助=请问有什么可以帮到您?\n\n"
        f"💡 <b>默认回复:</b> 如果用户回复不匹配任何关键词，发送默认回复\n"
        f"输入格式: default=默认回复内容\n\n"
        f"⚠️ 发送 'clear' 可清空所有配置\n"
        f"💬 提示：可以随时点击下方按钮取消或查看示例",
        parse_mode='HTML',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    context.user_data['config_prompt_msg_id'] = prompt_msg.message_id
    return CONFIG_REPLY_MODE_INPUT


async def handle_reply_mode_config(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle reply mode configuration input"""
    task_id = context.user_data.get('config_task_id')
    if not task_id:
        await update.message.reply_text("❌ 配置会话已过期，请重新开始")
        return ConversationHandler.END
    
    user_input = update.message.text.strip()
    
    # Handle cancel
    if user_input == '返回':
        await update.message.reply_text("❌ 已取消配置")
        return ConversationHandler.END
    
    # Handle clear
    if user_input.lower() == 'clear':
        result = db[Task.COLLECTION_NAME].update_one(
            {'_id': ObjectId(task_id)},
            {'$set': {
                'reply_keywords': {},
                'reply_default': '',
                'updated_at': datetime.utcnow()
            }}
        )
        
        if result.modified_count > 0:
            logger.info(f"Task {task_id}: Reply mode config cleared")
        
        msg = await update.message.reply_text("✅ 回复模式配置已清空")
        
        # Auto-cleanup
        await asyncio.sleep(Config.CONFIG_MESSAGE_DELETE_DELAY)
        try:
            await update.message.delete()
            await msg.delete()
            if 'config_prompt_msg_id' in context.user_data:
                await context.bot.delete_message(
                    chat_id=update.effective_chat.id,
                    message_id=context.user_data['config_prompt_msg_id']
                )
        except Exception as e:
            logger.debug(f"Failed to delete config messages: {e}")
        
        return ConversationHandler.END
    
    try:
        # Parse the input
        reply_keywords = {}
        reply_default = None
        
        # Split by semicolon
        pairs = user_input.split(';')
        
        for pair in pairs:
            pair = pair.strip()
            if not pair:
                continue
            
            if '=' not in pair:
                await update.message.reply_text(
                    f"❌ 格式错误：'{pair}' 缺少等号\n"
                    f"正确格式：关键词=回复内容"
                )
                return CONFIG_REPLY_MODE_INPUT
            
            key, value = pair.split('=', 1)
            key = key.strip()
            value = value.strip()
            
            if not key or not value:
                await update.message.reply_text(
                    f"❌ 格式错误：关键词和回复内容不能为空\n"
                    f"错误项：'{pair}'"
                )
                return CONFIG_REPLY_MODE_INPUT
            
            # Check if it's default reply
            if key.lower() == 'default':
                reply_default = value
            else:
                reply_keywords[key] = value
        
        # Update database
        update_dict = {
            'reply_keywords': reply_keywords,
            'updated_at': datetime.utcnow()
        }
        
        if reply_default is not None:
            update_dict['reply_default'] = reply_default
        
        result = db[Task.COLLECTION_NAME].update_one(
            {'_id': ObjectId(task_id)},
            {'$set': update_dict}
        )
        
        # Build success message
        success_msg = f"✅ 回复模式配置成功！\n\n"
        if reply_keywords:
            success_msg += f"📝 配置了 {len(reply_keywords)} 个关键词:\n"
            for k, v in reply_keywords.items():
                success_msg += f"  • {k} → {v}\n"
        if reply_default:
            success_msg += f"\n💬 默认回复: {reply_default}"
        
        if result.modified_count > 0:
            logger.info(f"Task {task_id}: Reply mode configured with {len(reply_keywords)} keywords")
        
        msg = await update.message.reply_text(success_msg)
        
        # Auto-cleanup
        await asyncio.sleep(Config.CONFIG_MESSAGE_DELETE_DELAY)
        try:
            await update.message.delete()
            await msg.delete()
            if 'config_prompt_msg_id' in context.user_data:
                await context.bot.delete_message(
                    chat_id=update.effective_chat.id,
                    message_id=context.user_data['config_prompt_msg_id']
                )
        except Exception as e:
            logger.debug(f"Failed to delete config messages: {e}")
        
        return ConversationHandler.END
        
    except Exception as e:
        logger.error(f"Error parsing reply mode config: {e}")
        await update.message.reply_text(
            f"❌ 配置格式错误，请按照示例格式重新输入\n"
            f"示例: 你好=你好啊！;价格=请联系我们"
        )
        return CONFIG_REPLY_MODE_INPUT



async def request_batch_pause_config(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Request batch pause configuration"""
    query = update.callback_query
    await safe_answer_query(query)
    task_id = query.data.split('_')[3]
    context.user_data['config_task_id'] = task_id
    
    task_doc = db[Task.COLLECTION_NAME].find_one({'_id': ObjectId(task_id)})
    task = Task.from_dict(task_doc)
    
    batch_pause_count = getattr(task, 'batch_pause_count', 0)
    batch_pause_min = getattr(task, 'batch_pause_min', 0)
    batch_pause_max = getattr(task, 'batch_pause_max', 5)
    
    keyboard = [
        [InlineKeyboardButton(f"📊 每{batch_pause_count}条停顿", callback_data=f'set_batch_count_{task_id}')],
        [InlineKeyboardButton(f"⏱️ 停顿{batch_pause_min}-{batch_pause_max}秒", callback_data=f'set_batch_delay_{task_id}')],
        [InlineKeyboardButton("❌ 禁用批次停顿", callback_data=f'disable_batch_pause_{task_id}')],
        [InlineKeyboardButton("🔙 返回", callback_data=f'show_config_{task_id}')]
    ]
    
    text = (
        f"⏸️ <b>批次停顿配置</b>\n\n"
        f"当前设置:\n"
        f"• 每 <b>{batch_pause_count}</b> 条消息停顿\n"
        f"• 停顿 <b>{batch_pause_min}-{batch_pause_max}</b> 秒\n\n"
        f"💡 防封策略：定期停顿可降低被检测风险\n"
        f"⚠️ 设置为0表示禁用批次停顿\n\n"
        f"请选择要配置的选项："
    )
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='HTML')


async def disable_batch_pause(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Disable batch pause"""
    query = update.callback_query
    await safe_answer_query(query)
    task_id = query.data.split('_')[3]
    
    db[Task.COLLECTION_NAME].update_one(
        {'_id': ObjectId(task_id)},
        {'$set': {'batch_pause_count': 0, 'updated_at': datetime.utcnow()}}
    )
    
    await safe_answer_query(query, "✅ 已禁用批次停顿")
    return await show_config_menu_handler(update, context, task_id)


async def request_batch_count_config(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Request batch count configuration"""
    query = update.callback_query
    await safe_answer_query(query)
    task_id = query.data.split('_')[3]
    context.user_data['config_task_id'] = task_id
    
    task_doc = db[Task.COLLECTION_NAME].find_one({'_id': ObjectId(task_id)})
    task = Task.from_dict(task_doc)
    batch_pause_count = getattr(task, 'batch_pause_count', 0)
    
    text = (
        f"📊 <b>批次停顿条数配置</b>\n\n"
        f"当前设置: 每 <b>{batch_pause_count}</b> 条消息停顿\n\n"
        f"💡 建议范围: 10-50 条\n"
        f"⚠️ 设置为 0 表示禁用批次停顿\n\n"
        f"请输入批次停顿条数（如：20）："
    )
    
    prompt_msg = await query.edit_message_text(text, parse_mode='HTML')
    context.user_data['config_prompt_msg_id'] = prompt_msg.message_id
    return CONFIG_BATCH_COUNT_INPUT


async def handle_batch_count_config(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle batch count configuration input"""
    try:
        task_id = context.user_data.get('config_task_id')
        if not task_id:
            await update.message.reply_text("❌ 配置会话已过期，请重新开始")
            return ConversationHandler.END
        
        # Parse input
        batch_count = int(update.message.text.strip())
        
        if batch_count < 0:
            await update.message.reply_text("❌ 批次停顿条数不能为负数，请重新输入")
            return CONFIG_BATCH_COUNT_INPUT
        
        # Update database
        result = db[Task.COLLECTION_NAME].update_one(
            {'_id': ObjectId(task_id)},
            {'$set': {'batch_pause_count': batch_count, 'updated_at': datetime.utcnow()}}
        )
        
        if result.modified_count > 0:
            logger.info(f"Task {task_id}: Batch pause count updated to {batch_count}")
        
        msg = await update.message.reply_text(f"✅ 批次停顿条数已设置为: {batch_count}")
        
        # Auto-cleanup
        await asyncio.sleep(Config.CONFIG_MESSAGE_DELETE_DELAY)
        try:
            await update.message.delete()
            await msg.delete()
            if 'config_prompt_msg_id' in context.user_data:
                await context.bot.delete_message(
                    chat_id=update.effective_chat.id,
                    message_id=context.user_data['config_prompt_msg_id']
                )
        except Exception as e:
            logger.debug(f"Failed to delete config messages: {e}")
        
        return ConversationHandler.END
        
    except ValueError:
        await update.message.reply_text("❌ 格式错误，请输入有效的整数")
        return CONFIG_BATCH_COUNT_INPUT
    except Exception as e:
        logger.error(f"Error handling batch count config: {e}")
        await update.message.reply_text(f"❌ 配置失败: {str(e)}")
        return ConversationHandler.END


async def request_batch_delay_config(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Request batch delay configuration"""
    query = update.callback_query
    await safe_answer_query(query)
    task_id = query.data.split('_')[3]
    context.user_data['config_task_id'] = task_id
    
    task_doc = db[Task.COLLECTION_NAME].find_one({'_id': ObjectId(task_id)})
    task = Task.from_dict(task_doc)
    batch_pause_min = getattr(task, 'batch_pause_min', 0)
    batch_pause_max = getattr(task, 'batch_pause_max', 5)
    
    text = (
        f"⏱️ <b>批次停顿时长配置</b>\n\n"
        f"当前设置: 停顿 <b>{batch_pause_min}-{batch_pause_max}</b> 秒\n\n"
        f"💡 建议范围: 30-300 秒\n"
        f"📝 系统会在此范围内随机选择停顿时长\n\n"
        f"请输入停顿时长范围（格式：最小值-最大值，如：30-60）："
    )
    
    prompt_msg = await query.edit_message_text(text, parse_mode='HTML')
    context.user_data['config_prompt_msg_id'] = prompt_msg.message_id
    return CONFIG_BATCH_DELAY_INPUT


async def handle_batch_delay_config(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle batch delay configuration input"""
    try:
        task_id = context.user_data.get('config_task_id')
        if not task_id:
            await update.message.reply_text("❌ 配置会话已过期，请重新开始")
            return ConversationHandler.END
        
        # Parse input (format: min-max)
        text = update.message.text.strip()
        if '-' not in text:
            await update.message.reply_text(
                "❌ 格式错误\n"
                "正确格式：最小值-最大值（如：30-60）"
            )
            return CONFIG_BATCH_DELAY_INPUT
        
        parts = text.split('-')
        if len(parts) != 2:
            await update.message.reply_text(
                "❌ 格式错误\n"
                "正确格式：最小值-最大值（如：30-60）"
            )
            return CONFIG_BATCH_DELAY_INPUT
        
        min_delay = int(parts[0].strip())
        max_delay = int(parts[1].strip())
        
        if min_delay < 0 or max_delay < 0:
            await update.message.reply_text("❌ 停顿时长不能为负数，请重新输入")
            return CONFIG_BATCH_DELAY_INPUT
        
        if min_delay > max_delay:
            await update.message.reply_text("❌ 最小值不能大于最大值，请重新输入")
            return CONFIG_BATCH_DELAY_INPUT
        
        # Update database
        result = db[Task.COLLECTION_NAME].update_one(
            {'_id': ObjectId(task_id)},
            {'$set': {
                'batch_pause_min': min_delay,
                'batch_pause_max': max_delay,
                'updated_at': datetime.utcnow()
            }}
        )
        
        if result.modified_count > 0:
            logger.info(f"Task {task_id}: Batch pause delay updated to {min_delay}-{max_delay}")
        
        msg = await update.message.reply_text(f"✅ 批次停顿时长已设置为: {min_delay}-{max_delay} 秒")
        
        # Auto-cleanup
        await asyncio.sleep(Config.CONFIG_MESSAGE_DELETE_DELAY)
        try:
            await update.message.delete()
            await msg.delete()
            if 'config_prompt_msg_id' in context.user_data:
                await context.bot.delete_message(
                    chat_id=update.effective_chat.id,
                    message_id=context.user_data['config_prompt_msg_id']
                )
        except Exception as e:
            logger.debug(f"Failed to delete config messages: {e}")
        
        return ConversationHandler.END
        
    except ValueError:
        await update.message.reply_text("❌ 格式错误，请输入有效的整数")
        return CONFIG_BATCH_DELAY_INPUT
    except Exception as e:
        logger.error(f"Error handling batch delay config: {e}")
        await update.message.reply_text(f"❌ 配置失败: {str(e)}")
        return ConversationHandler.END


async def request_flood_strategy_config(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Request FloodWait strategy configuration"""
    query = update.callback_query
    await safe_answer_query(query)
    task_id = query.data.split('_')[3]
    
    task_doc = db[Task.COLLECTION_NAME].find_one({'_id': ObjectId(task_id)})
    task = Task.from_dict(task_doc)
    
    current_strategy = getattr(task, 'flood_wait_strategy', 'switch_account')
    
    keyboard = [
        [InlineKeyboardButton("🔄 切换账号 (推荐)", callback_data=f'set_flood_switch_{task_id}')],
        [InlineKeyboardButton("⏳ 继续等待", callback_data=f'set_flood_wait_{task_id}')],
        [InlineKeyboardButton("⛔ 停止任务", callback_data=f'set_flood_stop_{task_id}')],
        [InlineKeyboardButton("🔙 返回", callback_data=f'show_config_{task_id}')]
    ]
    
    text = (
        f"🌊 <b>FloodWait策略配置</b>\n\n"
        f"当前策略: <b>{FLOOD_STRATEGY_DISPLAY.get(current_strategy, current_strategy)}</b>\n\n"
        f"💡 <b>什么是FloodWait？</b>\n"
        f"当Telegram检测到账号发送消息过于频繁时，会返回FloodWait错误，要求等待一段时间。\n\n"
        f"<b>策略说明：</b>\n\n"
        f"🔄 <b>切换账号（推荐）</b>\n"
        f"  ├─ 遇到FloodWait立即切换到下一个账号\n"
        f"  ├─ 最大化发送效率\n"
        f"  └─ 适合多账号场景\n\n"
        f"⏳ <b>继续等待</b>\n"
        f"  ├─ 等待Telegram指定的时间后继续\n"
        f"  ├─ 保持使用当前账号\n"
        f"  └─ 适合单账号或等待时间较短的情况\n\n"
        f"⛔ <b>停止任务</b>\n"
        f"  ├─ 遇到FloodWait立即停止整个任务\n"
        f"  ├─ 最保守的策略\n"
        f"  └─ 适合需要人工介入的场景\n\n"
        f"请选择FloodWait处理策略："
    )
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='HTML')


async def set_flood_strategy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set FloodWait strategy"""
    query = update.callback_query
    await safe_answer_query(query)
    
    parts = query.data.split('_')
    strategy_type = parts[2]  # switch, wait, or stop
    task_id = parts[3]
    
    strategy = FLOOD_STRATEGY_SHORT_TO_FULL.get(strategy_type, 'switch_account')
    
    result = db[Task.COLLECTION_NAME].update_one(
        {'_id': ObjectId(task_id)},
        {'$set': {'flood_wait_strategy': strategy, 'updated_at': datetime.utcnow()}}
    )
    
    if result.modified_count > 0:
        logger.info(f"Task {task_id}: FloodWait strategy updated to {strategy}")
        await safe_answer_query(query, f"✅ FloodWait策略已设置为：{FLOOD_STRATEGY_DISPLAY_SHORT.get(strategy_type, strategy)}")
    else:
        await safe_answer_query(query, f"✅ FloodWait策略已设置为：{FLOOD_STRATEGY_DISPLAY_SHORT.get(strategy_type, strategy)}（值未变更）")
    
    return await show_config_menu_handler(update, context, task_id)


async def request_voice_call_config(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Request voice call configuration"""
    query = update.callback_query
    await safe_answer_query(query)
    task_id = query.data.split('_')[3]
    
    task_doc = db[Task.COLLECTION_NAME].find_one({'_id': ObjectId(task_id)})
    task = Task.from_dict(task_doc)
    
    voice_enabled = getattr(task, 'voice_call_enabled', False)
    voice_duration = getattr(task, 'voice_call_duration', 10)
    voice_wait = getattr(task, 'voice_call_wait_after', 3)
    voice_send_if_failed = getattr(task, 'voice_call_send_if_failed', True)
    
    keyboard = [
        [InlineKeyboardButton("❌ 禁用", callback_data=f'set_voice_disabled_{task_id}')],
        [InlineKeyboardButton("📞 失败继续发", callback_data=f'set_voice_continue_{task_id}')],
        [InlineKeyboardButton("📞 失败不发", callback_data=f'set_voice_strict_{task_id}')],
        [InlineKeyboardButton("🔙 返回", callback_data=f'show_config_{task_id}')]
    ]
    
    # Determine current mode
    if not voice_enabled:
        current_mode = "❌ 禁用"
    elif voice_send_if_failed:
        current_mode = "📞 失败继续发"
    else:
        current_mode = "📞 失败不发"
    
    text = (
        f"📞 <b>语音拨打配置</b>\n\n"
        f"⚠️ <b>功能状态：开发中</b>\n"
        f"此功能需要额外依赖库，暂不可用\n\n"
        f"当前模式: <b>{current_mode}</b>\n"
        f"拨打时长: {voice_duration}秒\n"
        f"拨打后等待: {voice_wait}秒\n\n"
        f"<b>模式说明：</b>\n\n"
        f"❌ <b>禁用</b>\n"
        f"  └─ 不拨打语音电话，直接发送消息\n\n"
        f"📞 <b>失败继续发（开发中）</b>\n"
        f"  ├─ 发送消息前先拨打语音电话\n"
        f"  ├─ 如果拨打失败，仍然发送消息\n"
        f"  └─ 兼顾互动率和送达率\n\n"
        f"📞 <b>失败不发（开发中）</b>\n"
        f"  ├─ 发送消息前先拨打语音电话\n"
        f"  ├─ 如果拨打失败，跳过该用户\n"
        f"  └─ 仅对能接通的用户发送\n\n"
        f"💡 注意: Telethon库不支持client.call()方法\n"
        f"🔧 建议: 使用禁用模式，语音功能待后续开发\n\n"
        f"请选择语音拨打模式："
    )
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='HTML')


async def set_voice_call_mode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set voice call mode"""
    query = update.callback_query
    await safe_answer_query(query)
    
    parts = query.data.split('_')
    mode = parts[2]  # disabled, continue, or strict
    task_id = parts[3]
    
    # Configure based on mode
    if mode == 'disabled':
        voice_enabled = False
        voice_send_if_failed = True
        mode_display = "禁用"
    elif mode == 'continue':
        voice_enabled = True
        voice_send_if_failed = True
        mode_display = "失败继续发"
    elif mode == 'strict':
        voice_enabled = True
        voice_send_if_failed = False
        mode_display = "失败不发"
    else:
        voice_enabled = False
        voice_send_if_failed = True
        mode_display = "禁用"
    
    result = db[Task.COLLECTION_NAME].update_one(
        {'_id': ObjectId(task_id)},
        {'$set': {
            'voice_call_enabled': voice_enabled,
            'voice_call_send_if_failed': voice_send_if_failed,
            'updated_at': datetime.utcnow()
        }}
    )
    
    if result.modified_count > 0:
        logger.info(f"Task {task_id}: Voice call mode set to {mode}")
        await safe_answer_query(query, f"✅ 语音拨打模式已设置为：{mode_display}")
    else:
        await safe_answer_query(query, f"✅ 语音拨打模式已设置为：{mode_display}（值未变更）")
    
    return await request_voice_call_config(update, context)


async def toggle_voice_call(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Toggle voice call enabled (deprecated - use set_voice_call_mode instead)"""
    query = update.callback_query
    await safe_answer_query(query)
    task_id = query.data.split('_')[2]
    
    task_doc = db[Task.COLLECTION_NAME].find_one({'_id': ObjectId(task_id)})
    task = Task.from_dict(task_doc)
    
    new_value = not getattr(task, 'voice_call_enabled', False)
    
    result = db[Task.COLLECTION_NAME].update_one(
        {'_id': ObjectId(task_id)},
        {'$set': {'voice_call_enabled': new_value, 'updated_at': datetime.utcnow()}}
    )
    
    if result.modified_count > 0:
        logger.info(f"Task {task_id}: Voice call {'enabled' if new_value else 'disabled'}")
    
    await safe_answer_query(query, f"✅ 语音拨打已{'启用' if new_value else '禁用'}")
    return await request_voice_call_config(update, context)


async def toggle_dead_account_switch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Toggle auto switch dead account"""
    query = update.callback_query
    await safe_answer_query(query)
    # Callback data format: cfg_toggle_dead_account_{task_id}
    # Extract task_id from the last part
    task_id = query.data.split('_')[-1]
    
    task_doc = db[Task.COLLECTION_NAME].find_one({'_id': ObjectId(task_id)})
    task = Task.from_dict(task_doc)
    
    new_value = not getattr(task, 'auto_switch_dead_account', True)
    
    result = db[Task.COLLECTION_NAME].update_one(
        {'_id': ObjectId(task_id)},
        {'$set': {'auto_switch_dead_account': new_value, 'updated_at': datetime.utcnow()}}
    )
    
    if result.modified_count > 0:
        logger.info(f"Task {task_id}: Auto switch dead account {'enabled' if new_value else 'disabled'}")
    
    await safe_answer_query(query, f"✅ 死号自动换号已{'启用' if new_value else '禁用'}")
    return await show_config_menu_handler(update, context, task_id)


async def request_thread_interval_config(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Request thread start interval configuration"""
    query = update.callback_query
    await safe_answer_query(query)
    task_id = query.data.split('_')[3]
    context.user_data['config_task_id'] = task_id
    context.user_data['retry_count'] = 0
    context.user_data['current_config_type'] = 'threadinterval'
    
    keyboard = [
        [
            InlineKeyboardButton("💡 查看示例", callback_data='cfg_example_threadinterval'),
            InlineKeyboardButton("❌ 取消", callback_data=f'cfg_cancel_{task_id}')
        ],
        [InlineKeyboardButton("🔙 返回", callback_data=f'task_config_{task_id}')]
    ]
    
    prompt_msg = await query.message.reply_text(
        "⏲️ <b>配置线程启动间隔</b>\n\n"
        "请输入线程启动间隔（秒）：\n\n"
        "💡 建议：0-5秒\n"
        "⚠️ 间隔可以避免瞬间并发过高\n\n"
        "💬 提示：可以随时点击下方按钮取消或查看示例",
        parse_mode='HTML',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    context.user_data['config_prompt_msg_id'] = prompt_msg.message_id
    return CONFIG_THREAD_INTERVAL_INPUT


async def request_daily_limit_config(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Request daily limit configuration"""
    query = update.callback_query
    await safe_answer_query(query)
    task_id = query.data.split('_')[3]
    context.user_data['config_task_id'] = task_id
    context.user_data['retry_count'] = 0
    context.user_data['current_config_type'] = 'daily'
    
    keyboard = [
        [
            InlineKeyboardButton("💡 查看示例", callback_data='cfg_example_daily'),
            InlineKeyboardButton("❌ 取消", callback_data=f'cfg_cancel_{task_id}')
        ],
        [InlineKeyboardButton("🔙 返回", callback_data=f'task_config_{task_id}')]
    ]
    
    prompt_msg = await query.message.reply_text(
        "📊 <b>配置单账号日限</b>\n\n"
        "请输入每个账号每天最多发送的消息数量：\n\n"
        "💡 建议范围：1-200条\n"
        "💡 默认值：50条\n"
        "⚠️ 设置过高可能导致封号风险增加\n\n"
        "💬 提示：可以随时点击下方按钮取消或查看示例",
        parse_mode='HTML',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    context.user_data['config_prompt_msg_id'] = prompt_msg.message_id
    return CONFIG_DAILY_LIMIT_INPUT


async def handle_daily_limit_config(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle daily limit configuration input"""
    task_id = context.user_data.get('config_task_id')
    if not task_id:
        await update.message.reply_text("❌ 配置会话已过期，请重新开始")
        return ConversationHandler.END
    
    try:
        daily_limit = int(update.message.text.strip())
        
        if daily_limit < 1 or daily_limit > 200:
            retry_count = context.user_data.get('retry_count', 0) + 1
            context.user_data['retry_count'] = retry_count
            
            if retry_count >= 3:
                msg = await update.message.reply_text(
                    "❌ <b>输入错误次数过多</b>\n\n"
                    "已自动取消配置，请重新开始",
                    parse_mode='HTML'
                )
                await asyncio.sleep(2)
                try:
                    await msg.delete()
                    await update.message.delete()
                    if 'config_prompt_msg_id' in context.user_data:
                        await context.bot.delete_message(
                            chat_id=update.effective_chat.id,
                            message_id=context.user_data['config_prompt_msg_id']
                        )
                except Exception:
                    pass
                context.user_data.clear()
                return ConversationHandler.END
            
            await update.message.reply_text(
                f"❌ <b>格式错误（第{retry_count}次）</b>\n\n"
                f"日限必须在 1-200 之间\n"
                f"还剩 {3 - retry_count} 次尝试机会",
                parse_mode='HTML'
            )
            return CONFIG_DAILY_LIMIT_INPUT
        
        # Update database
        result = db[Task.COLLECTION_NAME].update_one(
            {'_id': ObjectId(task_id)},
            {'$set': {'daily_limit': daily_limit, 'updated_at': datetime.utcnow()}}
        )
        
        # Verify update
        if result.modified_count > 0:
            logger.info(f"Task {task_id}: Daily limit updated to {daily_limit}")
            msg = await update.message.reply_text(f"✅ 单账号日限已设置为：{daily_limit}条")
        else:
            msg = await update.message.reply_text(f"✅ 单账号日限已设置为：{daily_limit}条（值未变更）")
        
        # Auto-cleanup
        await asyncio.sleep(Config.CONFIG_MESSAGE_DELETE_DELAY)
        try:
            await update.message.delete()
            await msg.delete()
            if 'config_prompt_msg_id' in context.user_data:
                await context.bot.delete_message(
                    chat_id=update.effective_chat.id,
                    message_id=context.user_data['config_prompt_msg_id']
                )
        except Exception as e:
            logger.debug(f"Failed to delete config messages: {e}")
        
        return ConversationHandler.END
        
    except ValueError:
        retry_count = context.user_data.get('retry_count', 0) + 1
        context.user_data['retry_count'] = retry_count
        
        if retry_count >= 3:
            msg = await update.message.reply_text(
                "❌ <b>输入错误次数过多</b>\n\n"
                "已自动取消配置，请重新开始",
                parse_mode='HTML'
            )
            await asyncio.sleep(2)
            try:
                await msg.delete()
                await update.message.delete()
                if 'config_prompt_msg_id' in context.user_data:
                    await context.bot.delete_message(
                        chat_id=update.effective_chat.id,
                        message_id=context.user_data['config_prompt_msg_id']
                    )
            except Exception:
                pass
            context.user_data.clear()
            return ConversationHandler.END
        
        await update.message.reply_text(
            f"❌ <b>格式错误（第{retry_count}次）</b>\n\n"
            f"请输入有效的数字（1-200）\n"
            f"还剩 {3 - retry_count} 次尝试机会",
            parse_mode='HTML'
        )
        return CONFIG_DAILY_LIMIT_INPUT


async def request_retry_config(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Request retry configuration"""
    query = update.callback_query
    await safe_answer_query(query)
    task_id = query.data.split('_')[2]
    context.user_data['config_task_id'] = task_id
    context.user_data['retry_count'] = 0
    context.user_data['current_config_type'] = 'retry'
    
    task_doc = db[Task.COLLECTION_NAME].find_one({'_id': ObjectId(task_id)})
    task = Task.from_dict(task_doc)
    
    keyboard = [
        [
            InlineKeyboardButton("💡 查看示例", callback_data='cfg_example_retry'),
            InlineKeyboardButton("❌ 取消", callback_data=f'cfg_cancel_{task_id}')
        ],
        [InlineKeyboardButton("🔙 返回", callback_data=f'task_config_{task_id}')]
    ]
    
    prompt_msg = await query.message.reply_text(
        "🔄 <b>配置重试策略</b>\n\n"
        f"当前设置: {task.retry_count}次，间隔{task.retry_interval}秒\n\n"
        "请输入重试次数和间隔时间（秒），用空格分隔：\n\n"
        "💡 格式：重试次数 间隔时间\n"
        "💡 例如：3 60（重试3次，每次间隔60秒）\n"
        "💡 建议：1-10次，间隔30-300秒\n"
        "⚠️ 重试过于频繁可能被检测为异常行为\n\n"
        "💬 提示：可以随时点击下方按钮取消或查看示例",
        parse_mode='HTML',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    context.user_data['config_prompt_msg_id'] = prompt_msg.message_id
    return CONFIG_RETRY_INPUT


async def handle_retry_config(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle retry configuration input"""
    task_id = context.user_data.get('config_task_id')
    if not task_id:
        await update.message.reply_text("❌ 配置会话已过期，请重新开始")
        return ConversationHandler.END
    
    try:
        parts = update.message.text.strip().split()
        if len(parts) != 2:
            retry_count = context.user_data.get('retry_count', 0) + 1
            context.user_data['retry_count'] = retry_count
            
            if retry_count >= 3:
                msg = await update.message.reply_text(
                    "❌ <b>输入错误次数过多</b>\n\n"
                    "已自动取消配置，请重新开始",
                    parse_mode='HTML'
                )
                await asyncio.sleep(2)
                try:
                    await msg.delete()
                    await update.message.delete()
                    if 'config_prompt_msg_id' in context.user_data:
                        await context.bot.delete_message(
                            chat_id=update.effective_chat.id,
                            message_id=context.user_data['config_prompt_msg_id']
                        )
                except Exception:
                    pass
                context.user_data.clear()
                return ConversationHandler.END
            
            await update.message.reply_text(
                f"❌ <b>格式错误（第{retry_count}次）</b>\n\n"
                f"请输入两个数字（重试次数 间隔时间）\n"
                f"还剩 {3 - retry_count} 次尝试机会",
                parse_mode='HTML'
            )
            return CONFIG_RETRY_INPUT
        
        retry_count_val = int(parts[0])
        retry_interval = int(parts[1])
        
        if retry_count_val < 0 or retry_count_val > 10:
            retry_count = context.user_data.get('retry_count', 0) + 1
            context.user_data['retry_count'] = retry_count
            
            if retry_count >= 3:
                msg = await update.message.reply_text(
                    "❌ <b>输入错误次数过多</b>\n\n"
                    "已自动取消配置，请重新开始",
                    parse_mode='HTML'
                )
                await asyncio.sleep(2)
                try:
                    await msg.delete()
                    await update.message.delete()
                    if 'config_prompt_msg_id' in context.user_data:
                        await context.bot.delete_message(
                            chat_id=update.effective_chat.id,
                            message_id=context.user_data['config_prompt_msg_id']
                        )
                except Exception:
                    pass
                context.user_data.clear()
                return ConversationHandler.END
            
            await update.message.reply_text(
                f"❌ <b>格式错误（第{retry_count}次）</b>\n\n"
                f"重试次数必须在 0-10 之间\n"
                f"还剩 {3 - retry_count} 次尝试机会",
                parse_mode='HTML'
            )
            return CONFIG_RETRY_INPUT
        
        if retry_interval < 10 or retry_interval > 300:
            retry_count = context.user_data.get('retry_count', 0) + 1
            context.user_data['retry_count'] = retry_count
            
            if retry_count >= 3:
                msg = await update.message.reply_text(
                    "❌ <b>输入错误次数过多</b>\n\n"
                    "已自动取消配置，请重新开始",
                    parse_mode='HTML'
                )
                await asyncio.sleep(2)
                try:
                    await msg.delete()
                    await update.message.delete()
                    if 'config_prompt_msg_id' in context.user_data:
                        await context.bot.delete_message(
                            chat_id=update.effective_chat.id,
                            message_id=context.user_data['config_prompt_msg_id']
                        )
                except Exception:
                    pass
                context.user_data.clear()
                return ConversationHandler.END
            
            await update.message.reply_text(
                f"❌ <b>格式错误（第{retry_count}次）</b>\n\n"
                f"间隔时间必须在 10-300秒 之间\n"
                f"还剩 {3 - retry_count} 次尝试机会",
                parse_mode='HTML'
            )
            return CONFIG_RETRY_INPUT
        
        # Update database
        result = db[Task.COLLECTION_NAME].update_one(
            {'_id': ObjectId(task_id)},
            {'$set': {
                'retry_count': retry_count_val,
                'retry_interval': retry_interval,
                'updated_at': datetime.utcnow()
            }}
        )
        
        # Verify update
        if result.modified_count > 0:
            logger.info(f"Task {task_id}: Retry config updated to {retry_count_val} times, {retry_interval}s interval")
            msg = await update.message.reply_text(
                f"✅ 重试策略已设置为：{retry_count_val}次，间隔{retry_interval}秒"
            )
        else:
            msg = await update.message.reply_text(
                f"✅ 重试策略已设置为：{retry_count_val}次，间隔{retry_interval}秒（值未变更）"
            )
        
        # Auto-cleanup
        await asyncio.sleep(Config.CONFIG_MESSAGE_DELETE_DELAY)
        try:
            await update.message.delete()
            await msg.delete()
            if 'config_prompt_msg_id' in context.user_data:
                await context.bot.delete_message(
                    chat_id=update.effective_chat.id,
                    message_id=context.user_data['config_prompt_msg_id']
                )
        except Exception as e:
            logger.debug(f"Failed to delete config messages: {e}")
        
        return ConversationHandler.END
        
    except ValueError:
        retry_count = context.user_data.get('retry_count', 0) + 1
        context.user_data['retry_count'] = retry_count
        
        if retry_count >= 3:
            msg = await update.message.reply_text(
                "❌ <b>输入错误次数过多</b>\n\n"
                "已自动取消配置，请重新开始",
                parse_mode='HTML'
            )
            await asyncio.sleep(2)
            try:
                await msg.delete()
                await update.message.delete()
                if 'config_prompt_msg_id' in context.user_data:
                    await context.bot.delete_message(
                        chat_id=update.effective_chat.id,
                        message_id=context.user_data['config_prompt_msg_id']
                    )
            except Exception:
                pass
            context.user_data.clear()
            return ConversationHandler.END
        
        await update.message.reply_text(
            f"❌ <b>格式错误（第{retry_count}次）</b>\n\n"
            f"请输入有效的数字\n"
            f"还剩 {3 - retry_count} 次尝试机会",
            parse_mode='HTML'
        )
        return CONFIG_RETRY_INPUT


async def toggle_force_private_mode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Toggle force private mode"""
    query = update.callback_query
    await safe_answer_query(query)
    # Callback data format: cfg_toggle_force_private_{task_id}
    # Extract task_id from the last part
    task_id = query.data.split('_')[-1]
    
    task_doc = db[Task.COLLECTION_NAME].find_one({'_id': ObjectId(task_id)})
    task = Task.from_dict(task_doc)
    
    new_value = not getattr(task, 'force_private_mode', False)
    
    result = db[Task.COLLECTION_NAME].update_one(
        {'_id': ObjectId(task_id)},
        {'$set': {'force_private_mode': new_value, 'updated_at': datetime.utcnow()}}
    )
    
    if result.modified_count > 0:
        logger.info(f"Task {task_id}: Force private mode {'enabled' if new_value else 'disabled'}")
    
    await safe_answer_query(query, f"✅ 强制私信模式已{'启用' if new_value else '禁用'}")
    return await show_config_menu_handler(update, context, task_id)


async def handle_thread_interval_config(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle thread interval configuration input"""
    task_id = context.user_data.get('config_task_id')
    if not task_id:
        await update.message.reply_text("❌ 配置会话已过期，请重新开始")
        return ConversationHandler.END
    
    try:
        interval = int(update.message.text.strip())
        
        if interval < 0 or interval > 60:
            retry_count = context.user_data.get('retry_count', 0) + 1
            context.user_data['retry_count'] = retry_count
            
            if retry_count >= 3:
                msg = await update.message.reply_text(
                    "❌ <b>输入错误次数过多</b>\n\n"
                    "已自动取消配置，请重新开始",
                    parse_mode='HTML'
                )
                await asyncio.sleep(2)
                try:
                    await msg.delete()
                    await update.message.delete()
                    if 'config_prompt_msg_id' in context.user_data:
                        await context.bot.delete_message(
                            chat_id=update.effective_chat.id,
                            message_id=context.user_data['config_prompt_msg_id']
                        )
                except Exception:
                    pass
                context.user_data.clear()
                return ConversationHandler.END
            
            await update.message.reply_text(
                f"❌ <b>格式错误（第{retry_count}次）</b>\n\n"
                f"间隔时间必须在 0-60秒 之间\n"
                f"还剩 {3 - retry_count} 次尝试机会",
                parse_mode='HTML'
            )
            return CONFIG_THREAD_INTERVAL_INPUT
        
        # Update database
        result = db[Task.COLLECTION_NAME].update_one(
            {'_id': ObjectId(task_id)},
            {'$set': {'thread_start_interval': interval, 'updated_at': datetime.utcnow()}}
        )
        
        # Verify update
        if result.modified_count > 0:
            logger.info(f"Task {task_id}: Thread start interval updated to {interval}s")
            msg = await update.message.reply_text(f"✅ 线程启动间隔已设置为：{interval}秒")
        else:
            msg = await update.message.reply_text(f"✅ 线程启动间隔已设置为：{interval}秒（值未变更）")
        
        # Auto-cleanup
        await asyncio.sleep(Config.CONFIG_MESSAGE_DELETE_DELAY)
        try:
            await update.message.delete()
            await msg.delete()
            if 'config_prompt_msg_id' in context.user_data:
                await context.bot.delete_message(
                    chat_id=update.effective_chat.id,
                    message_id=context.user_data['config_prompt_msg_id']
                )
        except Exception as e:
            logger.debug(f"Failed to delete config messages: {e}")
        
        return ConversationHandler.END
        
    except ValueError:
        retry_count = context.user_data.get('retry_count', 0) + 1
        context.user_data['retry_count'] = retry_count
        
        if retry_count >= 3:
            msg = await update.message.reply_text(
                "❌ <b>输入错误次数过多</b>\n\n"
                "已自动取消配置，请重新开始",
                parse_mode='HTML'
            )
            await asyncio.sleep(2)
            try:
                await msg.delete()
                await update.message.delete()
                if 'config_prompt_msg_id' in context.user_data:
                    await context.bot.delete_message(
                        chat_id=update.effective_chat.id,
                        message_id=context.user_data['config_prompt_msg_id']
                    )
            except Exception:
                pass
            context.user_data.clear()
            return ConversationHandler.END
        
        await update.message.reply_text(
            f"❌ <b>格式错误（第{retry_count}次）</b>\n\n"
            f"请输入有效的数字（0-60）\n"
            f"还剩 {3 - retry_count} 次尝试机会",
            parse_mode='HTML'
        )
        return CONFIG_THREAD_INTERVAL_INPUT



async def handle_config_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理配置取消 - 统一处理器"""
    query = update.callback_query
    await safe_answer_query(query, "✅ 已取消配置")
    
    # 清理临时消息
    try:
        prompt_msg_id = context.user_data.get('config_prompt_msg_id')
        if prompt_msg_id:
            await context.bot.delete_message(update.effective_chat.id, prompt_msg_id)
    except Exception as e:
        logger.warning(f"Failed to delete prompt message: {e}")
    
    # 清理用户数据
    task_id = context.user_data.get('config_task_id')
    context.user_data.clear()
    
    # 返回任务配置界面
    if task_id:
        await show_task_config(query, task_id)
    else:
        await show_tasks_menu(query)
    
    return ConversationHandler.END


async def show_config_example(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """显示配置示例"""
    query = update.callback_query
    config_type = query.data.split('_')[2]
    
    examples = {
        'edit': (
            "✏️ <b>编辑模式配置示例</b>\n\n"
            "格式：延迟最小 延迟最大 | 编辑内容\n\n"
            "<b>示例1：</b> 5 15 | 🎉 限时优惠！\n"
            "→ 5-15秒后编辑为优惠信息\n\n"
            "<b>示例2：</b> 3 10 | 点击链接：http://xxx.com\n"
            "→ 3-10秒后编辑为链接\n\n"
            "<b>示例3：</b> 10 20 | 联系客服获取更多信息\n"
            "→ 10-20秒后编辑为联系方式"
        ),
        'reply': (
            "💬 <b>回复模式配置示例</b>\n\n"
            "格式：关键词1=回复1;关键词2=回复2\n\n"
            "<b>示例1：</b>\n"
            "价格=我们的价格是199元;联系=请加微信abc123\n\n"
            "<b>示例2：</b>\n"
            "多少钱=试听免费，正式课199元;怎么报名=请加QQ群123456\n\n"
            "<b>示例3：</b>\n"
            "在哪=我们在北京;电话=联系电话：13800138000;default=感谢回复！"
        ),
        'batch': (
            "🔄 <b>分批停顿配置示例</b>\n\n"
            "格式：条数 最小秒 最大秒\n\n"
            "<b>示例1：</b> 3 5 10\n"
            "→ 每发送3条消息，停顿5-10秒\n\n"
            "<b>示例2：</b> 5 10 20\n"
            "→ 每发送5条消息，停顿10-20秒\n\n"
            "<b>示例3：</b> 10 30 60\n"
            "→ 每发送10条消息，停顿30-60秒"
        ),
        'voice': (
            "📞 <b>语音拨打配置示例</b>\n\n"
            "格式：持续时间 等待时间 失败继续\n\n"
            "<b>示例1：</b> 10 3 yes\n"
            "→ 拨打10秒，等待3秒，失败继续发消息\n\n"
            "<b>示例2：</b> 15 5 no\n"
            "→ 拨打15秒，等待5秒，失败跳过\n\n"
            "<b>示例3：</b> 5 2 yes\n"
            "→ 拨打5秒，等待2秒，失败继续发消息"
        ),
        'bidirect': (
            "🔄 <b>双向重试配置示例</b>\n\n"
            "格式：重试次数 间隔秒数\n\n"
            "<b>示例1：</b> 15 5\n"
            "→ 尝试15次，每次间隔5秒\n\n"
            "<b>示例2：</b> 10 3\n"
            "→ 尝试10次，每次间隔3秒\n\n"
            "<b>示例3：</b> 20 10\n"
            "→ 尝试20次，每次间隔10秒"
        ),
        'thread': (
            "🧵 <b>线程数配置示例</b>\n\n"
            "格式：线程数（1-100）\n\n"
            "<b>示例1：</b> 1\n"
            "→ 使用1个账号发送（最安全）\n\n"
            "<b>示例2：</b> 5\n"
            "→ 使用5个账号并发发送\n\n"
            "<b>示例3：</b> 10\n"
            "→ 使用10个账号并发发送（高速）"
        ),
        'interval': (
            "⏱️ <b>发送间隔配置示例</b>\n\n"
            "格式：最小秒数 最大秒数\n\n"
            "<b>示例1：</b> 30 120\n"
            "→ 每次发送间隔30-120秒\n\n"
            "<b>示例2：</b> 10 60\n"
            "→ 每次发送间隔10-60秒\n\n"
            "<b>示例3：</b> 60 300\n"
            "→ 每次发送间隔1-5分钟（更安全）"
        ),
        'daily': (
            "📊 <b>单账号日限配置示例</b>\n\n"
            "格式：每日消息数量（1-200）\n\n"
            "<b>示例1：</b> 50\n"
            "→ 每个账号每天最多发50条\n\n"
            "<b>示例2：</b> 100\n"
            "→ 每个账号每天最多发100条\n\n"
            "<b>示例3：</b> 20\n"
            "→ 每个账号每天最多发20条（保守）"
        ),
        'retry': (
            "🔄 <b>重试配置示例</b>\n\n"
            "格式：重试次数（0-10）\n\n"
            "<b>示例1：</b> 3\n"
            "→ 失败后重试3次\n\n"
            "<b>示例2：</b> 5\n"
            "→ 失败后重试5次\n\n"
            "<b>示例3：</b> 0\n"
            "→ 不重试，失败即跳过"
        ),
        'threadinterval': (
            "⏲️ <b>线程启动间隔示例</b>\n\n"
            "格式：间隔秒数（0-300）\n\n"
            "<b>示例1：</b> 10\n"
            "→ 每个线程间隔10秒启动\n\n"
            "<b>示例2：</b> 30\n"
            "→ 每个线程间隔30秒启动\n\n"
            "<b>示例3：</b> 0\n"
            "→ 所有线程同时启动"
        )
    }
    
    text = examples.get(config_type, "❌ 示例不存在")
    
    keyboard = [[InlineKeyboardButton("✅ 知道了", callback_data='close_example')]]
    
    await query.edit_message_text(
        text,
        parse_mode='HTML',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def handle_config_return(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理配置返回按钮 - 返回到任务配置界面"""
    query = update.callback_query
    await safe_answer_query(query)
    
    # 清理临时消息
    try:
        prompt_msg_id = context.user_data.get('config_prompt_msg_id')
        if prompt_msg_id:
            await context.bot.delete_message(update.effective_chat.id, prompt_msg_id)
    except Exception as e:
        logger.warning(f"Failed to delete prompt message: {e}")
    
    # 提取task_id
    task_id = query.data.split('_')[2]
    
    # 清理用户数据
    context.user_data.clear()
    
    # 显示任务配置界面
    await show_task_config(query, task_id)
    
    return ConversationHandler.END


async def show_config_menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, task_id=None):
    """Helper to show config menu"""
    if task_id is None:
        query = update.callback_query
        task_id = query.data.split('_')[2]
    
    task_doc = db[Task.COLLECTION_NAME].find_one({'_id': ObjectId(task_id)})
    task = Task.from_dict(task_doc)
    
    query = update.callback_query
    text = (
        f"⚙️ <b>配置 - {task.name}</b>\n\n"
        f"当前配置如下，点击按钮进行调整："
    )
    
    keyboard = [
        [
            InlineKeyboardButton(f"🧵 线程数: {task.thread_count}", callback_data=f'cfg_thread_{task_id}'),
            InlineKeyboardButton(f"⏱️ 间隔: {task.min_interval}-{task.max_interval}s", callback_data=f'cfg_interval_{task_id}')
        ],
        [InlineKeyboardButton(f"🔄 无视双向: {task.ignore_bidirectional_limit}次", callback_data=f'cfg_bidirect_{task_id}')],
        [
            InlineKeyboardButton(f"{'✔️' if task.pin_message else '❌'} 置顶消息", callback_data=f'cfg_toggle_pin_{task_id}'),
            InlineKeyboardButton(f"{'✔️' if task.delete_dialog else '❌'} 删除对话", callback_data=f'cfg_toggle_delete_{task_id}')
        ],
        [InlineKeyboardButton(f"{'✔️' if task.repeat_send else '❌'} 重复发送", callback_data=f'cfg_toggle_repeat_{task_id}')],
        [
            InlineKeyboardButton(f"✏️ 编辑模式", callback_data=f'cfg_edit_mode_{task_id}'),
            InlineKeyboardButton(f"💬 回复模式", callback_data=f'cfg_reply_mode_{task_id}')
        ],
        [
            InlineKeyboardButton(f"⏸️ 批次停顿", callback_data=f'cfg_batch_pause_{task_id}'),
            InlineKeyboardButton(f"🌊 FloodWait策略", callback_data=f'cfg_flood_strategy_{task_id}')
        ],
        [
            InlineKeyboardButton(f"📞 语音拨打", callback_data=f'cfg_voice_call_{task_id}'),
            InlineKeyboardButton(f"⏲️ 线程启动间隔: {task.thread_start_interval}s", callback_data=f'cfg_thread_interval_{task_id}')
        ],
        [InlineKeyboardButton(f"{'✔️' if task.auto_switch_dead_account else '❌'} 死号自动换号", callback_data=f'cfg_toggle_dead_account_{task_id}')],
        [InlineKeyboardButton("✅ 配置完成", callback_data=f'task_detail_{task_id}')],
        [InlineKeyboardButton("🔙 返回", callback_data=f'task_detail_{task_id}')]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='HTML')


async def start_task_handler(query, task_id, context):
    """Start task and show progress in new message with auto-refresh"""
    try:
        await task_manager.start_task(task_id)
        await safe_answer_query(query, "✅ 任务已开始")
        
        # Send a NEW message for progress tracking instead of editing the existing one
        task_doc = db[Task.COLLECTION_NAME].find_one({'_id': ObjectId(task_id)})
        task = Task.from_dict(task_doc)
        
        # Create initial progress message with inline buttons
        text = (
            f"⬇ <b>正在私信中</b> ⬇\n"
            f"进度 0/{task.total_targets} (0.0%)\n"
        )
        
        keyboard = [
            [
                InlineKeyboardButton("👥 总用户数", callback_data='noop'),
                InlineKeyboardButton(f"{task.total_targets}", callback_data='noop')
            ],
            [
                InlineKeyboardButton("✅ 发送成功", callback_data='noop'),
                InlineKeyboardButton("0", callback_data='noop')
            ],
            [
                InlineKeyboardButton("❌ 发送失败", callback_data='noop'),
                InlineKeyboardButton("0", callback_data='noop')
            ],
            [
                InlineKeyboardButton("🔄 刷新进度", callback_data=f'task_progress_refresh_{task_id}'),
                InlineKeyboardButton("⏸️ 停止任务", callback_data=f'task_stop_{task_id}')
            ]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        progress_msg = await query.message.reply_text(text, reply_markup=reply_markup, parse_mode='HTML')
        
        # 启动后台自动刷新任务（不阻塞）- 带异常处理包装
        async def auto_refresh_wrapper():
            try:
                await auto_refresh_task_progress(
                    context.bot,
                    query.message.chat_id,
                    progress_msg.message_id,
                    task_id
                )
            except asyncio.CancelledError:
                logger.info(f"Auto-refresh task for task {task_id} was cancelled")
                raise  # Re-raise to properly handle cancellation
            except Exception as e:
                logger.error(f"Unhandled exception in auto_refresh_task_progress for task {task_id}: {e}", exc_info=True)
        
        refresh_task = asyncio.create_task(auto_refresh_wrapper())
        
        # Store the refresh task so it can be cancelled later if needed
        if not hasattr(task_manager, 'refresh_tasks'):
            task_manager.refresh_tasks = {}
        task_manager.refresh_tasks[task_id] = refresh_task
        
        # Wait 1 second then refresh to show initial progress
        await asyncio.sleep(1)
        
        # Get updated task data
        task_doc = db[Task.COLLECTION_NAME].find_one({'_id': ObjectId(task_id)})
        if task_doc:
            task = Task.from_dict(task_doc)
            progress = (task.sent_count / task.total_targets * 100) if task.total_targets > 0 else 0
            
            text = (
                f"⬇ <b>正在私信中</b> ⬇\n"
                f"进度 {task.sent_count}/{task.total_targets} ({progress:.1f}%)\n"
            )
            
            keyboard = [
                [
                    InlineKeyboardButton("👥 总用户数", callback_data='noop'),
                    InlineKeyboardButton(f"{task.total_targets}", callback_data='noop')
                ],
                [
                    InlineKeyboardButton("✅ 发送成功", callback_data='noop'),
                    InlineKeyboardButton(f"{task.sent_count}", callback_data='noop')
                ],
                [
                    InlineKeyboardButton("❌ 发送失败", callback_data='noop'),
                    InlineKeyboardButton(f"{task.failed_count}", callback_data='noop')
                ],
                [
                    InlineKeyboardButton("🔄 刷新进度", callback_data=f'task_progress_refresh_{task_id}'),
                    InlineKeyboardButton("⏸️ 停止任务", callback_data=f'task_stop_{task_id}')
                ]
            ]
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            try:
                await progress_msg.edit_text(text, reply_markup=reply_markup, parse_mode='HTML')
            except Exception as e:
                logger.warning(f"Failed to update initial progress: {e}")
        
    except ValueError as e:
        # ValueError 通常包含用户友好的错误消息
        await query.message.reply_text(str(e), parse_mode='HTML')
    except Exception as e:
        logger.error(f"Unexpected error starting task {task_id}: {e}", exc_info=True)
        await safe_answer_query(query, f"❌ 启动失败: {str(e)}", show_alert=True)


async def auto_refresh_task_progress(bot, chat_id, message_id, task_id):
    """Auto refresh task progress with smart intervals and improved stop detection"""
    error_count = 0
    start_time = datetime.now(timezone.utc)
    last_data = None
    
    # Wait a bit for task to actually start
    await asyncio.sleep(2)
    
    while True:
        try:
            # 获取任务状态 - 强制从数据库读取最新数据
            task_doc = db[Task.COLLECTION_NAME].find_one({'_id': ObjectId(task_id)})
            if not task_doc:
                logger.info(f"Auto-refresh stopped: Task {task_id} not found")
                break
            
            task = Task.from_dict(task_doc)
            
            # ✅ Enhanced stop detection - check both DB status and running_tasks
            if task.status in [TaskStatus.COMPLETED.value, TaskStatus.STOPPED.value, TaskStatus.FAILED.value]:
                logger.info(f"Auto-refresh stopped: Task {task_id} status is {task.status}")
                
                # Wait a moment for completion report to be sent
                await asyncio.sleep(2)
                break
            
            # ✅ Double-check if task is still in running_tasks (additional safety)
            if str(task_id) not in task_manager.running_tasks:
                logger.info(f"Auto-refresh stopped: Task {task_id} not in running_tasks")
                break
            
            # 使用任务文档中的 total_targets（已在任务创建时设置）
            total_targets = task.total_targets
            sent_count = task.sent_count
            failed_count = task.failed_count
            
            # 计算进度百分比（带验证）
            if total_targets > 0 and sent_count is not None and failed_count is not None:
                progress_percent = min(100.0, (sent_count + failed_count) / total_targets * 100)
            else:
                progress_percent = 0.0
            
            
            # Calculate progress bar (20 characters)
            bar_length = 20
            filled = int(progress_percent / 5)
            progress_bar = '█' * filled + '░' * (bar_length - filled)
            
            # 计算时间和速度
            runtime_str = "00:00:00"
            speed_str = "计算中..."
            remaining_str = "计算中..."
            
            if task.started_at:
                # 确保时区一致 - Fix Bug 1
                started_at = task.started_at
                if started_at.tzinfo is None:
                    started_at = started_at.replace(tzinfo=timezone.utc)
                
                runtime = datetime.now(timezone.utc) - started_at
                hours, remainder = divmod(int(runtime.total_seconds()), 3600)
                minutes, seconds = divmod(remainder, 60)
                runtime_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
                
                # 计算速度
                processed = sent_count + failed_count
                if processed > 0 and runtime.total_seconds() > 0:
                    speed = processed / runtime.total_seconds() * 60  # messages per minute
                    speed_str = f"{speed:.1f} 条/分钟"
                    
                    # 预计剩余时间
                    remaining_count = total_targets - processed
                    if speed > 0:
                        remaining_seconds = remaining_count / speed * 60
                        rem_hours, rem_remainder = divmod(int(remaining_seconds), 3600)
                        rem_minutes, rem_seconds = divmod(rem_remainder, 60)
                        remaining_str = f"{rem_hours:02d}:{rem_minutes:02d}:{rem_seconds:02d}"
            
            # Get current account info
            account_info = task_manager._get_current_account(task_id)
            account_section = ""
            if account_info:
                masked_phone = mask_phone_number(account_info['phone'])
                remaining_quota = max(0, account_info['daily_limit'] - account_info['sent_today'])
                account_section = (
                    f"\n📱 <b>当前账号</b>\n"
                    f"• 账号: {masked_phone}\n"
                    f"• 今日已发: {account_info['sent_today']} 条\n"
                    f"• 剩余配额: {remaining_quota} 条\n"
                )
            
            # Get recent logs
            recent_logs = task_manager._get_recent_logs(task_id, limit=5)
            logs_section = ""
            if recent_logs:
                logs_section = "\n📝 <b>最近操作</b>\n━━━━━━━━━━━━━━━━\n"
                for log in reversed(recent_logs):  # Show newest first
                    time_str, status_emoji, target, message = format_log_entry(log)
                    logs_section += f"{time_str} {status_emoji} {target} {message}\n"
            
            # Build enhanced message
            text = (
                f"🚀 <b>正在私信中</b>\n\n"
                f"📊 <b>进度统计</b>\n"
                f"━━━━━━━━━━━━━━━━\n"
                f"进度: {sent_count + failed_count}/{total_targets} ({progress_percent:.1f}%)\n"
                f"<code>{progress_bar}</code>\n\n"
                f"⏱️ <b>时间统计</b>\n"
                f"• 已运行: {runtime_str}\n"
                f"• 预计剩余: {remaining_str}\n"
                f"• 发送速度: {speed_str}\n"
                f"{account_section}\n"
                f"📈 <b>发送统计</b>\n"
                f"• ✅ 成功: {sent_count}\n"
                f"• ❌ 失败: {failed_count}\n"
                f"• ⏸️ 待发送: {total_targets - sent_count - failed_count}\n"
                f"{logs_section}"
            )
            
            keyboard = [
                [InlineKeyboardButton("🔄 刷新进度", callback_data=f'task_progress_refresh_{task_id}')],
                [InlineKeyboardButton("⏹️ 停止任务", callback_data=f'task_stop_{task_id}')]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            # Update message only if data changed
            # Use both timestamp and count for reliable change detection
            recent_log_timestamp = recent_logs[-1]['time'] if recent_logs else None
            recent_log_count = len(recent_logs) if recent_logs else 0
            current_data = (sent_count, failed_count, task.status, recent_log_timestamp, recent_log_count)
            if current_data != last_data:
                try:
                    await bot.edit_message_text(
                        text=text,
                        chat_id=chat_id,
                        message_id=message_id,
                        reply_markup=reply_markup,
                        parse_mode='HTML'
                    )
                    last_data = current_data
                    error_count = 0
                except telegram_error.BadRequest as e:
                    error_str = str(e).lower()
                    if 'message to edit not found' in error_str or 'message is not modified' in error_str:
                        pass  # Ignore these errors
                    else:
                        error_count += 1
                except Exception as e:
                    error_count += 1
                    logger.error(f"Failed to update progress: {e}")
            
            if error_count >= Config.MAX_AUTO_REFRESH_ERRORS:
                break
            
            # Dynamic refresh interval
            elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
            interval = Config.AUTO_REFRESH_FAST_INTERVAL if elapsed < Config.AUTO_REFRESH_FAST_DURATION else random.randint(Config.AUTO_REFRESH_MIN_INTERVAL, Config.AUTO_REFRESH_MAX_INTERVAL)
            await asyncio.sleep(interval)
            
        except Exception as e:
            error_count += 1
            logger.error(f"Error in auto refresh: {e}")
            if error_count >= Config.MAX_AUTO_REFRESH_ERRORS:
                break
            await asyncio.sleep(Config.AUTO_REFRESH_FAST_INTERVAL)



async def send_task_completion_report(bot, chat_id, task_id):
    """Send enhanced completion report with detailed stats and account status"""
    task_doc = db[Task.COLLECTION_NAME].find_one({'_id': ObjectId(task_id)})
    if not task_doc:
        return
    
    task = Task.from_dict(task_doc)
    
    # 状态文本
    if task.status == TaskStatus.STOPPED.value:
        status_text = "⏸️ <b>任务已手动停止</b>"
    elif task.status == TaskStatus.FAILED.value:
        error_msg = task_doc.get('error_message', '未知')
        status_text = f"❌ <b>任务失败</b>\n原因: {error_msg}"
    else:
        status_text = "✅ <b>任务完成</b>"
    
    # 统计
    total_targets = db[Target.COLLECTION_NAME].count_documents({'task_id': str(task_id)})
    remaining_count = total_targets - task.sent_count - task.failed_count
    success_rate = (task.sent_count / (task.sent_count + task.failed_count) * 100) if (task.sent_count + task.failed_count) > 0 else 0
    
    # 时间
    if task.started_at and task.completed_at:
        runtime = task.completed_at - task.started_at
        hours, remainder = divmod(int(runtime.total_seconds()), 3600)
        minutes, seconds = divmod(remainder, 60)
        runtime_str = f"{hours}小时{minutes}分{seconds}秒" if hours > 0 else f"{minutes}分{seconds}秒"
    else:
        runtime_str = "未知"
    
    # 账号状态
    active_accounts = db[Account.COLLECTION_NAME].count_documents({'status': AccountStatus.ACTIVE.value})
    limited_accounts = db[Account.COLLECTION_NAME].count_documents({'status': AccountStatus.LIMITED.value})
    banned_accounts = db[Account.COLLECTION_NAME].count_documents({'status': AccountStatus.BANNED.value})
    
    text = (
        f"{status_text}\n\n"
        f"📊 <b>任务统计：</b>\n"
        f"👥 目标用户: {total_targets}\n"
        f"✅ 发送成功: {task.sent_count}\n"
        f"❌ 发送失败: {task.failed_count}\n"
        f"⏸️ 剩余未发送: {remaining_count}\n"
        f"📈 成功率: {success_rate:.1f}%\n"
        f"⏱️ 运行时间: {runtime_str}\n\n"
        f"📱 <b>账号状态：</b>\n"
        f"✅ 可用: {active_accounts}\n"
        f"⚠️ 受限: {limited_accounts}\n"
        f"🚫 封禁: {banned_accounts}"
    )
    
    # 操作按钮
    keyboard = [
        [InlineKeyboardButton("📥 导出日志", callback_data=f'task_export_{task_id}')],
        [InlineKeyboardButton("📊 查看详情", callback_data=f'task_detail_{task_id}')],
        [InlineKeyboardButton("🔙 返回任务列表", callback_data='tasks_list')]
    ]
    
    await bot.send_message(chat_id, text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')


async def stop_task_handler(query, task_id, context):
    """Stop task with confirmation dialog (improved UX)"""
    await safe_answer_query(query)
    
    # Show confirmation dialog
    text = (
        "⚠️ <b>确认停止任务？</b>\n\n"
        "⚡ 任务将立即停止（响应时间 3秒内）\n"
        "📝 已发送的消息无法撤回\n"
        "📊 将生成任务完成报告\n\n"
        "❓ 确定要停止吗？"
    )
    
    keyboard = [
        [
            InlineKeyboardButton("✅ 确认停止", callback_data=f'task_stop_confirm_{task_id}'),
            InlineKeyboardButton("❌ 取消", callback_data=f'task_progress_{task_id}')
        ]
    ]
    
    try:
        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
    except Exception as e:
        logger.error(f"Error showing stop confirmation: {e}")
        await query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')


async def stop_task_confirmed(query, task_id, context):
    """Execute confirmed stop action (improved with better feedback)"""
    await safe_answer_query(query, "⏹️ 正在停止任务...", show_alert=True)
    
    try:
        # Show stopping progress message
        stopping_text = (
            "⏹️ <b>正在停止任务...</b>\n\n"
            "⏳ 等待当前操作完成\n"
            "📝  即将生成任务报告"
        )
        
        try:
            await query.edit_message_text(stopping_text, parse_mode='HTML')
        except Exception as e:
            logger.debug(f"Could not edit message: {e}")
        
        # Execute stop using TaskManager
        await task_manager.stop_task(task_id)
        
        # Wait a moment for cleanup
        await asyncio.sleep(1)
        
        # Show success message
        success_text = (
            "✅ <b>任务已停止</b>\n\n"
            "📊 正在生成任务报告...\n"
            "⏰ 请稍候..."
        )
        
        try:
            await query.edit_message_text(success_text, parse_mode='HTML')
        except Exception:
            await query.message.reply_text(success_text, parse_mode='HTML')
        
        # Wait for completion report to be generated
        await asyncio.sleep(2)
        
        # Show task detail with final status
        await show_task_detail(query, task_id)
        
    except ValueError as e:
        # Task not running
        logger.warning(f"Stop task error: {e}")
        await query.message.reply_text(
            f"⚠️ <b>任务状态异常</b>\n\n"
            f"任务可能已经停止或完成。\n"
            f"详情: {str(e)}",
            parse_mode='HTML'
        )
        # Still show task detail
        await show_task_detail(query, task_id)
        
    except Exception as e:
        logger.error(f"Error stopping task {task_id}: {e}", exc_info=True)
        await query.message.reply_text(
            f"❌ <b>停止任务失败</b>\n\n"
            f"错误: {str(e)}\n\n"
            f"请查看日志或联系管理员。",
            parse_mode='HTML'
        )


async def show_task_progress(query, task_id):
    """Show progress"""
    progress = task_manager.get_task_progress(task_id)
    if not progress:
        await query.message.reply_text("❌ 任务不存在")
        return
    
    text = (
        f"📊 <b>任务进度</b>\n\n"
        f"任务: {progress['name']}\n"
        f"状态: {progress['status']}\n\n"
        f"总数: {progress['total_targets']}\n"
        f"已发送: {progress['sent_count']}\n"
        f"失败: {progress['failed_count']}\n"
        f"待发送: {progress['pending_count']}\n"
        f"进度: {progress['progress_percent']:.1f}%"
    )
    await query.message.reply_text(text, parse_mode='HTML')


async def refresh_task_progress(query, task_id):
    """刷新任务进度 - 更新进度显示的内联按钮"""
    logger.info(f"刷新任务进度: Task ID={task_id}")
    
    task_doc = db[Task.COLLECTION_NAME].find_one({'_id': ObjectId(task_id)})
    if not task_doc:
        await safe_answer_query(query, "❌ 任务不存在", show_alert=True)
        return
    
    task = Task.from_dict(task_doc)
    
    # Calculate progress
    total = task.total_targets or 0
    sent = task.sent_count or 0
    failed = task.failed_count or 0
    processed = sent + failed
    progress_percent = (processed / total * 100) if total > 0 else 0
    
    # Progress bar (20 characters)
    bar_length = 20
    filled = int(progress_percent / 5)
    progress_bar = '█' * filled + '░' * (bar_length - filled)
    
    # Time calculations
    runtime_str = "00:00:00"
    speed_str = "计算中..."
    remaining_str = "计算中..."
    
    if task.started_at:
        # 确保时区一致 - Fix Bug 1
        started_at = task.started_at
        if started_at.tzinfo is None:
            started_at = started_at.replace(tzinfo=timezone.utc)
        
        runtime = datetime.now(timezone.utc) - started_at
        hours, remainder = divmod(int(runtime.total_seconds()), 3600)
        minutes, seconds = divmod(remainder, 60)
        runtime_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        
        if processed > 0 and runtime.total_seconds() > 0:
            speed = processed / runtime.total_seconds() * 60
            speed_str = f"{speed:.1f} 条/分钟"
            
            remaining_count = total - processed
            if speed > 0:
                remaining_seconds = remaining_count / speed * 60
                rem_hours, rem_remainder = divmod(int(remaining_seconds), 3600)
                rem_minutes, rem_seconds = divmod(rem_remainder, 60)
                remaining_str = f"{rem_hours:02d}:{rem_minutes:02d}:{rem_seconds:02d}"
    
    # Get current account info
    account_info = task_manager._get_current_account(task_id)
    account_section = ""
    if account_info:
        masked_phone = mask_phone_number(account_info['phone'])
        remaining_quota = max(0, account_info['daily_limit'] - account_info['sent_today'])
        account_section = (
            f"\n📱 <b>当前账号</b>\n"
            f"• 账号: {masked_phone}\n"
            f"• 今日已发: {account_info['sent_today']} 条\n"
            f"• 剩余配额: {remaining_quota} 条\n"
        )
    
    # Get recent logs
    recent_logs = task_manager._get_recent_logs(task_id, limit=5)
    logs_section = ""
    if recent_logs:
        logs_section = "\n📝 <b>最近操作</b>\n━━━━━━━━━━━━━━━━\n"
        for log in reversed(recent_logs):  # Show newest first
            time_str, status_emoji, target, message = format_log_entry(log)
            logs_section += f"{time_str} {status_emoji} {target} {message}\n"
    
    # Build enhanced message
    text = (
        f"🚀 <b>正在私信中</b>\n\n"
        f"📊 <b>进度统计</b>\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"进度: {processed}/{total} ({progress_percent:.1f}%)\n"
        f"<code>{progress_bar}</code>\n\n"
        f"⏱️ <b>时间统计</b>\n"
        f"• 已运行: {runtime_str}\n"
        f"• 预计剩余: {remaining_str}\n"
        f"• 发送速度: {speed_str}\n"
        f"{account_section}\n"
        f"📈 <b>发送统计</b>\n"
        f"• ✅ 成功: {sent}\n"
        f"• ❌ 失败: {failed}\n"
        f"• ⏸️ 待发送: {total - processed}\n"
        f"{logs_section}"
    )
    
    keyboard = [
        [InlineKeyboardButton("🔄 刷新进度", callback_data=f'task_progress_refresh_{task_id}')],
        [InlineKeyboardButton("⏹️ 停止任务", callback_data=f'task_stop_{task_id}')]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    try:
        await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='HTML')
        await safe_answer_query(query, "✅ 进度已刷新")
    except Exception as e:
        logger.error(f"更新进度显示失败: {e}")
        await safe_answer_query(query, "刷新完成")


async def export_results(query, task_id):
    """Export results"""
    results = task_manager.export_task_results(task_id)
    if not results:
        await query.message.reply_text("❌ 任务不存在")
        return
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    success_file = os.path.join(Config.RESULTS_DIR, f"success_{task_id}_{timestamp}.txt")
    with open(success_file, 'w', encoding='utf-8') as f:
        for t in results['success_targets']:
            f.write(f"{t.username or t.user_id}\n")
    
    failed_file = os.path.join(Config.RESULTS_DIR, f"failed_{task_id}_{timestamp}.txt")
    with open(failed_file, 'w', encoding='utf-8') as f:
        for t in results['failed_targets']:
            f.write(f"{t.username or t.user_id}: {t.error_message}\n")
    
    log_file = os.path.join(Config.RESULTS_DIR, f"log_{task_id}_{timestamp}.txt")
    with open(log_file, 'w', encoding='utf-8') as f:
        for log in results['logs']:
            status = "成功" if log.success else "失败"
            f.write(f"[{log.sent_at}] {status}: {log.error_message or 'OK'}\n")
    
    # Only send non-empty files (Telegram API rejects empty files)
    try:
        if os.path.getsize(success_file) > 0:
            with open(success_file, 'rb') as f:
                await query.message.reply_document(document=f, filename="success.txt")
    except Exception as e:
        logger.warning(f"Failed to send success file: {e}")
    
    try:
        if os.path.getsize(failed_file) > 0:
            with open(failed_file, 'rb') as f:
                await query.message.reply_document(document=f, filename="failed.txt")
    except Exception as e:
        logger.warning(f"Failed to send failed file: {e}")
    
    try:
        if os.path.getsize(log_file) > 0:
            with open(log_file, 'rb') as f:
                await query.message.reply_document(document=f, filename="log.txt")
    except Exception as e:
        logger.warning(f"Failed to send log file: {e}")
    
    await query.message.reply_text("✅ 结果已导出")


async def toggle_task_config(query, task_id, toggle_type):
    """Toggle task configuration options"""
    task_doc = db[Task.COLLECTION_NAME].find_one({'_id': ObjectId(task_id)})
    if not task_doc:
        await safe_answer_query(query, "❌ 任务不存在", show_alert=True)
        return
    
    task = Task.from_dict(task_doc)
    
    # Toggle the appropriate field
    if toggle_type == 'pin':
        task.pin_message = not task.pin_message
        db[Task.COLLECTION_NAME].update_one(
            {'_id': ObjectId(task_id)},
            {'$set': {'pin_message': task.pin_message, 'updated_at': datetime.utcnow()}}
        )
        await safe_answer_query(query, f"{'✔️ 已启用' if task.pin_message else '❌ 已禁用'} 置顶消息")
    elif toggle_type == 'delete':
        task.delete_dialog = not task.delete_dialog
        db[Task.COLLECTION_NAME].update_one(
            {'_id': ObjectId(task_id)},
            {'$set': {'delete_dialog': task.delete_dialog, 'updated_at': datetime.utcnow()}}
        )
        await safe_answer_query(query, f"{'✔️ 已启用' if task.delete_dialog else '❌ 已禁用'} 删除对话框")
    elif toggle_type == 'repeat':
        task.repeat_send = not task.repeat_send
        db[Task.COLLECTION_NAME].update_one(
            {'_id': ObjectId(task_id)},
            {'$set': {'repeat_send': task.repeat_send, 'updated_at': datetime.utcnow()}}
        )
        await safe_answer_query(query, f"{'✔️ 已启用' if task.repeat_send else '❌ 已禁用'} 重复发送")
    
    # Refresh the config page
    await show_task_config(query, task_id)


async def delete_task_handler(query, task_id):
    """Delete task handler"""
    try:
        # Get task info before deleting
        task_doc = db[Task.COLLECTION_NAME].find_one({'_id': ObjectId(task_id)})
        if not task_doc:
            await safe_answer_query(query, "❌ 任务不存在", show_alert=True)
            return
        
        task = Task.from_dict(task_doc)
        
        # Delete the task
        task_manager.delete_task(task_id)
        
        await safe_answer_query(query, f"✅ 任务 '{task.name}' 已删除", show_alert=True)
        
        # Refresh the task list
        await list_tasks(query)
        
    except ValueError as e:
        logger.error(f"Error deleting task {task_id}: {e}")
        await safe_answer_query(query, f"❌ {str(e)}", show_alert=True)
    except Exception as e:
        logger.error(f"Unexpected error deleting task {task_id}: {e}")
        await safe_answer_query(query, "❌ 删除任务时发生错误", show_alert=True)


async def show_config(query):
    """Show config"""
    # Get proxy count
    total_proxies = db[Proxy.COLLECTION_NAME].count_documents({})
    active_proxies = db[Proxy.COLLECTION_NAME].count_documents({'is_active': True})
    
    text = (
        "⚙️ <b>全局配置</b>\n\n"
        f"⏱️ 最小间隔: {Config.DEFAULT_MIN_INTERVAL}s\n"
        f"⏱️ 最大间隔: {Config.DEFAULT_MAX_INTERVAL}s\n"
        f"📮 每日限制: {Config.DEFAULT_DAILY_LIMIT}\n"
        f"🌐 全局代理: {'启用' if Config.PROXY_ENABLED else '禁用'}\n"
        f"🌐 代理池: {active_proxies}/{total_proxies} 个可用\n\n"
        "修改请编辑 .env 文件"
    )
    keyboard = [
        [InlineKeyboardButton("🌐 代理管理", callback_data='config_proxy')],
        [InlineKeyboardButton("🔙 返回", callback_data='menu_messaging')]
    ]
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')


async def show_stats(query):
    """Show stats"""
    total_accounts = db[Account.COLLECTION_NAME].count_documents({})
    active_accounts = db[Account.COLLECTION_NAME].count_documents({'status': AccountStatus.ACTIVE.value})
    total_tasks = db[Task.COLLECTION_NAME].count_documents({})
    completed_tasks = db[Task.COLLECTION_NAME].count_documents({'status': TaskStatus.COMPLETED.value})
    total_msgs = db[MessageLog.COLLECTION_NAME].count_documents({})
    success_msgs = db[MessageLog.COLLECTION_NAME].count_documents({'success': True})
    
    text = (
        "📊 <b>统计信息</b>\n\n"
        f"📱 账户: {active_accounts}/{total_accounts}\n"
        f"📝 任务: {completed_tasks}/{total_tasks}\n"
        f"📨 消息: {success_msgs}/{total_msgs}\n"
        f"成功率: {(success_msgs/total_msgs*100):.1f}%" if total_msgs > 0 else "成功率: 0%"
    )
    keyboard = [[InlineKeyboardButton("🔙 返回", callback_data='menu_messaging')]]
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')


async def show_help(query):
    """Show help"""
    text = (
        "❓ <b>帮助</b>\n\n"
        "<b>快速开始：</b>\n"
        "1️⃣ 添加账户\n"
        "2️⃣ 创建任务\n"
        "3️⃣ 配置消息\n"
        "4️⃣ 开始任务\n"
        "5️⃣ 查看进度\n"
        "6️⃣ 导出结果\n\n"
        "<b>变量：</b>\n"
        "{name}, {first_name}, {last_name}, {full_name}, {username}"
    )
    keyboard = [[InlineKeyboardButton("🔙 返回", callback_data='back_main')]]
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')


async def back_to_main(query):
    """Back to main"""
    keyboard = [
        [InlineKeyboardButton("📢 广告私信", callback_data='menu_messaging'), InlineKeyboardButton("👥 采集用户", callback_data='menu_collection')],
        [InlineKeyboardButton("❓ 帮助", callback_data='menu_help')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    text = "🤖 <b>主菜单</b>\n\n请选择："
    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='HTML')


# ============================================================================
# 代理管理界面
# ============================================================================
async def show_proxy_menu(query):
    """Show proxy management menu"""
    total_proxies = db[Proxy.COLLECTION_NAME].count_documents({})
    active_proxies = db[Proxy.COLLECTION_NAME].count_documents({'is_active': True})
    
    text = (
        "🌐 <b>代理管理</b>\n\n"
        f"代理总数: {total_proxies}\n"
        f"可用代理: {active_proxies}\n\n"
        f"💡 <b>自动分配模式</b>\n"
        f"账户登录时自动从代理池获取代理\n"
        f"连接超时则自动退回本地连接\n\n"
        "选择操作："
    )
    
    keyboard = [
        [InlineKeyboardButton("📋 代理列表", callback_data='proxy_list')],
        [InlineKeyboardButton("📤 上传代理文件", callback_data='proxy_upload')],
        [InlineKeyboardButton("🗑️ 清空所有代理", callback_data='proxy_clear')],
        [InlineKeyboardButton("🔙 返回", callback_data='menu_config')]
    ]
    
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')


async def list_proxies(query):
    """List all proxies"""
    proxies = list(db[Proxy.COLLECTION_NAME].find().limit(20))
    
    if not proxies:
        text = "🌐 <b>代理列表</b>\n\n暂无代理"
        keyboard = [
            [InlineKeyboardButton("📤 上传代理文件", callback_data='proxy_upload')],
            [InlineKeyboardButton("🔙 返回", callback_data='config_proxy')]
        ]
    else:
        text = f"🌐 <b>代理列表</b> (共 {len(proxies)} 个)\n\n"
        keyboard = []
        
        for proxy_doc in proxies:
            proxy = Proxy.from_dict(proxy_doc)
            status_emoji = '✅' if proxy.is_active else '❌'
            auth_info = f"({proxy.username})" if proxy.username else "(无认证)"
            text += (
                f"{status_emoji} <code>{proxy.host}:{proxy.port}</code> {auth_info}\n"
                f"   类型: {proxy.proxy_type} | 成功: {proxy.success_count} | 失败: {proxy.fail_count}\n\n"
            )
            
            # Add action buttons for each proxy
            keyboard.append([
                InlineKeyboardButton(f"测试 {proxy.host}:{proxy.port}", callback_data=f'proxy_test_{str(proxy._id)}'),
                InlineKeyboardButton("🔄" if not proxy.is_active else "⏸️", callback_data=f'proxy_toggle_{str(proxy._id)}'),
                InlineKeyboardButton("🗑️", callback_data=f'proxy_delete_{str(proxy._id)}')
            ])
        
        keyboard.append([InlineKeyboardButton("🔙 返回", callback_data='config_proxy')])
    
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')


async def handle_proxy_upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle proxy file upload"""
    if context.user_data.get('waiting_for') != 'proxy_file':
        return
    
    user_id = update.message.from_user.id
    if user_id != Config.ADMIN_USER_ID:
        await update.message.reply_text("❌ 无权限")
        return
    
    try:
        # Download file
        file = await update.message.document.get_file()
        file_path = os.path.join(Config.UPLOADS_DIR, f"proxies_{user_id}.txt")
        await file.download_to_drive(file_path)
        
        # Parse and import proxies
        imported_count = 0
        failed_count = 0
        
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                proxy = parse_proxy_line(line)
                if proxy:
                    try:
                        db[Proxy.COLLECTION_NAME].insert_one(proxy.to_dict())
                        imported_count += 1
                    except Exception as e:
                        logger.warning(f"Failed to insert proxy: {e}")
                        failed_count += 1
                else:
                    failed_count += 1
        
        # Clean up
        os.remove(file_path)
        context.user_data['waiting_for'] = None
        
        # Add test button if proxies were imported
        keyboard = []
        if imported_count > 0:
            keyboard.append([InlineKeyboardButton("🧪 测试所有代理", callback_data='proxy_test_all')])
        keyboard.append([InlineKeyboardButton("🔙 返回代理管理", callback_data='config_proxy')])
        
        await update.message.reply_text(
            f"✅ <b>代理导入完成</b>\n\n"
            f"成功导入: {imported_count} 个\n"
            f"导入失败: {failed_count} 个\n\n"
            f"💡 代理将在账户连接时自动分配使用\n\n"
            f"{'📝 点击下方按钮测试所有代理' if imported_count > 0 else ''}",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )
        
    except Exception as e:
        logger.error(f"Error uploading proxies: {e}", exc_info=True)
        await update.message.reply_text(f"❌ 上传失败：{str(e)}")
        context.user_data['waiting_for'] = None


# ============================================================================
# MAIN
# ============================================================================
def main():
    """Main function"""
    global account_manager, task_manager, collection_manager, db
    
    logger.info("=" * 80)
    logger.info("Starting Telegram Bot")
    logger.info("=" * 80)
    
    try:
        logger.info("Validating configuration...")
        Config.validate()
        logger.info("Configuration validated successfully")
        
        logger.info("Ensuring directories exist...")
        Config.ensure_directories()
        logger.info("Directories created/verified")
    except ValueError as e:
        logger.error(f"Config error: {e}")
        return
    
    logger.info(f"Initializing database: {Config.MONGODB_URI}")
    db = init_db(Config.MONGODB_URI, Config.MONGODB_DATABASE)
    logger.info("Database initialized successfully")
    
    # 数据迁移：为已存在的账户添加默认 account_type
    logger.info("Running database migration for account_type...")
    migration_result = db[Account.COLLECTION_NAME].update_many(
        {'account_type': {'$exists': False}},
        {'$set': {'account_type': 'messaging'}}
    )
    if migration_result.modified_count > 0:
        logger.info(f"Migrated {migration_result.modified_count} existing accounts to messaging type")
    else:
        logger.info("No accounts needed migration")
    
    logger.info("Initializing caiji module database...")
    caiji.init_db(db)
    logger.info("Caiji module database initialized")
    
    logger.info("Initializing account manager...")
    account_manager = AccountManager(db)
    logger.info("Account manager initialized")
    
    logger.info("Initializing task manager...")
    # 先创建application以便传递给TaskManager
    logger.info("Building bot application...")
    application = Application.builder().token(Config.BOT_TOKEN).build()
    
    # 创建task_manager时传入bot_application
    task_manager = TaskManager(db, account_manager, application)
    logger.info("Task manager initialized with bot application")
    
    logger.info("Initializing collection manager...")
    collection_manager = CollectionManager(db, account_manager)
    logger.info("Collection manager initialized")
    
    logger.info("Initializing caiji module collection manager...")
    caiji.init_collection_manager(collection_manager)
    logger.info("Caiji module collection manager initialized")
    
    logger.info("Registering command handlers...")
    application.add_handler(CommandHandler("start", start))
    
    # File upload conversation handler (registered BEFORE button_handler to catch specific callbacks first)
    logger.info("Registering file upload conversation handler...")
    upload_conv = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(request_session_upload, pattern='^upload_session_file$'),
            CallbackQueryHandler(request_tdata_upload, pattern='^upload_tdata_file$')
        ],
        states={
            SESSION_UPLOAD: [
                MessageHandler(filters.Document.ALL & ~filters.COMMAND, handle_file_upload),
                CallbackQueryHandler(button_handler)
            ],
            TDATA_UPLOAD: [
                MessageHandler(filters.Document.ALL & ~filters.COMMAND, handle_file_upload),
                CallbackQueryHandler(button_handler)
            ]
        },
        fallbacks=[CommandHandler("start", start)]
    )
    application.add_handler(upload_conv)
    
    # Task creation conversation handler
    logger.info("Registering task conversation handler...")
    task_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(start_create_task, pattern='^tasks_create$')],
        states={
            TASK_NAME_INPUT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_task_name),
                CallbackQueryHandler(button_handler)
            ],
            MESSAGE_INPUT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message_input),
                CallbackQueryHandler(button_handler)
            ],
            FORMAT_SELECT: [CallbackQueryHandler(button_handler)],
            SEND_METHOD_SELECT: [CallbackQueryHandler(button_handler)],
            POSTBOT_CODE_INPUT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_postbot_code_input),
                CallbackQueryHandler(button_handler)
            ],
            CHANNEL_LINK_INPUT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_channel_link_input),
                CallbackQueryHandler(button_handler)
            ],
            PREVIEW_CONFIG: [CallbackQueryHandler(button_handler)],
            MEDIA_SELECT: [CallbackQueryHandler(button_handler)],
            MEDIA_UPLOAD: [
                MessageHandler((filters.Document.ALL | filters.PHOTO | filters.VIDEO) & ~filters.COMMAND, handle_media_upload),
                CallbackQueryHandler(button_handler)
            ],
            TARGET_INPUT: [
                MessageHandler((filters.TEXT | filters.Document.ALL) & ~filters.COMMAND, handle_target_input),
                CallbackQueryHandler(button_handler)  # Allow clicking config buttons to exit TARGET_INPUT
            ]
        },
        fallbacks=[CommandHandler("start", start)]
    )
    
    application.add_handler(task_conv)
    
    # Task configuration conversation handler
    logger.info("Registering task configuration conversation handler...")
    config_conv = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(request_thread_interval_config, pattern='^cfg_thread_interval_'),
            CallbackQueryHandler(request_thread_config, pattern='^cfg_thread_'),
            CallbackQueryHandler(request_interval_config, pattern='^cfg_interval_'),
            CallbackQueryHandler(request_bidirect_config, pattern='^cfg_bidirect_'),
            CallbackQueryHandler(request_daily_limit_config, pattern='^cfg_daily_limit_'),
            CallbackQueryHandler(request_retry_config, pattern='^cfg_retry_'),
            CallbackQueryHandler(request_reply_mode_config, pattern='^cfg_reply_mode_'),
            CallbackQueryHandler(request_batch_count_config, pattern='^set_batch_count_'),
            CallbackQueryHandler(request_batch_delay_config, pattern='^set_batch_delay_')
        ],
        states={
            CONFIG_THREAD_INPUT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_thread_config),
                CallbackQueryHandler(handle_config_cancel, pattern='^cfg_cancel_'),
                CallbackQueryHandler(show_config_example, pattern='^cfg_example_'),
                CallbackQueryHandler(handle_config_return, pattern='^task_config_')
            ],
            CONFIG_INTERVAL_MIN_INPUT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_interval_config),
                CallbackQueryHandler(handle_config_cancel, pattern='^cfg_cancel_'),
                CallbackQueryHandler(show_config_example, pattern='^cfg_example_'),
                CallbackQueryHandler(handle_config_return, pattern='^task_config_')
            ],
            CONFIG_BIDIRECT_INPUT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_bidirect_config),
                CallbackQueryHandler(handle_config_cancel, pattern='^cfg_cancel_'),
                CallbackQueryHandler(show_config_example, pattern='^cfg_example_'),
                CallbackQueryHandler(handle_config_return, pattern='^task_config_')
            ],
            CONFIG_THREAD_INTERVAL_INPUT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_thread_interval_config),
                CallbackQueryHandler(handle_config_cancel, pattern='^cfg_cancel_'),
                CallbackQueryHandler(show_config_example, pattern='^cfg_example_'),
                CallbackQueryHandler(handle_config_return, pattern='^task_config_')
            ],
            CONFIG_DAILY_LIMIT_INPUT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_daily_limit_config),
                CallbackQueryHandler(handle_config_cancel, pattern='^cfg_cancel_'),
                CallbackQueryHandler(show_config_example, pattern='^cfg_example_'),
                CallbackQueryHandler(handle_config_return, pattern='^task_config_')
            ],
            CONFIG_RETRY_INPUT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_retry_config),
                CallbackQueryHandler(handle_config_cancel, pattern='^cfg_cancel_'),
                CallbackQueryHandler(show_config_example, pattern='^cfg_example_'),
                CallbackQueryHandler(handle_config_return, pattern='^task_config_')
            ],
            CONFIG_REPLY_MODE_INPUT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_reply_mode_config),
                CallbackQueryHandler(handle_config_cancel, pattern='^cfg_cancel_'),
                CallbackQueryHandler(show_config_example, pattern='^cfg_example_'),
                CallbackQueryHandler(handle_config_return, pattern='^task_config_')
            ],
            CONFIG_BATCH_COUNT_INPUT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_batch_count_config),
                CallbackQueryHandler(handle_config_cancel, pattern='^cfg_cancel_'),
                CallbackQueryHandler(handle_config_return, pattern='^task_config_')
            ],
            CONFIG_BATCH_DELAY_INPUT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_batch_delay_config),
                CallbackQueryHandler(handle_config_cancel, pattern='^cfg_cancel_'),
                CallbackQueryHandler(handle_config_return, pattern='^task_config_')
            ]
        },
        fallbacks=[
            CommandHandler("start", start),
            CallbackQueryHandler(handle_config_cancel, pattern='^cfg_cancel_')
        ]
    )
    application.add_handler(config_conv)
    
    # Collection conversation handler
    logger.info("Registering collection conversation handler...")
    collection_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(caiji.start_create_collection, pattern='^collection_create$')],
        states={
            caiji.COLLECTION_NAME_INPUT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, caiji.handle_collection_name),
                CallbackQueryHandler(button_handler)
            ],
            caiji.COLLECTION_TYPE_SELECT: [CallbackQueryHandler(caiji.handle_collection_type, pattern='^coll_type_')],
            caiji.COLLECTION_ACCOUNT_SELECT: [CallbackQueryHandler(caiji.handle_collection_account, pattern='^coll_account_')],
            caiji.COLLECTION_TARGET_INPUT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, caiji.handle_collection_target),
                CallbackQueryHandler(button_handler)
            ],
            caiji.COLLECTION_KEYWORD_INPUT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, caiji.handle_collection_keyword),
                CallbackQueryHandler(button_handler)
            ],
            caiji.COLLECTION_FILTER_CONFIG: [
                CallbackQueryHandler(caiji.show_filter_config, pattern='^coll_configure_filters$'),
                CallbackQueryHandler(caiji.toggle_filter, pattern='^coll_filter_toggle_'),
                CallbackQueryHandler(caiji.create_collection_now, pattern='^coll_create_now$')
            ]
        },
        fallbacks=[CommandHandler("start", start)]
    )
    application.add_handler(collection_conv)
    
    # Proxy file upload handler (for document uploads when waiting for proxy file)
    logger.info("Registering proxy file upload handler...")
    application.add_handler(MessageHandler(filters.Document.ALL & ~filters.COMMAND, handle_proxy_upload))
    
    # General button handler (registered AFTER conversation handlers)
    logger.info("Registering general button handler...")
    application.add_handler(CallbackQueryHandler(button_handler))
    
    logger.info("=" * 80)
    logger.info("Bot started successfully! Listening for updates...")
    logger.info("=" * 80)
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == '__main__':
    main()