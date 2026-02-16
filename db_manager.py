"""数据库管理模块 - SQLAlchemy ORM版本

提供DBManager类，封装所有数据库操作。
使用SQLAlchemy ORM替代原生sqlite3操作，同时兼容SQLite和PostgreSQL。
所有公共方法签名和返回格式保持不变，确保调用方无需修改。
"""

import os
import time
import json
import hashlib
import random
import string
import io
import base64
import logging
import threading
from datetime import datetime, timedelta
from contextlib import contextmanager
from typing import Dict, List, Optional, Any, Tuple

from passlib.hash import bcrypt as bcrypt_hash
from PIL import Image, ImageDraw, ImageFont

from database import SessionLocal, engine, Base, is_postgresql
from models import (
    User, EmailVerification, CaptchaCode,
    CookieAccount, CookieStatus, Keyword,
    AIReplySetting, AIConversation, AIItemCache,
    Card, Order, ItemInfo, DeliveryRule,
    DefaultReply, DefaultReplyRecord, ItemReplay,
    NotificationChannel, MessageNotification,
    SystemSetting, UserSetting, RiskControlLog
)
from sqlalchemy import func, and_, or_, literal, text, inspect as sa_inspect
from sqlalchemy.exc import IntegrityError

logger = logging.getLogger(__name__)

# AI回复设置默认值
DEFAULT_MODEL = 'qwen-plus'
DEFAULT_BASE_URL = 'https://dashscope.aliyuncs.com/compatible-mode/v1'


class DBManager:
    """数据库管理器 - SQLAlchemy ORM版本

    封装所有数据库CRUD操作，使用SQLAlchemy ORM，
    兼容SQLite和PostgreSQL。
    """

    # 允许访问的表名白名单
    ALLOWED_TABLES = {
        'users', 'cookies', 'cookie_status', 'keywords',
        'default_replies', 'default_reply_records', 'item_replay',
        'ai_reply_settings', 'ai_conversations', 'ai_item_cache',
        'item_info', 'message_notifications', 'cards',
        'delivery_rules', 'notification_channels', 'user_settings',
        'system_settings', 'email_verifications', 'captcha_codes', 'orders',
        'risk_control_logs'
    }

    # 表名到模型的映射
    TABLE_MODEL_MAP = {
        'users': User,
        'cookies': CookieAccount,
        'cookie_status': CookieStatus,
        'keywords': Keyword,
        'default_replies': DefaultReply,
        'default_reply_records': DefaultReplyRecord,
        'item_replay': ItemReplay,
        'ai_reply_settings': AIReplySetting,
        'ai_conversations': AIConversation,
        'ai_item_cache': AIItemCache,
        'item_info': ItemInfo,
        'message_notifications': MessageNotification,
        'cards': Card,
        'delivery_rules': DeliveryRule,
        'notification_channels': NotificationChannel,
        'user_settings': UserSetting,
        'system_settings': SystemSetting,
        'email_verifications': EmailVerification,
        'captcha_codes': CaptchaCode,
        'orders': Order,
        'risk_control_logs': RiskControlLog,
    }

    def __init__(self, db_path: str = None):
        """初始化数据库管理器

        Args:
            db_path: 数据库路径（仅用于向后兼容，ORM模式下由database.py管理连接）
        """
        # 向后兼容：保留db_path属性，供reply_server.py等外部代码引用
        if db_path is None:
            db_path = os.getenv('DB_PATH', 'data/xianyu_data.db')
        self.db_path = db_path

        self.lock = threading.RLock()
        self.init_db()
        # 向后兼容：提供原始DBAPI连接，供外部直接使用cursor的代码继续工作
        # （如 ai_reply_engine.py、reply_server.py 中直接访问 db_manager.conn 的代码）
        self.conn = engine.raw_connection()
        logger.info(f"DBManager初始化完成 (SQLAlchemy ORM), db_path={self.db_path}")

    def _validate_table_name(self, table_name: str):
        """验证表名是否在白名单中，防止SQL注入"""
        if table_name not in self.ALLOWED_TABLES:
            raise ValueError(f"非法表名: {table_name}")

    @contextmanager
    def _session(self):
        """获取数据库会话的上下文管理器"""
        session = SessionLocal()
        try:
            yield session
        finally:
            session.close()

    def init_db(self):
        """初始化数据库 - 创建所有表"""
        try:
            Base.metadata.create_all(bind=engine)
            logger.info("数据库表结构初始化完成")
        except Exception as e:
            logger.error(f"数据库初始化失败: {e}")
            raise

    def close(self):
        """关闭数据库连接"""
        try:
            if hasattr(self, 'conn') and self.conn:
                self.conn.close()
                logger.info("原始数据库连接已关闭")
        except Exception as e:
            logger.warning(f"关闭原始连接时出错: {e}")
        logger.info("DBManager关闭")

    # ==================== Cookie操作 ====================

    def save_cookie(self, cookie_id: str, cookie_value: str, user_id: int = 1):
        """保存或更新Cookie"""
        with self._session() as session:
            try:
                existing = session.query(CookieAccount).filter_by(id=cookie_id).first()
                if existing:
                    existing.value = cookie_value
                    existing.user_id = user_id
                else:
                    cookie = CookieAccount(id=cookie_id, value=cookie_value, user_id=user_id)
                    session.add(cookie)
                session.commit()
                logger.debug(f"保存Cookie: {cookie_id}")
            except Exception as e:
                session.rollback()
                logger.error(f"保存Cookie失败: {e}")
                raise

    def delete_cookie(self, cookie_id: str):
        """删除Cookie"""
        with self._session() as session:
            try:
                cookie = session.query(CookieAccount).filter_by(id=cookie_id).first()
                if cookie:
                    session.delete(cookie)
                    session.commit()
                    logger.debug(f"删除Cookie: {cookie_id}")
                    return True
                return False
            except Exception as e:
                session.rollback()
                logger.error(f"删除Cookie失败: {e}")
                raise

    def get_cookie(self, cookie_id: str) -> Optional[str]:
        """获取Cookie值"""
        with self._session() as session:
            try:
                cookie = session.query(CookieAccount).filter_by(id=cookie_id).first()
                return cookie.value if cookie else None
            except Exception as e:
                logger.error(f"获取Cookie失败: {e}")
                return None

    def get_all_cookies(self, user_id: int = None) -> list:
        """获取所有Cookie（返回列表格式）"""
        with self._session() as session:
            try:
                query = session.query(CookieAccount)
                if user_id is not None:
                    query = query.filter_by(user_id=user_id)
                cookies = query.all()
                return [
                    {
                        'id': c.id,
                        'value': c.value,
                        'user_id': c.user_id,
                        'auto_confirm': c.auto_confirm,
                        'remark': c.remark or '',
                        'pause_duration': c.pause_duration or 10,
                        'username': c.username or '',
                        'password': c.password or '',
                        'show_browser': c.show_browser or 0
                    }
                    for c in cookies
                ]
            except Exception as e:
                logger.error(f"获取所有Cookie失败: {e}")
                return []

    def get_all_cookies_dict(self, user_id: int = None) -> dict:
        """获取所有Cookie（返回 {cookie_id: cookie_value} 字典格式，用于快速查找和权限校验）"""
        cookies_list = self.get_all_cookies(user_id)
        return {c['id']: c.get('value', '') for c in cookies_list}

    def get_cookie_by_id(self, cookie_id: str, user_id: int = None) -> Optional[Dict[str, Any]]:
        """根据ID获取Cookie详情"""
        with self._session() as session:
            try:
                query = session.query(CookieAccount).filter_by(id=cookie_id)
                if user_id is not None:
                    query = query.filter_by(user_id=user_id)
                cookie = query.first()
                if cookie:
                    return {
                        'id': cookie.id,
                        'value': cookie.value,
                        'user_id': cookie.user_id,
                        'auto_confirm': cookie.auto_confirm,
                        'remark': cookie.remark or '',
                        'pause_duration': cookie.pause_duration or 10,
                        'username': cookie.username or '',
                        'password': cookie.password or '',
                        'show_browser': cookie.show_browser or 0
                    }
                return None
            except Exception as e:
                logger.error(f"获取Cookie详情失败: {e}")
                return None

    def get_cookie_details(self, cookie_id: str) -> Optional[Dict[str, Any]]:
        """获取Cookie详细信息"""
        with self._session() as session:
            try:
                cookie = session.query(CookieAccount).filter_by(id=cookie_id).first()
                if cookie:
                    return {
                        'id': cookie.id,
                        'value': cookie.value,
                        'auto_confirm': cookie.auto_confirm,
                        'remark': cookie.remark or '',
                        'pause_duration': cookie.pause_duration or 10,
                        'username': cookie.username or '',
                        'password': cookie.password or '',
                        'show_browser': cookie.show_browser or 0
                    }
                return None
            except Exception as e:
                logger.error(f"获取Cookie详细信息失败: {e}")
                return None

    def update_auto_confirm(self, cookie_id: str, auto_confirm: int):
        """更新自动确认状态"""
        with self._session() as session:
            try:
                cookie = session.query(CookieAccount).filter_by(id=cookie_id).first()
                if cookie:
                    cookie.auto_confirm = auto_confirm
                    session.commit()
                    logger.debug(f"更新自动确认: {cookie_id} -> {auto_confirm}")
            except Exception as e:
                session.rollback()
                logger.error(f"更新自动确认失败: {e}")
                raise

    def update_cookie_remark(self, cookie_id: str, remark: str):
        """更新Cookie备注"""
        with self._session() as session:
            try:
                cookie = session.query(CookieAccount).filter_by(id=cookie_id).first()
                if cookie:
                    cookie.remark = remark
                    session.commit()
                    logger.debug(f"更新Cookie备注: {cookie_id} -> {remark}")
            except Exception as e:
                session.rollback()
                logger.error(f"更新Cookie备注失败: {e}")
                raise

    def update_cookie_pause_duration(self, cookie_id: str, pause_duration: int):
        """更新Cookie暂停时长"""
        with self._session() as session:
            try:
                cookie = session.query(CookieAccount).filter_by(id=cookie_id).first()
                if cookie:
                    cookie.pause_duration = pause_duration
                    session.commit()
                    logger.debug(f"更新暂停时长: {cookie_id} -> {pause_duration}")
            except Exception as e:
                session.rollback()
                logger.error(f"更新暂停时长失败: {e}")
                raise

    def get_cookie_pause_duration(self, cookie_id: str) -> int:
        """获取Cookie暂停时长"""
        with self._session() as session:
            try:
                cookie = session.query(CookieAccount).filter_by(id=cookie_id).first()
                return cookie.pause_duration if cookie and cookie.pause_duration else 10
            except Exception as e:
                logger.error(f"获取暂停时长失败: {e}")
                return 10

    def update_cookie_account_info(self, cookie_id: str, username: str = None,
                                   password: str = None, show_browser: int = None,
                                   cookie_value: str = None):
        """更新Cookie账号信息（包括cookie值、用户名、密码、显示浏览器设置）"""
        with self._session() as session:
            try:
                cookie = session.query(CookieAccount).filter_by(id=cookie_id).first()
                if cookie:
                    if cookie_value is not None:
                        cookie.value = cookie_value
                    if username is not None:
                        cookie.username = username
                    if password is not None:
                        cookie.password = password
                    if show_browser is not None:
                        cookie.show_browser = show_browser
                    session.commit()
                    logger.debug(f"更新Cookie账号信息: {cookie_id}")
                    return True
                return False
            except Exception as e:
                session.rollback()
                logger.error(f"更新Cookie账号信息失败: {e}")
                return False

    def get_auto_confirm(self, cookie_id: str) -> int:
        """获取自动确认状态"""
        with self._session() as session:
            try:
                cookie = session.query(CookieAccount).filter_by(id=cookie_id).first()
                return cookie.auto_confirm if cookie else 1
            except Exception as e:
                logger.error(f"获取自动确认状态失败: {e}")
                return 1

    # ==================== 关键词操作 ====================

    def save_keywords(self, cookie_id: str, keywords: dict):
        """保存关键词回复（删除旧的，插入新的）"""
        with self._session() as session:
            try:
                # 删除该cookie_id的所有文本关键词
                session.query(Keyword).filter_by(cookie_id=cookie_id, type='text').delete()
                # 插入新关键词
                for keyword, reply in keywords.items():
                    kw = Keyword(cookie_id=cookie_id, keyword=keyword, reply=reply, type='text')
                    session.add(kw)
                session.commit()
                logger.debug(f"保存关键词: {cookie_id}, 数量: {len(keywords)}")
            except Exception as e:
                session.rollback()
                logger.error(f"保存关键词失败: {e}")
                raise

    def save_keywords_with_item_id(self, cookie_id: str, keywords: dict, item_id: str):
        """保存带商品ID的关键词回复"""
        with self._session() as session:
            try:
                # 删除该cookie_id和item_id的关键词
                session.query(Keyword).filter_by(
                    cookie_id=cookie_id, item_id=item_id, type='text'
                ).delete()
                for keyword, reply in keywords.items():
                    kw = Keyword(
                        cookie_id=cookie_id, keyword=keyword, reply=reply,
                        item_id=item_id, type='text'
                    )
                    session.add(kw)
                session.commit()
                logger.debug(f"保存带商品ID的关键词: {cookie_id}, 商品: {item_id}")
            except Exception as e:
                session.rollback()
                logger.error(f"保存带商品ID的关键词失败: {e}")
                raise

    def save_text_keywords_only(self, cookie_id: str, keywords):
        """仅保存文本类型关键词，不影响图片关键词
        
        Args:
            cookie_id: Cookie ID
            keywords: list of tuples [(keyword, reply, item_id), ...] 或 dict {keyword: reply}
        """
        with self._session() as session:
            try:
                # 删除该cookie_id的所有文本类型关键词（含带item_id和不带item_id的）
                session.query(Keyword).filter(
                    Keyword.cookie_id == cookie_id,
                    Keyword.type == 'text'
                ).delete(synchronize_session='fetch')
                
                # 兼容 list of tuples 和 dict 两种格式
                if isinstance(keywords, dict):
                    for keyword, reply in keywords.items():
                        kw = Keyword(cookie_id=cookie_id, keyword=keyword, reply=reply, type='text')
                        session.add(kw)
                else:
                    for item in keywords:
                        keyword, reply = item[0], item[1]
                        item_id = item[2] if len(item) > 2 else None
                        kw = Keyword(
                            cookie_id=cookie_id, keyword=keyword, reply=reply,
                            item_id=item_id, type='text'
                        )
                        session.add(kw)
                
                session.commit()
                logger.debug(f"保存文本关键词: {cookie_id}, 数量: {len(keywords)}")
                return True
            except Exception as e:
                session.rollback()
                logger.error(f"保存文本关键词失败: {e}")
                raise

    def get_keywords(self, cookie_id: str) -> dict:
        """获取指定Cookie的关键词回复（不含商品ID的）"""
        with self._session() as session:
            try:
                keywords = session.query(Keyword).filter(
                    Keyword.cookie_id == cookie_id,
                    or_(Keyword.item_id == None, Keyword.item_id == '')
                ).all()
                return {kw.keyword: kw.reply for kw in keywords}
            except Exception as e:
                logger.error(f"获取关键词失败: {e}")
                return {}

    def get_keywords_with_item_id(self, cookie_id: str, item_id: str) -> dict:
        """获取指定Cookie和商品ID的关键词回复"""
        with self._session() as session:
            try:
                keywords = session.query(Keyword).filter_by(
                    cookie_id=cookie_id, item_id=item_id
                ).all()
                return {kw.keyword: kw.reply for kw in keywords}
            except Exception as e:
                logger.error(f"获取带商品ID的关键词失败: {e}")
                return {}

    def check_keyword_duplicate(self, cookie_id: str, keyword: str) -> bool:
        """检查关键词是否重复"""
        with self._session() as session:
            try:
                count = session.query(Keyword).filter_by(
                    cookie_id=cookie_id, keyword=keyword
                ).count()
                return count > 0
            except Exception as e:
                logger.error(f"检查关键词重复失败: {e}")
                return False

    def save_image_keyword(self, cookie_id: str, keyword: str, image_url: str, item_id: str = None) -> bool:
        """保存图片关键词"""
        with self._session() as session:
            try:
                kw = Keyword(
                    cookie_id=cookie_id, keyword=keyword, reply='[图片回复]',
                    item_id=item_id, type='image', image_url=image_url
                )
                session.add(kw)
                session.commit()
                logger.debug(f"保存图片关键词: {cookie_id}, 关键词: {keyword}")
                return True
            except Exception as e:
                session.rollback()
                logger.error(f"保存图片关键词失败: {e}")
                return False

    def get_keywords_with_type(self, cookie_id: str) -> list:
        """获取指定Cookie的所有关键词（含类型信息）"""
        with self._session() as session:
            try:
                keywords = session.query(Keyword).filter_by(cookie_id=cookie_id).all()
                return [
                    {
                        'keyword': kw.keyword,
                        'reply': kw.reply,
                        'item_id': kw.item_id or '',
                        'type': kw.type or 'text',
                        'image_url': kw.image_url or ''
                    }
                    for kw in keywords
                ]
            except Exception as e:
                logger.error(f"获取关键词（含类型）失败: {e}")
                return []

    def update_keyword_image_url(self, cookie_id: str, keyword: str, new_image_url: str) -> bool:
        """更新关键词的图片URL（用于将本地图片URL更新为CDN URL）"""
        with self._session() as session:
            try:
                kw = session.query(Keyword).filter_by(
                    cookie_id=cookie_id, keyword=keyword, type='image'
                ).first()
                if kw:
                    kw.image_url = new_image_url
                    session.commit()
                    logger.debug(f"更新关键词图片URL: {cookie_id}, {keyword} -> {new_image_url}")
                    return True
                return False
            except Exception as e:
                session.rollback()
                logger.error(f"更新关键词图片URL失败: {e}")
                return False

    def delete_keyword_by_index(self, cookie_id: str, keyword: str, item_id: str = None) -> bool:
        """删除指定关键词"""
        with self._session() as session:
            try:
                query = session.query(Keyword).filter_by(cookie_id=cookie_id, keyword=keyword)
                if item_id:
                    query = query.filter_by(item_id=item_id)
                count = query.delete()
                session.commit()
                logger.debug(f"删除关键词: {cookie_id}, {keyword}, 删除数量: {count}")
                return count > 0
            except Exception as e:
                session.rollback()
                logger.error(f"删除关键词失败: {e}")
                return False

    def get_all_keywords(self) -> Dict[str, dict]:
        """获取所有关键词"""
        with self._session() as session:
            try:
                all_keywords = session.query(Keyword).all()
                result = {}
                for kw in all_keywords:
                    if kw.cookie_id not in result:
                        result[kw.cookie_id] = {}
                    result[kw.cookie_id][kw.keyword] = kw.reply
                return result
            except Exception as e:
                logger.error(f"获取所有关键词失败: {e}")
                return {}

    # ==================== Cookie状态操作 ====================

    def save_cookie_status(self, cookie_id: str, enabled: bool):
        """保存Cookie启用状态"""
        with self._session() as session:
            try:
                status = session.query(CookieStatus).filter_by(cookie_id=cookie_id).first()
                if status:
                    status.enabled = enabled
                else:
                    status = CookieStatus(cookie_id=cookie_id, enabled=enabled)
                    session.add(status)
                session.commit()
                logger.debug(f"保存Cookie状态: {cookie_id} -> {'启用' if enabled else '禁用'}")
            except Exception as e:
                session.rollback()
                logger.error(f"保存Cookie状态失败: {e}")
                raise

    def get_cookie_status(self, cookie_id: str) -> bool:
        """获取Cookie启用状态"""
        with self._session() as session:
            try:
                status = session.query(CookieStatus).filter_by(cookie_id=cookie_id).first()
                return bool(status.enabled) if status else True
            except Exception as e:
                logger.error(f"获取Cookie状态失败: {e}")
                return True

    def get_all_cookie_status(self) -> Dict[str, bool]:
        """获取所有Cookie的启用状态"""
        with self._session() as session:
            try:
                statuses = session.query(CookieStatus).all()
                return {s.cookie_id: bool(s.enabled) for s in statuses}
            except Exception as e:
                logger.error(f"获取所有Cookie状态失败: {e}")
                return {}

    # ==================== AI回复设置操作 ====================

    def save_ai_reply_settings(self, cookie_id: str, ai_enabled: bool = False,
                               model_name: str = None, api_key: str = None,
                               base_url: str = None, max_discount_percent: int = 10,
                               max_discount_amount: int = 100, max_bargain_rounds: int = 3,
                               custom_prompts: str = ''):
        """保存AI回复设置"""
        with self._session() as session:
            try:
                setting = session.query(AIReplySetting).filter_by(cookie_id=cookie_id).first()
                if setting:
                    setting.ai_enabled = ai_enabled
                    if model_name is not None:
                        setting.model_name = model_name
                    if api_key is not None:
                        setting.api_key = api_key
                    if base_url is not None:
                        setting.base_url = base_url
                    setting.max_discount_percent = max_discount_percent
                    setting.max_discount_amount = max_discount_amount
                    setting.max_bargain_rounds = max_bargain_rounds
                    setting.custom_prompts = custom_prompts
                else:
                    setting = AIReplySetting(
                        cookie_id=cookie_id, ai_enabled=ai_enabled,
                        model_name=model_name or DEFAULT_MODEL,
                        api_key=api_key or '',
                        base_url=base_url or DEFAULT_BASE_URL,
                        max_discount_percent=max_discount_percent,
                        max_discount_amount=max_discount_amount,
                        max_bargain_rounds=max_bargain_rounds,
                        custom_prompts=custom_prompts
                    )
                    session.add(setting)
                session.commit()
                logger.debug(f"保存AI回复设置: {cookie_id}")
            except Exception as e:
                session.rollback()
                logger.error(f"保存AI回复设置失败: {e}")
                raise

    def get_ai_reply_settings(self, cookie_id: str) -> dict:
        """获取指定账号的AI回复设置（带系统级设置回退）"""
        with self._session() as session:
            try:
                # 先获取系统级设置作为默认值
                system_model = DEFAULT_MODEL
                system_api_key = ''
                system_base_url = DEFAULT_BASE_URL

                sys_model = session.query(SystemSetting).filter_by(key='ai_model_name').first()
                sys_key = session.query(SystemSetting).filter_by(key='ai_api_key').first()
                sys_url = session.query(SystemSetting).filter_by(key='ai_base_url').first()

                if sys_model:
                    system_model = sys_model.value
                if sys_key:
                    system_api_key = sys_key.value
                if sys_url:
                    system_base_url = sys_url.value

                # 获取账号级设置
                setting = session.query(AIReplySetting).filter_by(cookie_id=cookie_id).first()
                if setting:
                    account_model = setting.model_name
                    account_api_key = setting.api_key
                    account_base_url = setting.base_url

                    # 如果账号值为空或等于硬编码默认值，则使用系统设置
                    use_model = account_model if (account_model and account_model != DEFAULT_MODEL) else system_model
                    use_api_key = account_api_key if account_api_key else system_api_key
                    use_base_url = account_base_url if (account_base_url and account_base_url != DEFAULT_BASE_URL) else system_base_url

                    return {
                        'ai_enabled': bool(setting.ai_enabled),
                        'model_name': use_model,
                        'api_key': use_api_key,
                        'base_url': use_base_url,
                        'max_discount_percent': setting.max_discount_percent,
                        'max_discount_amount': setting.max_discount_amount,
                        'max_bargain_rounds': setting.max_bargain_rounds,
                        'custom_prompts': setting.custom_prompts or ''
                    }
                else:
                    # 账号没有设置，使用系统设置作为默认值
                    return {
                        'ai_enabled': False,
                        'model_name': system_model,
                        'api_key': system_api_key,
                        'base_url': system_base_url,
                        'max_discount_percent': 10,
                        'max_discount_amount': 100,
                        'max_bargain_rounds': 3,
                        'custom_prompts': ''
                    }
            except Exception as e:
                logger.error(f"获取AI回复设置失败: {e}")
                return {
                    'ai_enabled': False,
                    'model_name': 'qwen-plus',
                    'api_key': '',
                    'base_url': 'https://dashscope.aliyuncs.com/compatible-mode/v1',
                    'max_discount_percent': 10,
                    'max_discount_amount': 100,
                    'max_bargain_rounds': 3,
                    'custom_prompts': ''
                }

    def get_all_ai_reply_settings(self) -> Dict[str, dict]:
        """获取所有账号的AI回复设置"""
        with self._session() as session:
            try:
                settings = session.query(AIReplySetting).all()
                result = {}
                for s in settings:
                    result[s.cookie_id] = {
                        'ai_enabled': bool(s.ai_enabled),
                        'model_name': s.model_name,
                        'api_key': s.api_key,
                        'base_url': s.base_url,
                        'max_discount_percent': s.max_discount_percent,
                        'max_discount_amount': s.max_discount_amount,
                        'max_bargain_rounds': s.max_bargain_rounds,
                        'custom_prompts': s.custom_prompts or ''
                    }
                return result
            except Exception as e:
                logger.error(f"获取所有AI回复设置失败: {e}")
                return {}

    # ==================== 默认回复操作 ====================

    def save_default_reply(self, cookie_id: str, enabled: bool, reply_content: str = None,
                           reply_once: bool = False, reply_image_url: str = None):
        """保存默认回复设置"""
        with self._session() as session:
            try:
                dr = session.query(DefaultReply).filter_by(cookie_id=cookie_id).first()
                if dr:
                    dr.enabled = enabled
                    dr.reply_content = reply_content
                    dr.reply_image_url = reply_image_url
                    dr.reply_once = reply_once
                else:
                    dr = DefaultReply(
                        cookie_id=cookie_id, enabled=enabled,
                        reply_content=reply_content, reply_image_url=reply_image_url,
                        reply_once=reply_once
                    )
                    session.add(dr)
                session.commit()
                logger.debug(f"保存默认回复设置: {cookie_id} -> {'启用' if enabled else '禁用'}, 只回复一次: {'是' if reply_once else '否'}, 图片: {reply_image_url}")
            except Exception as e:
                logger.error(f"保存默认回复设置失败: {e}")
                raise

    def get_default_reply(self, cookie_id: str) -> Optional[Dict[str, any]]:
        """获取指定账号的默认回复设置"""
        with self._session() as session:
            try:
                dr = session.query(DefaultReply).filter_by(cookie_id=cookie_id).first()
                if dr:
                    return {
                        'enabled': bool(dr.enabled),
                        'reply_content': dr.reply_content or '',
                        'reply_once': bool(dr.reply_once) if dr.reply_once is not None else False,
                        'reply_image_url': dr.reply_image_url or ''
                    }
                return None
            except Exception as e:
                logger.error(f"获取默认回复设置失败: {e}")
                return None

    def get_all_default_replies(self) -> Dict[str, Dict[str, any]]:
        """获取所有账号的默认回复设置"""
        with self._session() as session:
            try:
                replies = session.query(DefaultReply).all()
                result = {}
                for dr in replies:
                    result[dr.cookie_id] = {
                        'enabled': bool(dr.enabled),
                        'reply_content': dr.reply_content or '',
                        'reply_once': bool(dr.reply_once) if dr.reply_once is not None else False,
                        'reply_image_url': dr.reply_image_url or ''
                    }
                return result
            except Exception as e:
                logger.error(f"获取所有默认回复设置失败: {e}")
                return {}

    def add_default_reply_record(self, cookie_id: str, chat_id: str):
        """记录已回复的chat_id"""
        with self._session() as session:
            try:
                existing = session.query(DefaultReplyRecord).filter_by(
                    cookie_id=cookie_id, chat_id=chat_id
                ).first()
                if not existing:
                    record = DefaultReplyRecord(cookie_id=cookie_id, chat_id=chat_id)
                    session.add(record)
                    session.commit()
                logger.debug(f"记录默认回复: {cookie_id} -> {chat_id}")
            except Exception as e:
                logger.error(f"记录默认回复失败: {e}")

    def has_default_reply_record(self, cookie_id: str, chat_id: str) -> bool:
        """检查是否已经回复过该chat_id"""
        with self._session() as session:
            try:
                record = session.query(DefaultReplyRecord).filter_by(
                    cookie_id=cookie_id, chat_id=chat_id
                ).first()
                return record is not None
            except Exception as e:
                logger.error(f"检查默认回复记录失败: {e}")
                return False

    def clear_default_reply_records(self, cookie_id: str):
        """清空指定账号的默认回复记录"""
        with self._session() as session:
            try:
                session.query(DefaultReplyRecord).filter_by(cookie_id=cookie_id).delete()
                session.commit()
                logger.debug(f"清空默认回复记录: {cookie_id}")
            except Exception as e:
                logger.error(f"清空默认回复记录失败: {e}")

    def delete_default_reply(self, cookie_id: str) -> bool:
        """删除指定账号的默认回复设置"""
        with self._session() as session:
            try:
                count = session.query(DefaultReply).filter_by(cookie_id=cookie_id).delete()
                session.commit()
                logger.debug(f"删除默认回复设置: {cookie_id}")
                return count > 0
            except Exception as e:
                session.rollback()
                logger.error(f"删除默认回复设置失败: {e}")
                return False

    def update_default_reply_image_url(self, cookie_id: str, new_image_url: str) -> bool:
        """更新默认回复的图片URL（用于将本地图片URL更新为CDN URL）"""
        with self._session() as session:
            try:
                dr = session.query(DefaultReply).filter_by(cookie_id=cookie_id).first()
                if dr:
                    dr.reply_image_url = new_image_url
                    session.commit()
                    logger.debug(f"更新默认回复图片URL: {cookie_id} -> {new_image_url}")
                    return True
                return False
            except Exception as e:
                session.rollback()
                logger.error(f"更新默认回复图片URL失败: {e}")
                return False

    # ==================== 通知渠道操作 ====================

    def create_notification_channel(self, name: str, channel_type: str, config: str, user_id: int = None) -> int:
        """创建通知渠道"""
        with self._session() as session:
            try:
                channel = NotificationChannel(
                    name=name, type=channel_type, config=config, user_id=user_id
                )
                session.add(channel)
                session.commit()
                channel_id = channel.id
                logger.debug(f"创建通知渠道: {name} (ID: {channel_id})")
                return channel_id
            except Exception as e:
                session.rollback()
                logger.error(f"创建通知渠道失败: {e}")
                raise

    def get_notification_channels(self, user_id: int = None) -> List[Dict[str, any]]:
        """获取所有通知渠道"""
        with self._session() as session:
            try:
                query = session.query(NotificationChannel)
                if user_id is not None:
                    query = query.filter_by(user_id=user_id)
                query = query.order_by(NotificationChannel.created_at.desc())
                channels = query.all()
                return [
                    {
                        'id': ch.id, 'name': ch.name, 'type': ch.type,
                        'config': ch.config, 'enabled': bool(ch.enabled),
                        'created_at': str(ch.created_at) if ch.created_at else None,
                        'updated_at': str(ch.updated_at) if ch.updated_at else None
                    }
                    for ch in channels
                ]
            except Exception as e:
                logger.error(f"获取通知渠道失败: {e}")
                return []

    def get_notification_channel(self, channel_id: int) -> Optional[Dict[str, any]]:
        """获取指定通知渠道"""
        with self._session() as session:
            try:
                ch = session.query(NotificationChannel).filter_by(id=channel_id).first()
                if ch:
                    return {
                        'id': ch.id, 'name': ch.name, 'type': ch.type,
                        'config': ch.config, 'enabled': bool(ch.enabled),
                        'created_at': str(ch.created_at) if ch.created_at else None,
                        'updated_at': str(ch.updated_at) if ch.updated_at else None
                    }
                return None
            except Exception as e:
                logger.error(f"获取通知渠道失败: {e}")
                return None

    def update_notification_channel(self, channel_id: int, name: str, config: str, enabled: bool = True) -> bool:
        """更新通知渠道"""
        with self._session() as session:
            try:
                ch = session.query(NotificationChannel).filter_by(id=channel_id).first()
                if ch:
                    ch.name = name
                    ch.config = config
                    ch.enabled = enabled
                    session.commit()
                    logger.debug(f"更新通知渠道: {channel_id}")
                    return True
                return False
            except Exception as e:
                session.rollback()
                logger.error(f"更新通知渠道失败: {e}")
                return False

    def delete_notification_channel(self, channel_id: int) -> bool:
        """删除通知渠道"""
        with self._session() as session:
            try:
                count = session.query(NotificationChannel).filter_by(id=channel_id).delete()
                session.commit()
                logger.debug(f"删除通知渠道: {channel_id}")
                return count > 0
            except Exception as e:
                session.rollback()
                logger.error(f"删除通知渠道失败: {e}")
                return False

    # ==================== 消息通知配置操作 ====================

    def set_message_notification(self, cookie_id: str, channel_id: int, enabled: bool = True) -> bool:
        """设置账号的消息通知"""
        with self._session() as session:
            try:
                mn = session.query(MessageNotification).filter_by(
                    cookie_id=cookie_id, channel_id=channel_id
                ).first()
                if mn:
                    mn.enabled = enabled
                else:
                    mn = MessageNotification(
                        cookie_id=cookie_id, channel_id=channel_id, enabled=enabled
                    )
                    session.add(mn)
                session.commit()
                logger.debug(f"设置消息通知: {cookie_id} -> {channel_id}")
                return True
            except Exception as e:
                session.rollback()
                logger.error(f"设置消息通知失败: {e}")
                return False

    def get_account_notifications(self, cookie_id: str) -> List[Dict[str, any]]:
        """获取账号的通知配置"""
        with self._session() as session:
            try:
                results = session.query(
                    MessageNotification, NotificationChannel
                ).join(
                    NotificationChannel, MessageNotification.channel_id == NotificationChannel.id
                ).filter(
                    MessageNotification.cookie_id == cookie_id,
                    NotificationChannel.enabled == True
                ).order_by(MessageNotification.id).all()

                notifications = []
                for mn, nc in results:
                    notifications.append({
                        'id': mn.id, 'channel_id': mn.channel_id,
                        'enabled': bool(mn.enabled), 'channel_name': nc.name,
                        'channel_type': nc.type, 'channel_config': nc.config
                    })
                return notifications
            except Exception as e:
                logger.error(f"获取账号通知配置失败: {e}")
                return []

    def get_all_message_notifications(self) -> Dict[str, List[Dict[str, any]]]:
        """获取所有账号的通知配置"""
        with self._session() as session:
            try:
                results = session.query(
                    MessageNotification, NotificationChannel
                ).join(
                    NotificationChannel, MessageNotification.channel_id == NotificationChannel.id
                ).filter(
                    NotificationChannel.enabled == True
                ).order_by(MessageNotification.cookie_id, MessageNotification.id).all()

                result = {}
                for mn, nc in results:
                    if mn.cookie_id not in result:
                        result[mn.cookie_id] = []
                    result[mn.cookie_id].append({
                        'id': mn.id, 'channel_id': mn.channel_id,
                        'enabled': bool(mn.enabled), 'channel_name': nc.name,
                        'channel_type': nc.type, 'channel_config': nc.config
                    })
                return result
            except Exception as e:
                logger.error(f"获取所有消息通知配置失败: {e}")
                return {}

    def delete_message_notification(self, notification_id: int) -> bool:
        """删除消息通知配置"""
        with self._session() as session:
            try:
                count = session.query(MessageNotification).filter_by(id=notification_id).delete()
                session.commit()
                logger.debug(f"删除消息通知配置: {notification_id}")
                return count > 0
            except Exception as e:
                session.rollback()
                logger.error(f"删除消息通知配置失败: {e}")
                return False

    def delete_account_notifications(self, cookie_id: str) -> bool:
        """删除账号的所有消息通知配置"""
        with self._session() as session:
            try:
                count = session.query(MessageNotification).filter_by(cookie_id=cookie_id).delete()
                session.commit()
                logger.debug(f"删除账号通知配置: {cookie_id}")
                return count > 0
            except Exception as e:
                session.rollback()
                logger.error(f"删除账号通知配置失败: {e}")
                return False

    # ==================== 备份和恢复操作 ====================

    def export_backup(self, user_id: int = None) -> Dict[str, any]:
        """导出系统备份数据（支持用户隔离）"""
        with self._session() as session:
            try:
                backup_data = {
                    'version': '1.0',
                    'timestamp': time.time(),
                    'user_id': user_id,
                    'data': {}
                }

                def _table_to_backup(model_cls):
                    """将ORM查询结果转为备份格式"""
                    mapper = sa_inspect(model_cls)
                    columns = [c.key for c in mapper.column_attrs]
                    rows = session.query(model_cls).all()
                    return {
                        'columns': columns,
                        'rows': [[getattr(row, col) for col in columns] for row in rows]
                    }

                def _filtered_table_to_backup(model_cls, cookie_ids):
                    """将带cookie_id过滤的ORM查询结果转为备份格式"""
                    mapper = sa_inspect(model_cls)
                    columns = [c.key for c in mapper.column_attrs]
                    rows = session.query(model_cls).filter(
                        model_cls.cookie_id.in_(cookie_ids)
                    ).all()
                    return {
                        'columns': columns,
                        'rows': [[getattr(row, col) for col in columns] for row in rows]
                    }

                if user_id is not None:
                    # 用户级备份：只备份该用户的数据
                    backup_data['data']['cookies'] = _table_to_backup(CookieAccount)
                    cookies = session.query(CookieAccount).filter_by(user_id=user_id).all()
                    backup_data['data']['cookies'] = {
                        'columns': [c.key for c in sa_inspect(CookieAccount).column_attrs],
                        'rows': [[getattr(c, col.key) for col in sa_inspect(CookieAccount).column_attrs] for c in cookies]
                    }
                    user_cookie_ids = [c.id for c in cookies]

                    if user_cookie_ids:
                        related_models = {
                            'keywords': Keyword, 'cookie_status': CookieStatus,
                            'default_replies': DefaultReply, 'message_notifications': MessageNotification,
                            'item_info': ItemInfo, 'ai_reply_settings': AIReplySetting,
                            'ai_conversations': AIConversation
                        }
                        for table_name, model_cls in related_models.items():
                            backup_data['data'][table_name] = _filtered_table_to_backup(model_cls, user_cookie_ids)
                else:
                    # 系统级备份：备份所有数据
                    backup_tables = {
                        'cookies': CookieAccount, 'keywords': Keyword,
                        'cookie_status': CookieStatus, 'cards': Card,
                        'delivery_rules': DeliveryRule, 'default_replies': DefaultReply,
                        'notification_channels': NotificationChannel,
                        'message_notifications': MessageNotification,
                        'system_settings': SystemSetting, 'item_info': ItemInfo,
                        'ai_reply_settings': AIReplySetting,
                        'ai_conversations': AIConversation, 'ai_item_cache': AIItemCache
                    }
                    for table_name, model_cls in backup_tables.items():
                        backup_data['data'][table_name] = _table_to_backup(model_cls)

                logger.info(f"导出备份成功，用户ID: {user_id}")
                return backup_data

            except Exception as e:
                logger.error(f"导出备份失败: {e}")
                raise

    def import_backup(self, backup_data: Dict[str, any], user_id: int = None) -> bool:
        """导入系统备份数据（支持用户隔离）"""
        with self._session() as session:
            try:
                # 验证备份数据格式
                if not isinstance(backup_data, dict) or 'data' not in backup_data:
                    raise ValueError("备份数据格式无效")

                allowed_tables = {
                    'cookies', 'keywords', 'cookie_status', 'cards',
                    'delivery_rules', 'default_replies', 'notification_channels',
                    'message_notifications', 'system_settings', 'item_info',
                    'ai_reply_settings', 'ai_conversations', 'ai_item_cache'
                }

                if user_id is not None:
                    # 用户级导入：只清空该用户的数据
                    user_cookies = session.query(CookieAccount).filter_by(user_id=user_id).all()
                    user_cookie_ids = [c.id for c in user_cookies]

                    if user_cookie_ids:
                        for model_cls in [MessageNotification, DefaultReply, ItemInfo,
                                          CookieStatus, Keyword, AIConversation, AIReplySetting]:
                            session.query(model_cls).filter(
                                model_cls.cookie_id.in_(user_cookie_ids)
                            ).delete(synchronize_session='fetch')
                        session.query(CookieAccount).filter_by(user_id=user_id).delete()
                else:
                    # 系统级导入：清空所有数据（除了用户和管理员密码）
                    delete_order = [
                        MessageNotification, NotificationChannel, DefaultReply,
                        DeliveryRule, Card, ItemInfo, CookieStatus, Keyword,
                        AIConversation, AIReplySetting, AIItemCache, CookieAccount
                    ]
                    for model_cls in delete_order:
                        session.query(model_cls).delete()
                    # 清空系统设置（保留管理员密码）
                    session.query(SystemSetting).filter(
                        SystemSetting.key != 'admin_password_hash'
                    ).delete(synchronize_session='fetch')

                # 导入数据
                data = backup_data['data']
                for table_name, table_data in data.items():
                    if table_name not in allowed_tables:
                        continue

                    model_cls = self.TABLE_MODEL_MAP.get(table_name)
                    if not model_cls:
                        continue

                    columns = table_data.get('columns', [])
                    rows = table_data.get('rows', [])
                    if not rows:
                        continue

                    # 如果是用户级导入，更新cookies的user_id
                    if user_id is not None and table_name == 'cookies':
                        user_id_idx = columns.index('user_id') if 'user_id' in columns else -1
                        if user_id_idx >= 0:
                            for row in rows:
                                row[user_id_idx] = user_id

                    for row in rows:
                        row_dict = dict(zip(columns, row))
                        if table_name == 'system_settings' and row_dict.get('key') == 'admin_password_hash':
                            continue
                        obj = model_cls(**row_dict)
                        session.merge(obj)

                session.commit()
                logger.info("导入备份成功")
                return True

            except Exception as e:
                session.rollback()
                logger.error(f"导入备份失败: {e}")
                return False

    # ==================== 系统设置操作 ====================

    def get_system_setting(self, key: str) -> Optional[str]:
        """获取系统设置"""
        with self._session() as session:
            try:
                setting = session.query(SystemSetting).filter_by(key=key).first()
                return setting.value if setting else None
            except Exception as e:
                logger.error(f"获取系统设置失败: {e}")
                return None

    def set_system_setting(self, key: str, value: str, description: str = None) -> bool:
        """设置系统设置"""
        with self._session() as session:
            try:
                setting = session.query(SystemSetting).filter_by(key=key).first()
                if setting:
                    setting.value = value
                    if description is not None:
                        setting.description = description
                else:
                    setting = SystemSetting(key=key, value=value, description=description)
                    session.add(setting)
                session.commit()
                logger.debug(f"设置系统设置: {key}")
                return True
            except Exception as e:
                session.rollback()
                logger.error(f"设置系统设置失败: {e}")
                return False

    def get_all_system_settings(self) -> Dict[str, str]:
        """获取所有系统设置"""
        with self._session() as session:
            try:
                settings = session.query(SystemSetting).all()
                return {s.key: s.value for s in settings}
            except Exception as e:
                logger.error(f"获取所有系统设置失败: {e}")
                return {}

    # ==================== 用户管理方法 ====================

    def create_user(self, username: str, email: str, password: str) -> bool:
        """创建新用户"""
        with self._session() as session:
            try:
                password_hash = bcrypt_hash.hash(password)
                user = User(username=username, email=email, password_hash=password_hash)
                session.add(user)
                session.commit()
                logger.info(f"创建用户成功: {username} ({email})")
                return True
            except IntegrityError as e:
                session.rollback()
                logger.error(f"创建用户失败，用户名或邮箱已存在: {e}")
                return False
            except Exception as e:
                session.rollback()
                logger.error(f"创建用户失败: {e}")
                return False

    def get_user_by_username(self, username: str) -> Optional[Dict[str, Any]]:
        """根据用户名获取用户信息"""
        with self._session() as session:
            try:
                user = session.query(User).filter_by(username=username).first()
                if user:
                    return {
                        'id': user.id, 'username': user.username, 'email': user.email,
                        'password_hash': user.password_hash, 'is_active': user.is_active,
                        'created_at': str(user.created_at) if user.created_at else None,
                        'updated_at': str(user.updated_at) if user.updated_at else None
                    }
                return None
            except Exception as e:
                logger.error(f"获取用户信息失败: {e}")
                return None

    def get_user_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        """根据邮箱获取用户信息"""
        with self._session() as session:
            try:
                user = session.query(User).filter_by(email=email).first()
                if user:
                    return {
                        'id': user.id, 'username': user.username, 'email': user.email,
                        'password_hash': user.password_hash, 'is_active': user.is_active,
                        'created_at': str(user.created_at) if user.created_at else None,
                        'updated_at': str(user.updated_at) if user.updated_at else None
                    }
                return None
            except Exception as e:
                logger.error(f"获取用户信息失败: {e}")
                return None

    def verify_user_password(self, username: str, password: str) -> bool:
        """验证用户密码（支持bcrypt和旧版SHA256自动升级）"""
        user = self.get_user_by_username(username)
        if not user:
            return False
        if not user['is_active']:
            return False

        stored_hash = user['password_hash']

        # 尝试bcrypt验证
        if stored_hash.startswith('$2'):
            return bcrypt_hash.verify(password, stored_hash)

        # 回退到旧版SHA256验证，验证成功后自动升级为bcrypt
        sha256_hash = hashlib.sha256(password.encode()).hexdigest()
        if stored_hash == sha256_hash:
            self.update_user_password(username, password)
            logger.info(f"用户 {username} 密码哈希已自动升级为bcrypt")
            return True

        return False

    def update_user_password(self, username: str, new_password: str) -> bool:
        """更新用户密码"""
        with self._session() as session:
            try:
                user = session.query(User).filter_by(username=username).first()
                if user:
                    user.password_hash = bcrypt_hash.hash(new_password)
                    session.commit()
                    logger.info(f"用户 {username} 密码更新成功")
                    return True
                else:
                    logger.warning(f"用户 {username} 不存在，密码更新失败")
                    return False
            except Exception as e:
                session.rollback()
                logger.error(f"更新用户密码失败: {e}")
                return False

    def generate_verification_code(self) -> str:
        """生成6位数字验证码"""
        return ''.join(random.choices(string.digits, k=6))

    def generate_captcha(self) -> Tuple[str, str]:
        """生成图形验证码
        返回: (验证码文本, base64编码的图片)
        """
        try:
            chars = string.ascii_uppercase + string.digits
            captcha_text = ''.join(random.choices(chars, k=4))

            width, height = 120, 40
            image = Image.new('RGB', (width, height), color='white')
            draw = ImageDraw.Draw(image)

            try:
                font = ImageFont.truetype("arial.ttf", 20)
            except:
                try:
                    font = ImageFont.truetype("C:/Windows/Fonts/arial.ttf", 20)
                except:
                    font = ImageFont.load_default()

            for i, char in enumerate(captcha_text):
                color = (random.randint(0, 100), random.randint(0, 100), random.randint(0, 100))
                x = 20 + i * 20 + random.randint(-3, 3)
                y = 8 + random.randint(-3, 3)
                draw.text((x, y), char, font=font, fill=color)

            for _ in range(3):
                start = (random.randint(0, width), random.randint(0, height))
                end = (random.randint(0, width), random.randint(0, height))
                draw.line([start, end], fill=(random.randint(100, 200), random.randint(100, 200), random.randint(100, 200)), width=1)

            for _ in range(20):
                x = random.randint(0, width)
                y = random.randint(0, height)
                draw.point((x, y), fill=(random.randint(0, 255), random.randint(0, 255), random.randint(0, 255)))

            buffer = io.BytesIO()
            image.save(buffer, format='PNG')
            img_base64 = base64.b64encode(buffer.getvalue()).decode()

            return captcha_text, f"data:image/png;base64,{img_base64}"

        except Exception as e:
            logger.error(f"生成图形验证码失败: {e}")
            simple_code = ''.join(random.choices(string.digits, k=4))
            return simple_code, ""

    def save_captcha(self, session_id: str, captcha_text: str, expires_minutes: int = 5) -> bool:
        """保存图形验证码"""
        with self._session() as session:
            try:
                expires_at = time.time() + (expires_minutes * 60)
                session.query(CaptchaCode).filter_by(session_id=session_id).delete()
                captcha = CaptchaCode(session_id=session_id, code=captcha_text.upper(), expires_at=expires_at)
                session.add(captcha)
                session.commit()
                logger.debug(f"保存图形验证码成功: {session_id}")
                return True
            except Exception as e:
                session.rollback()
                logger.error(f"保存图形验证码失败: {e}")
                return False

    def verify_captcha(self, session_id: str, user_input: str) -> bool:
        """验证图形验证码"""
        with self._session() as session:
            try:
                current_time = time.time()
                captcha = session.query(CaptchaCode).filter(
                    CaptchaCode.session_id == session_id,
                    CaptchaCode.code == user_input.upper(),
                    CaptchaCode.expires_at > current_time
                ).order_by(CaptchaCode.created_at.desc()).first()

                if captcha:
                    session.delete(captcha)
                    session.commit()
                    logger.debug(f"图形验证码验证成功: {session_id}")
                    return True
                else:
                    logger.warning(f"图形验证码验证失败: {session_id} - {user_input}")
                    return False
            except Exception as e:
                logger.error(f"验证图形验证码失败: {e}")
                return False

    def save_verification_code(self, email: str, code: str, code_type: str = 'register', expires_minutes: int = 10) -> bool:
        """保存邮箱验证码"""
        with self._session() as session:
            try:
                expires_at = time.time() + (expires_minutes * 60)
                verification = EmailVerification(
                    email=email, code=code, type=code_type, expires_at=expires_at
                )
                session.add(verification)
                session.commit()
                logger.info(f"保存验证码成功: {email} ({code_type})")
                return True
            except Exception as e:
                session.rollback()
                logger.error(f"保存验证码失败: {e}")
                return False

    def verify_email_code(self, email: str, code: str, code_type: str = 'register') -> bool:
        """验证邮箱验证码"""
        with self._session() as session:
            try:
                current_time = time.time()
                verification = session.query(EmailVerification).filter(
                    EmailVerification.email == email,
                    EmailVerification.code == code,
                    EmailVerification.type == code_type,
                    EmailVerification.expires_at > current_time,
                    EmailVerification.used == False
                ).order_by(EmailVerification.created_at.desc()).first()

                if verification:
                    verification.used = True
                    session.commit()
                    logger.info(f"验证码验证成功: {email} ({code_type})")
                    return True
                else:
                    logger.warning(f"验证码验证失败: {email} - {code} ({code_type})")
                    return False
            except Exception as e:
                logger.error(f"验证邮箱验证码失败: {e}")
                return False

    async def send_verification_email(self, email: str, code: str) -> bool:
        """发送验证码邮件（支持SMTP和API两种方式）"""
        try:
            subject = "闲鱼自动回复系统 - 邮箱验证码"
            text_content = f"""【闲鱼自动回复系统】邮箱验证码

您好！

感谢您使用闲鱼自动回复系统。为了确保账户安全，请使用以下验证码完成邮箱验证：

验证码：{code}

重要提醒：
• 验证码有效期为 10 分钟，请及时使用
• 请勿将验证码分享给任何人
• 如非本人操作，请忽略此邮件
• 系统不会主动索要您的验证码

如果您在使用过程中遇到任何问题，请联系我们的技术支持团队。
感谢您选择闲鱼自动回复系统！

---
此邮件由系统自动发送，请勿直接回复
© 2025 闲鱼自动回复系统"""

            try:
                smtp_server = self.get_system_setting('smtp_server') or ''
                smtp_port = int(self.get_system_setting('smtp_port') or 0)
                smtp_user = self.get_system_setting('smtp_user') or ''
                smtp_password = self.get_system_setting('smtp_password') or ''
                smtp_from = (self.get_system_setting('smtp_from') or '').strip() or smtp_user
                smtp_use_tls = (self.get_system_setting('smtp_use_tls') or 'true').lower() == 'true'
                smtp_use_ssl = (self.get_system_setting('smtp_use_ssl') or 'false').lower() == 'true'
            except Exception as e:
                logger.error(f"读取SMTP系统设置失败: {e}")
                return False

            if smtp_server and smtp_port and smtp_user and smtp_password:
                logger.info(f"使用SMTP方式发送验证码邮件: {email}")
                return await self._send_email_via_smtp(email, subject, text_content,
                                                     smtp_server, smtp_port, smtp_user,
                                                     smtp_password, smtp_from, smtp_use_tls, smtp_use_ssl)
            else:
                logger.error(f"SMTP配置不完整，无法发送验证码邮件: {email}，请在系统设置中配置SMTP")
                return False

        except Exception as e:
            logger.error(f"发送验证码邮件异常: {e}")
            return False

    async def _send_email_via_smtp(self, email: str, subject: str, text_content: str,
                                 smtp_server: str, smtp_port: int, smtp_user: str,
                                 smtp_password: str, smtp_from: str, smtp_use_tls: bool, smtp_use_ssl: bool) -> bool:
        """使用SMTP方式发送邮件"""
        try:
            import smtplib
            from email.mime.text import MIMEText
            from email.mime.multipart import MIMEMultipart

            msg = MIMEMultipart()
            msg['Subject'] = subject
            msg['From'] = smtp_from
            msg['To'] = email
            msg.attach(MIMEText(text_content, 'plain', 'utf-8'))

            if smtp_use_ssl:
                server = smtplib.SMTP_SSL(smtp_server, smtp_port)
            else:
                server = smtplib.SMTP(smtp_server, smtp_port)

            server.ehlo()
            if smtp_use_tls and not smtp_use_ssl:
                server.starttls()
                server.ehlo()

            server.login(smtp_user, smtp_password)
            server.sendmail(smtp_user, [email], msg.as_string())
            server.quit()

            logger.info(f"验证码邮件发送成功(SMTP): {email}")
            return True
        except Exception as e:
            logger.error(f"SMTP发送验证码邮件失败: {e}")
            return False

    # ==================== 卡券管理方法 ====================

    def create_card(self, name: str, card_type: str, api_config=None,
                   text_content: str = None, data_content: str = None, image_url: str = None,
                   description: str = None, enabled: bool = True, delay_seconds: int = 0,
                   is_multi_spec: bool = False, spec_name: str = None, spec_value: str = None,
                   user_id: int = None):
        """创建新卡券（支持多规格）"""
        with self._session() as session:
            try:
                if is_multi_spec:
                    if not spec_name or not spec_value:
                        raise ValueError("多规格卡券必须提供规格名称和规格值")
                    count = session.query(Card).filter_by(
                        name=name, spec_name=spec_name, spec_value=spec_value, user_id=user_id
                    ).count()
                    if count > 0:
                        raise ValueError(f"卡券已存在：{name} - {spec_name}:{spec_value}")
                else:
                    count = session.query(Card).filter(
                        Card.name == name,
                        or_(Card.is_multi_spec == False, Card.is_multi_spec == None),
                        Card.user_id == user_id
                    ).count()
                    if count > 0:
                        raise ValueError(f"卡券名称已存在：{name}")

                api_config_str = None
                if api_config is not None:
                    if isinstance(api_config, dict):
                        api_config_str = json.dumps(api_config)
                    else:
                        api_config_str = str(api_config)

                card = Card(
                    name=name, type=card_type, api_config=api_config_str,
                    text_content=text_content, data_content=data_content, image_url=image_url,
                    description=description, enabled=enabled, delay_seconds=delay_seconds,
                    is_multi_spec=is_multi_spec, spec_name=spec_name, spec_value=spec_value,
                    user_id=user_id
                )
                session.add(card)
                session.commit()
                card_id = card.id

                if is_multi_spec:
                    logger.info(f"创建多规格卡券成功: {name} - {spec_name}:{spec_value} (ID: {card_id})")
                else:
                    logger.info(f"创建卡券成功: {name} (ID: {card_id})")
                return card_id
            except Exception as e:
                session.rollback()
                logger.error(f"创建卡券失败: {e}")
                raise

    def _card_to_dict(self, card) -> dict:
        """将Card ORM对象转为字典"""
        api_config = card.api_config
        if api_config:
            try:
                api_config = json.loads(api_config)
            except (json.JSONDecodeError, TypeError):
                pass
        return {
            'id': card.id, 'name': card.name, 'type': card.type,
            'api_config': api_config, 'text_content': card.text_content,
            'data_content': card.data_content, 'image_url': card.image_url,
            'description': card.description, 'enabled': bool(card.enabled),
            'delay_seconds': card.delay_seconds or 0,
            'is_multi_spec': bool(card.is_multi_spec) if card.is_multi_spec is not None else False,
            'spec_name': card.spec_name, 'spec_value': card.spec_value,
            'created_at': str(card.created_at) if card.created_at else None,
            'updated_at': str(card.updated_at) if card.updated_at else None
        }

    def get_all_cards(self, user_id: int = None):
        """获取所有卡券（支持用户隔离）"""
        with self._session() as session:
            try:
                query = session.query(Card)
                if user_id is not None:
                    query = query.filter_by(user_id=user_id)
                query = query.order_by(Card.created_at.desc())
                return [self._card_to_dict(c) for c in query.all()]
            except Exception as e:
                logger.error(f"获取卡券列表失败: {e}")
                return []

    def get_card_by_id(self, card_id: int, user_id: int = None):
        """根据ID获取卡券（支持用户隔离）"""
        with self._session() as session:
            try:
                query = session.query(Card).filter_by(id=card_id)
                if user_id is not None:
                    query = query.filter_by(user_id=user_id)
                card = query.first()
                return self._card_to_dict(card) if card else None
            except Exception as e:
                logger.error(f"获取卡券失败: {e}")
                return None

    def update_card(self, card_id: int, name: str = None, card_type: str = None,
                   api_config=None, text_content: str = None, data_content: str = None,
                   image_url: str = None, description: str = None, enabled: bool = None,
                   delay_seconds: int = None, is_multi_spec: bool = None, spec_name: str = None,
                   spec_value: str = None):
        """更新卡券"""
        with self._session() as session:
            try:
                card = session.query(Card).filter_by(id=card_id).first()
                if not card:
                    return False

                if name is not None: card.name = name
                if card_type is not None: card.type = card_type
                if api_config is not None:
                    card.api_config = json.dumps(api_config) if isinstance(api_config, dict) else str(api_config)
                if text_content is not None: card.text_content = text_content
                if data_content is not None: card.data_content = data_content
                if image_url is not None: card.image_url = image_url
                if description is not None: card.description = description
                if enabled is not None: card.enabled = enabled
                if delay_seconds is not None: card.delay_seconds = delay_seconds
                if is_multi_spec is not None: card.is_multi_spec = is_multi_spec
                if spec_name is not None: card.spec_name = spec_name
                if spec_value is not None: card.spec_value = spec_value

                session.commit()
                logger.info(f"更新卡券成功: ID {card_id}")
                return True
            except Exception as e:
                session.rollback()
                logger.error(f"更新卡券失败: {e}")
                raise

    def update_card_image_url(self, card_id: int, new_image_url: str) -> bool:
        """更新卡券的图片URL"""
        with self._session() as session:
            try:
                card = session.query(Card).filter_by(id=card_id, type='image').first()
                if card:
                    card.image_url = new_image_url
                    session.commit()
                    logger.info(f"卡券图片URL更新成功: 卡券ID: {card_id}, 新URL: {new_image_url}")
                    return True
                else:
                    logger.warning(f"未找到匹配的图片卡券: 卡券ID: {card_id}")
                    return False
            except Exception as e:
                session.rollback()
                logger.error(f"更新卡券图片URL失败: {e}")
                return False

    def delete_card(self, card_id: int):
        """删除卡券"""
        with self._session() as session:
            try:
                count = session.query(Card).filter_by(id=card_id).delete()
                if count > 0:
                    session.commit()
                    logger.info(f"删除卡券成功: ID {card_id}")
                    return True
                return False
            except Exception as e:
                session.rollback()
                logger.error(f"删除卡券失败: {e}")
                raise

    # ==================== 自动发货规则方法 ====================

    def create_delivery_rule(self, keyword: str, card_id: int, delivery_count: int = 1,
                           enabled: bool = True, description: str = None, user_id: int = None):
        """创建发货规则"""
        with self._session() as session:
            try:
                rule = DeliveryRule(
                    keyword=keyword, card_id=card_id, delivery_count=delivery_count,
                    enabled=enabled, description=description, user_id=user_id
                )
                session.add(rule)
                session.commit()
                rule_id = rule.id
                logger.info(f"创建发货规则成功: {keyword} -> 卡券ID {card_id} (规则ID: {rule_id})")
                return rule_id
            except Exception as e:
                session.rollback()
                logger.error(f"创建发货规则失败: {e}")
                raise

    def get_all_delivery_rules(self, user_id: int = None):
        """获取所有发货规则"""
        with self._session() as session:
            try:
                query = session.query(DeliveryRule, Card).outerjoin(
                    Card, DeliveryRule.card_id == Card.id
                )
                if user_id is not None:
                    query = query.filter(DeliveryRule.user_id == user_id)
                query = query.order_by(DeliveryRule.created_at.desc())

                rules = []
                for dr, c in query.all():
                    rules.append({
                        'id': dr.id, 'keyword': dr.keyword, 'card_id': dr.card_id,
                        'delivery_count': dr.delivery_count, 'enabled': bool(dr.enabled),
                        'description': dr.description, 'delivery_times': dr.delivery_times,
                        'created_at': str(dr.created_at) if dr.created_at else None,
                        'updated_at': str(dr.updated_at) if dr.updated_at else None,
                        'card_name': c.name if c else None,
                        'card_type': c.type if c else None,
                        'is_multi_spec': bool(c.is_multi_spec) if c and c.is_multi_spec is not None else False,
                        'spec_name': c.spec_name if c else None,
                        'spec_value': c.spec_value if c else None
                    })
                return rules
            except Exception as e:
                logger.error(f"获取发货规则列表失败: {e}")
                return []

    def get_delivery_rules_by_keyword(self, keyword: str):
        """根据关键字获取匹配的发货规则"""
        with self._session() as session:
            try:
                # 使用text()处理复杂LIKE匹配，兼容SQLite和PostgreSQL
                sql = text('''
                SELECT dr.id, dr.keyword, dr.card_id, dr.delivery_count, dr.enabled,
                       dr.description, dr.delivery_times,
                       c.name as card_name, c.type as card_type, c.api_config,
                       c.text_content, c.data_content, c.image_url, c.enabled as card_enabled,
                       c.description as card_description, c.delay_seconds as card_delay_seconds,
                       c.is_multi_spec, c.spec_name, c.spec_value
                FROM delivery_rules dr
                LEFT JOIN cards c ON dr.card_id = c.id
                WHERE dr.enabled = true AND c.enabled = true
                AND (:kw LIKE '%' || dr.keyword || '%' OR dr.keyword LIKE '%' || :kw || '%')
                ORDER BY
                    CASE
                        WHEN :kw LIKE '%' || dr.keyword || '%' THEN LENGTH(dr.keyword)
                        ELSE LENGTH(dr.keyword) / 2
                    END DESC,
                    dr.id ASC
                ''')
                rows = session.execute(sql, {'kw': keyword}).fetchall()

                rules = []
                for row in rows:
                    api_config = row[9]
                    if api_config:
                        try:
                            api_config = json.loads(api_config)
                        except (json.JSONDecodeError, TypeError):
                            pass
                    rules.append({
                        'id': row[0], 'keyword': row[1], 'card_id': row[2],
                        'delivery_count': row[3], 'enabled': bool(row[4]),
                        'description': row[5], 'delivery_times': row[6],
                        'card_name': row[7], 'card_type': row[8],
                        'api_config': api_config, 'text_content': row[10],
                        'data_content': row[11], 'image_url': row[12],
                        'card_enabled': bool(row[13]),
                        'card_description': row[14],
                        'card_delay_seconds': row[15] or 0,
                        'is_multi_spec': bool(row[16]) if row[16] is not None else False,
                        'spec_name': row[17], 'spec_value': row[18]
                    })
                return rules
            except Exception as e:
                logger.error(f"根据关键字获取发货规则失败: {e}")
                return []

    def get_delivery_rule_by_id(self, rule_id: int, user_id: int = None):
        """根据ID获取发货规则（支持用户隔离）"""
        with self._session() as session:
            try:
                query = session.query(DeliveryRule, Card).outerjoin(
                    Card, DeliveryRule.card_id == Card.id
                ).filter(DeliveryRule.id == rule_id)
                if user_id is not None:
                    query = query.filter(DeliveryRule.user_id == user_id)
                result = query.first()
                if result:
                    dr, c = result
                    return {
                        'id': dr.id, 'keyword': dr.keyword, 'card_id': dr.card_id,
                        'delivery_count': dr.delivery_count, 'enabled': bool(dr.enabled),
                        'description': dr.description, 'delivery_times': dr.delivery_times,
                        'created_at': str(dr.created_at) if dr.created_at else None,
                        'updated_at': str(dr.updated_at) if dr.updated_at else None,
                        'card_name': c.name if c else None,
                        'card_type': c.type if c else None
                    }
                return None
            except Exception as e:
                logger.error(f"获取发货规则失败: {e}")
                return None

    def update_delivery_rule(self, rule_id: int, keyword: str = None, card_id: int = None,
                           delivery_count: int = None, enabled: bool = None,
                           description: str = None, user_id: int = None):
        """更新发货规则（支持用户隔离）"""
        with self._session() as session:
            try:
                query = session.query(DeliveryRule).filter_by(id=rule_id)
                if user_id is not None:
                    query = query.filter_by(user_id=user_id)
                rule = query.first()
                if not rule:
                    return False

                if keyword is not None: rule.keyword = keyword
                if card_id is not None: rule.card_id = card_id
                if delivery_count is not None: rule.delivery_count = delivery_count
                if enabled is not None: rule.enabled = enabled
                if description is not None: rule.description = description

                session.commit()
                logger.info(f"更新发货规则成功: ID {rule_id}")
                return True
            except Exception as e:
                session.rollback()
                logger.error(f"更新发货规则失败: {e}")
                raise

    def increment_delivery_times(self, rule_id: int):
        """增加发货次数"""
        with self._session() as session:
            try:
                rule = session.query(DeliveryRule).filter_by(id=rule_id).first()
                if rule:
                    rule.delivery_times = (rule.delivery_times or 0) + 1
                    session.commit()
                    logger.debug(f"发货规则 {rule_id} 发货次数已增加")
            except Exception as e:
                logger.error(f"更新发货次数失败: {e}")

    def get_delivery_rules_by_keyword_and_spec(self, keyword: str, spec_name: str = None, spec_value: str = None):
        """根据关键字和规格信息获取匹配的发货规则（支持多规格）"""
        with self._session() as session:
            try:
                # 优先匹配：卡券名称+规格名称+规格值
                if spec_name and spec_value:
                    sql = text('''
                    SELECT dr.id, dr.keyword, dr.card_id, dr.delivery_count, dr.enabled,
                           dr.description, dr.delivery_times,
                           c.name as card_name, c.type as card_type, c.api_config,
                           c.text_content, c.data_content, c.enabled as card_enabled,
                           c.description as card_description, c.delay_seconds as card_delay_seconds,
                           c.is_multi_spec, c.spec_name, c.spec_value
                    FROM delivery_rules dr
                    LEFT JOIN cards c ON dr.card_id = c.id
                    WHERE dr.enabled = true AND c.enabled = true
                    AND (:kw LIKE '%' || dr.keyword || '%' OR dr.keyword LIKE '%' || :kw || '%')
                    AND c.is_multi_spec = true AND c.spec_name = :sn AND c.spec_value = :sv
                    ORDER BY
                        CASE WHEN :kw LIKE '%' || dr.keyword || '%' THEN LENGTH(dr.keyword)
                        ELSE LENGTH(dr.keyword) / 2 END DESC,
                        dr.delivery_times ASC
                    ''')
                    rows = session.execute(sql, {'kw': keyword, 'sn': spec_name, 'sv': spec_value}).fetchall()

                    rules = self._parse_delivery_rules_rows(rows)
                    if rules:
                        logger.info(f"找到多规格匹配规则: {keyword} - {spec_name}:{spec_value}")
                        return rules

                # 兜底匹配：仅卡券名称（非多规格）
                sql = text('''
                SELECT dr.id, dr.keyword, dr.card_id, dr.delivery_count, dr.enabled,
                       dr.description, dr.delivery_times,
                       c.name as card_name, c.type as card_type, c.api_config,
                       c.text_content, c.data_content, c.enabled as card_enabled,
                       c.description as card_description, c.delay_seconds as card_delay_seconds,
                       c.is_multi_spec, c.spec_name, c.spec_value
                FROM delivery_rules dr
                LEFT JOIN cards c ON dr.card_id = c.id
                WHERE dr.enabled = true AND c.enabled = true
                AND (:kw LIKE '%' || dr.keyword || '%' OR dr.keyword LIKE '%' || :kw || '%')
                AND (c.is_multi_spec = false OR c.is_multi_spec IS NULL)
                ORDER BY
                    CASE WHEN :kw LIKE '%' || dr.keyword || '%' THEN LENGTH(dr.keyword)
                    ELSE LENGTH(dr.keyword) / 2 END DESC,
                    dr.delivery_times ASC
                ''')
                rows = session.execute(sql, {'kw': keyword}).fetchall()
                rules = self._parse_delivery_rules_rows(rows)

                if rules:
                    logger.info(f"找到兜底匹配规则: {keyword}")
                else:
                    logger.info(f"未找到匹配规则: {keyword}")
                return rules

            except Exception as e:
                logger.error(f"获取发货规则失败: {e}")
                return []

    def _parse_delivery_rules_rows(self, rows) -> list:
        """解析发货规则查询结果行"""
        rules = []
        for row in rows:
            api_config = row[9]
            if api_config:
                try:
                    api_config = json.loads(api_config)
                except (json.JSONDecodeError, TypeError):
                    pass
            rules.append({
                'id': row[0], 'keyword': row[1], 'card_id': row[2],
                'delivery_count': row[3], 'enabled': bool(row[4]),
                'description': row[5], 'delivery_times': row[6] or 0,
                'card_name': row[7], 'card_type': row[8], 'api_config': api_config,
                'text_content': row[10], 'data_content': row[11],
                'card_enabled': bool(row[12]),
                'card_description': row[13],
                'card_delay_seconds': row[14] or 0,
                'is_multi_spec': bool(row[15]) if row[15] is not None else False,
                'spec_name': row[16], 'spec_value': row[17]
            })
        return rules

    def delete_delivery_rule(self, rule_id: int, user_id: int = None):
        """删除发货规则（支持用户隔离）"""
        with self._session() as session:
            try:
                query = session.query(DeliveryRule).filter_by(id=rule_id)
                if user_id is not None:
                    query = query.filter_by(user_id=user_id)
                count = query.delete()
                if count > 0:
                    session.commit()
                    logger.info(f"删除发货规则成功: ID {rule_id} (用户ID: {user_id})")
                    return True
                return False
            except Exception as e:
                session.rollback()
                logger.error(f"删除发货规则失败: {e}")
                raise

    def consume_batch_data(self, card_id: int):
        """消费批量数据的第一条记录（线程安全）"""
        with self._session() as session:
            try:
                card = session.query(Card).filter_by(id=card_id, type='data').first()
                if not card or not card.data_content:
                    logger.warning(f"卡券 {card_id} 没有批量数据")
                    return None

                lines = [line.strip() for line in card.data_content.split('\n') if line.strip()]
                if not lines:
                    logger.warning(f"卡券 {card_id} 批量数据为空")
                    return None

                first_line = lines[0]
                remaining_lines = lines[1:]
                card.data_content = '\n'.join(remaining_lines)
                session.commit()

                logger.info(f"消费批量数据成功: 卡券ID={card_id}, 剩余={len(remaining_lines)}条")
                return first_line
            except Exception as e:
                session.rollback()
                logger.error(f"消费批量数据失败: {e}")
                return None

    # ==================== 商品信息管理 ====================

    def save_item_basic_info(self, cookie_id: str, item_id: str, item_title: str = None,
                            item_description: str = None, item_category: str = None,
                            item_price: str = None, item_detail: str = None) -> bool:
        """保存或更新商品基本信息，使用原子操作避免并发问题"""
        try:
            with self._session() as session:
                existing = session.query(ItemInfo).filter_by(
                    cookie_id=cookie_id, item_id=item_id
                ).first()

                if not existing:
                    # 新增
                    info = ItemInfo(
                        cookie_id=cookie_id, item_id=item_id,
                        item_title=item_title or '', item_description=item_description or '',
                        item_category=item_category or '', item_price=item_price or '',
                        item_detail=item_detail or ''
                    )
                    session.add(info)
                    session.commit()
                    logger.info(f"新增商品基本信息: {item_id} - {item_title}")
                    return True

                # 更新：只更新非空字段且不覆盖现有非空值
                changed = False
                if item_title and not existing.item_title:
                    existing.item_title = item_title; changed = True
                if item_description and not existing.item_description:
                    existing.item_description = item_description; changed = True
                if item_category and not existing.item_category:
                    existing.item_category = item_category; changed = True
                if item_price and not existing.item_price:
                    existing.item_price = item_price; changed = True
                if item_detail and not (existing.item_detail and existing.item_detail.strip()):
                    existing.item_detail = item_detail; changed = True

                if changed:
                    logger.info(f"更新商品基本信息: {item_id} - {item_title}")
                else:
                    logger.debug(f"商品信息无需更新: {item_id}")

                session.commit()
                return True

        except Exception as e:
            logger.error(f"保存商品基本信息失败: {e}")
            return False

    def save_item_info(self, cookie_id: str, item_id: str, item_data=None) -> bool:
        """保存或更新商品信息"""
        try:
            if not item_data:
                logger.debug(f"跳过保存商品信息：缺少商品详情数据 - {item_id}")
                return False

            if isinstance(item_data, dict):
                title = item_data.get('title', '').strip()
                if not title:
                    logger.debug(f"跳过保存商品信息：缺少商品标题 - {item_id}")
                    return False

            if isinstance(item_data, str) and not item_data.strip():
                logger.debug(f"跳过保存商品信息：商品详情为空 - {item_id}")
                return False

            with self._session() as session:
                existing = session.query(ItemInfo).filter_by(
                    cookie_id=cookie_id, item_id=item_id
                ).first()

                if existing:
                    if item_data is not None and item_data:
                        if isinstance(item_data, str):
                            existing.item_detail = item_data
                        else:
                            existing.item_title = item_data.get('title', '')
                            existing.item_description = item_data.get('description', '')
                            existing.item_category = item_data.get('category', '')
                            existing.item_price = item_data.get('price', '')
                            existing.item_detail = json.dumps(item_data, ensure_ascii=False)
                        logger.info(f"更新商品信息（覆盖）: {item_id}")
                    else:
                        logger.debug(f"商品信息已存在，无新数据，跳过更新: {item_id}")
                        return True
                else:
                    if isinstance(item_data, str):
                        info = ItemInfo(cookie_id=cookie_id, item_id=item_id, item_detail=item_data)
                    else:
                        info = ItemInfo(
                            cookie_id=cookie_id, item_id=item_id,
                            item_title=item_data.get('title', '') if item_data else '',
                            item_description=item_data.get('description', '') if item_data else '',
                            item_category=item_data.get('category', '') if item_data else '',
                            item_price=item_data.get('price', '') if item_data else '',
                            item_detail=json.dumps(item_data, ensure_ascii=False) if item_data else ''
                        )
                    session.add(info)
                    logger.info(f"新增商品信息: {item_id}")

                session.commit()
                return True

        except Exception as e:
            logger.error(f"保存商品信息失败: {e}")
            return False

    def get_item_info(self, cookie_id: str, item_id: str) -> Optional[Dict]:
        """获取商品信息"""
        try:
            with self._session() as session:
                info = session.query(ItemInfo).filter_by(
                    cookie_id=cookie_id, item_id=item_id
                ).first()
                if info:
                    item_dict = {
                        'id': info.id, 'cookie_id': info.cookie_id, 'item_id': info.item_id,
                        'item_title': info.item_title, 'item_description': info.item_description,
                        'item_category': info.item_category, 'item_price': info.item_price,
                        'item_detail': info.item_detail,
                        'is_multi_spec': info.is_multi_spec,
                        'multi_quantity_delivery': info.multi_quantity_delivery,
                        'created_at': str(info.created_at) if info.created_at else None,
                        'updated_at': str(info.updated_at) if info.updated_at else None
                    }
                    if info.item_detail:
                        try:
                            item_dict['item_detail_parsed'] = json.loads(info.item_detail)
                        except:
                            item_dict['item_detail_parsed'] = {}
                    logger.info(f"item_info: {item_dict}")
                    return item_dict
                return None
        except Exception as e:
            logger.error(f"获取商品信息失败: {e}")
            return None

    def update_item_multi_spec_status(self, cookie_id: str, item_id: str, is_multi_spec: bool) -> bool:
        """更新商品的多规格状态"""
        try:
            with self._session() as session:
                info = session.query(ItemInfo).filter_by(cookie_id=cookie_id, item_id=item_id).first()
                if info:
                    info.is_multi_spec = is_multi_spec
                    session.commit()
                    logger.info(f"更新商品多规格状态成功: {item_id} -> {is_multi_spec}")
                    return True
                logger.warning(f"商品不存在，无法更新多规格状态: {item_id}")
                return False
        except Exception as e:
            logger.error(f"更新商品多规格状态失败: {e}")
            return False

    def get_item_multi_spec_status(self, cookie_id: str, item_id: str) -> bool:
        """获取商品的多规格状态"""
        try:
            with self._session() as session:
                info = session.query(ItemInfo).filter_by(cookie_id=cookie_id, item_id=item_id).first()
                return bool(info.is_multi_spec) if info and info.is_multi_spec is not None else False
        except Exception as e:
            logger.error(f"获取商品多规格状态失败: {e}")
            return False

    def update_item_multi_quantity_delivery_status(self, cookie_id: str, item_id: str, multi_quantity_delivery: bool) -> bool:
        """更新商品的多数量发货状态"""
        try:
            with self._session() as session:
                info = session.query(ItemInfo).filter_by(cookie_id=cookie_id, item_id=item_id).first()
                if info:
                    info.multi_quantity_delivery = multi_quantity_delivery
                    session.commit()
                    logger.info(f"更新商品多数量发货状态成功: {item_id} -> {multi_quantity_delivery}")
                    return True
                logger.warning(f"未找到要更新的商品: {item_id}")
                return False
        except Exception as e:
            logger.error(f"更新商品多数量发货状态失败: {e}")
            return False

    def get_item_multi_quantity_delivery_status(self, cookie_id: str, item_id: str) -> bool:
        """获取商品的多数量发货状态"""
        try:
            with self._session() as session:
                info = session.query(ItemInfo).filter_by(cookie_id=cookie_id, item_id=item_id).first()
                return bool(info.multi_quantity_delivery) if info and info.multi_quantity_delivery is not None else False
        except Exception as e:
            logger.error(f"获取商品多数量发货状态失败: {e}")
            return False

    def _item_info_to_dict(self, info) -> dict:
        """将ItemInfo ORM对象转为字典"""
        item_dict = {
            'id': info.id, 'cookie_id': info.cookie_id, 'item_id': info.item_id,
            'item_title': info.item_title, 'item_description': info.item_description,
            'item_category': info.item_category, 'item_price': info.item_price,
            'item_detail': info.item_detail,
            'is_multi_spec': info.is_multi_spec,
            'multi_quantity_delivery': info.multi_quantity_delivery,
            'created_at': str(info.created_at) if info.created_at else None,
            'updated_at': str(info.updated_at) if info.updated_at else None
        }
        if info.item_detail:
            try:
                item_dict['item_detail_parsed'] = json.loads(info.item_detail)
            except:
                item_dict['item_detail_parsed'] = {}
        return item_dict

    def get_items_by_cookie(self, cookie_id: str) -> List[Dict]:
        """获取指定Cookie的所有商品信息"""
        try:
            with self._session() as session:
                items = session.query(ItemInfo).filter_by(
                    cookie_id=cookie_id
                ).order_by(ItemInfo.updated_at.desc()).all()
                return [self._item_info_to_dict(i) for i in items]
        except Exception as e:
            logger.error(f"获取Cookie商品信息失败: {e}")
            return []

    def get_all_items(self) -> List[Dict]:
        """获取所有商品信息"""
        try:
            with self._session() as session:
                items = session.query(ItemInfo).order_by(ItemInfo.updated_at.desc()).all()
                return [self._item_info_to_dict(i) for i in items]
        except Exception as e:
            logger.error(f"获取所有商品信息失败: {e}")
            return []

    def update_item_detail(self, cookie_id: str, item_id: str, item_detail: str) -> bool:
        """更新商品详情（不覆盖商品标题等基本信息）"""
        try:
            with self._session() as session:
                info = session.query(ItemInfo).filter_by(cookie_id=cookie_id, item_id=item_id).first()
                if info:
                    info.item_detail = item_detail
                    session.commit()
                    logger.info(f"更新商品详情成功: {item_id}")
                    return True
                logger.warning(f"未找到要更新的商品: {item_id}")
                return False
        except Exception as e:
            logger.error(f"更新商品详情失败: {e}")
            return False

    def update_item_title_only(self, cookie_id: str, item_id: str, item_title: str) -> bool:
        """仅更新商品标题（并发安全）"""
        try:
            with self._session() as session:
                info = session.query(ItemInfo).filter_by(cookie_id=cookie_id, item_id=item_id).first()
                if info:
                    info.item_title = item_title
                    session.commit()
                else:
                    info = ItemInfo(cookie_id=cookie_id, item_id=item_id, item_title=item_title)
                    session.add(info)
                    session.commit()
                logger.info(f"更新商品标题成功: {item_id} - {item_title}")
                return True
        except Exception as e:
            logger.error(f"更新商品标题失败: {e}")
            return False

    def batch_save_item_basic_info(self, items_data: list) -> int:
        """批量保存商品基本信息（并发安全）"""
        if not items_data:
            return 0
        success_count = 0
        try:
            with self._session() as session:
                for item_data in items_data:
                    try:
                        cookie_id = item_data.get('cookie_id')
                        item_id = item_data.get('item_id')
                        item_title = item_data.get('item_title', '')
                        if not cookie_id or not item_id:
                            continue
                        if not item_title or not item_title.strip():
                            logger.debug(f"跳过批量保存商品信息：缺少商品标题 - {item_id}")
                            continue

                        existing = session.query(ItemInfo).filter_by(
                            cookie_id=cookie_id, item_id=item_id
                        ).first()
                        if not existing:
                            info = ItemInfo(
                                cookie_id=cookie_id, item_id=item_id,
                                item_title=item_title,
                                item_description=item_data.get('item_description', ''),
                                item_category=item_data.get('item_category', ''),
                                item_price=item_data.get('item_price', ''),
                                item_detail=item_data.get('item_detail', '')
                            )
                            session.add(info)
                        else:
                            # 条件更新：只更新空字段
                            if not existing.item_title and item_title:
                                existing.item_title = item_title
                            if not existing.item_description and item_data.get('item_description'):
                                existing.item_description = item_data['item_description']
                            if not existing.item_category and item_data.get('item_category'):
                                existing.item_category = item_data['item_category']
                            if not existing.item_price and item_data.get('item_price'):
                                existing.item_price = item_data['item_price']
                            if not (existing.item_detail and existing.item_detail.strip()) and item_data.get('item_detail'):
                                existing.item_detail = item_data['item_detail']
                        success_count += 1
                    except Exception as item_e:
                        logger.warning(f"批量保存单个商品失败 {item_data.get('item_id', 'unknown')}: {item_e}")
                        continue

                session.commit()
                logger.info(f"批量保存商品信息完成: {success_count}/{len(items_data)} 个商品")
                return success_count
        except Exception as e:
            logger.error(f"批量保存商品信息失败: {e}")
            return success_count

    def delete_item_info(self, cookie_id: str, item_id: str) -> bool:
        """删除商品信息"""
        try:
            with self._session() as session:
                count = session.query(ItemInfo).filter_by(
                    cookie_id=cookie_id, item_id=item_id
                ).delete()
                if count > 0:
                    session.commit()
                    logger.info(f"删除商品信息成功: {cookie_id} - {item_id}")
                    return True
                logger.warning(f"未找到要删除的商品信息: {cookie_id} - {item_id}")
                return False
        except Exception as e:
            logger.error(f"删除商品信息失败: {e}")
            return False

    def batch_delete_item_info(self, items_to_delete: list) -> int:
        """批量删除商品信息"""
        if not items_to_delete:
            return 0
        success_count = 0
        try:
            with self._session() as session:
                for item_data in items_to_delete:
                    try:
                        cookie_id = item_data.get('cookie_id')
                        item_id = item_data.get('item_id')
                        if not cookie_id or not item_id:
                            continue
                        count = session.query(ItemInfo).filter_by(
                            cookie_id=cookie_id, item_id=item_id
                        ).delete()
                        if count > 0:
                            success_count += 1
                            logger.debug(f"删除商品信息: {cookie_id} - {item_id}")
                    except Exception as item_e:
                        logger.warning(f"删除单个商品失败 {item_data.get('item_id', 'unknown')}: {item_e}")
                        continue

                session.commit()
                logger.info(f"批量删除商品信息完成: {success_count}/{len(items_to_delete)} 个商品")
                return success_count
        except Exception as e:
            logger.error(f"批量删除商品信息失败: {e}")
            return success_count

    # ==================== 用户设置管理方法 ====================

    def get_user_settings(self, user_id: int):
        """获取用户的所有设置"""
        with self._session() as session:
            try:
                settings = session.query(UserSetting).filter_by(user_id=user_id).order_by(UserSetting.key).all()
                result = {}
                for s in settings:
                    result[s.key] = {
                        'value': s.value, 'description': s.description,
                        'updated_at': str(s.updated_at) if s.updated_at else None
                    }
                return result
            except Exception as e:
                logger.error(f"获取用户设置失败: {e}")
                return {}

    def get_user_setting(self, user_id: int, key: str):
        """获取用户的特定设置"""
        with self._session() as session:
            try:
                s = session.query(UserSetting).filter_by(user_id=user_id, key=key).first()
                if s:
                    return {
                        'key': key, 'value': s.value, 'description': s.description,
                        'updated_at': str(s.updated_at) if s.updated_at else None
                    }
                return None
            except Exception as e:
                logger.error(f"获取用户设置失败: {e}")
                return None

    def set_user_setting(self, user_id: int, key: str, value: str, description: str = None):
        """设置用户配置"""
        with self._session() as session:
            try:
                s = session.query(UserSetting).filter_by(user_id=user_id, key=key).first()
                if s:
                    s.value = value
                    if description is not None:
                        s.description = description
                else:
                    s = UserSetting(user_id=user_id, key=key, value=value, description=description)
                    session.add(s)
                session.commit()
                logger.info(f"用户设置更新成功: user_id={user_id}, key={key}")
                return True
            except Exception as e:
                session.rollback()
                logger.error(f"设置用户配置失败: {e}")
                return False

    # ==================== 管理员专用方法 ====================

    def get_all_users(self):
        """获取所有用户信息（管理员专用）"""
        with self._session() as session:
            try:
                users = session.query(User).order_by(User.created_at.desc()).all()
                return [
                    {
                        'id': u.id, 'username': u.username, 'email': u.email,
                        'created_at': str(u.created_at) if u.created_at else None,
                        'updated_at': str(u.updated_at) if u.updated_at else None
                    }
                    for u in users
                ]
            except Exception as e:
                logger.error(f"获取所有用户失败: {e}")
                return []

    def get_user_by_id(self, user_id: int):
        """根据ID获取用户信息"""
        with self._session() as session:
            try:
                user = session.query(User).filter_by(id=user_id).first()
                if user:
                    return {
                        'id': user.id, 'username': user.username, 'email': user.email,
                        'created_at': str(user.created_at) if user.created_at else None,
                        'updated_at': str(user.updated_at) if user.updated_at else None
                    }
                return None
            except Exception as e:
                logger.error(f"获取用户信息失败: {e}")
                return None

    def delete_user_and_data(self, user_id: int):
        """删除用户及其所有相关数据"""
        with self._session() as session:
            try:
                # 获取用户的cookie_ids
                user_cookies = session.query(CookieAccount).filter_by(user_id=user_id).all()
                user_cookie_ids = [c.id for c in user_cookies]

                # 删除用户设置
                session.query(UserSetting).filter_by(user_id=user_id).delete()
                # 删除用户的卡券
                session.query(Card).filter_by(user_id=user_id).delete()
                # 删除用户的发货规则
                session.query(DeliveryRule).filter_by(user_id=user_id).delete()
                # 删除用户的通知渠道
                session.query(NotificationChannel).filter_by(user_id=user_id).delete()

                if user_cookie_ids:
                    # 删除cookie相关数据
                    for model_cls in [Keyword, DefaultReply, AIReplySetting,
                                      MessageNotification, CookieStatus]:
                        session.query(model_cls).filter(
                            model_cls.cookie_id.in_(user_cookie_ids)
                        ).delete(synchronize_session='fetch')

                # 删除用户的Cookie
                session.query(CookieAccount).filter_by(user_id=user_id).delete()
                # 删除用户本身
                session.query(User).filter_by(id=user_id).delete()

                session.commit()
                logger.info(f"用户及相关数据删除成功: user_id={user_id}")
                return True
            except Exception as e:
                session.rollback()
                logger.error(f"删除用户及相关数据失败: {e}")
                return False

    def get_table_data(self, table_name: str):
        """获取指定表的所有数据"""
        with self._session() as session:
            try:
                self._validate_table_name(table_name)
                model_cls = self.TABLE_MODEL_MAP.get(table_name)
                if not model_cls:
                    return [], []

                mapper = sa_inspect(model_cls)
                columns = [c.key for c in mapper.column_attrs]
                rows = session.query(model_cls).all()

                data = []
                for row in rows:
                    row_dict = {col: getattr(row, col) for col in columns}
                    data.append(row_dict)

                return data, columns
            except Exception as e:
                logger.error(f"获取表数据失败: {table_name} - {e}")
                return [], []

    # ==================== 订单管理 ====================

    def insert_or_update_order(self, order_id: str, item_id: str = None, buyer_id: str = None,
                              spec_name: str = None, spec_value: str = None, quantity: str = None,
                              amount: str = None, order_status: str = None, cookie_id: str = None,
                              is_bargain: bool = None):
        """插入或更新订单信息"""
        with self._session() as session:
            try:
                # 检查cookie_id是否存在
                if cookie_id:
                    cookie_exists = session.query(CookieAccount).filter_by(id=cookie_id).first()
                    if not cookie_exists:
                        logger.warning(f"Cookie ID {cookie_id} 不存在于cookies表中，拒绝插入订单 {order_id}")
                        return False

                existing = session.query(Order).filter_by(order_id=order_id).first()

                if existing:
                    if item_id is not None: existing.item_id = item_id
                    if buyer_id is not None: existing.buyer_id = buyer_id
                    if spec_name is not None: existing.spec_name = spec_name
                    if spec_value is not None: existing.spec_value = spec_value
                    if quantity is not None: existing.quantity = quantity
                    if amount is not None: existing.amount = amount
                    if order_status is not None: existing.order_status = order_status
                    if cookie_id is not None: existing.cookie_id = cookie_id
                    if is_bargain is not None: existing.is_bargain = 1 if is_bargain else 0
                    logger.info(f"更新订单信息: {order_id}")
                else:
                    order = Order(
                        order_id=order_id, item_id=item_id, buyer_id=buyer_id,
                        spec_name=spec_name, spec_value=spec_value, quantity=quantity,
                        amount=amount, order_status=order_status or 'unknown',
                        cookie_id=cookie_id, is_bargain=1 if is_bargain else 0
                    )
                    session.add(order)
                    logger.info(f"插入新订单: {order_id}")

                session.commit()
                return True
            except Exception as e:
                session.rollback()
                logger.error(f"插入或更新订单失败: {order_id} - {e}")
                return False

    def get_order_by_id(self, order_id: str):
        """根据订单ID获取订单信息"""
        with self._session() as session:
            try:
                o = session.query(Order).filter_by(order_id=order_id).first()
                if o:
                    return {
                        'id': o.order_id, 'order_id': o.order_id,
                        'item_id': o.item_id, 'buyer_id': o.buyer_id,
                        'spec_name': o.spec_name, 'spec_value': o.spec_value,
                        'quantity': o.quantity, 'amount': o.amount,
                        'status': o.order_status, 'cookie_id': o.cookie_id,
                        'is_bargain': bool(o.is_bargain) if o.is_bargain is not None else False,
                        'created_at': str(o.created_at) if o.created_at else None,
                        'updated_at': str(o.updated_at) if o.updated_at else None
                    }
                return None
            except Exception as e:
                logger.error(f"获取订单信息失败: {order_id} - {e}")
                return None

    def delete_order(self, order_id: str):
        """删除订单"""
        with self._session() as session:
            try:
                count = session.query(Order).filter_by(order_id=order_id).delete()
                if count > 0:
                    session.commit()
                    logger.info(f"删除订单成功: {order_id}")
                    return True
                return False
            except Exception as e:
                session.rollback()
                logger.error(f"删除订单失败: {order_id} - {e}")
                return False

    def get_orders_by_cookie(self, cookie_id: str, limit: int = 100):
        """根据Cookie ID获取订单列表"""
        with self._session() as session:
            try:
                orders = session.query(Order).filter_by(
                    cookie_id=cookie_id
                ).order_by(Order.created_at.desc()).limit(limit).all()
                return [
                    {
                        'id': o.order_id, 'order_id': o.order_id,
                        'item_id': o.item_id, 'buyer_id': o.buyer_id,
                        'spec_name': o.spec_name, 'spec_value': o.spec_value,
                        'quantity': o.quantity, 'amount': o.amount,
                        'status': o.order_status,
                        'is_bargain': bool(o.is_bargain) if o.is_bargain is not None else False,
                        'created_at': str(o.created_at) if o.created_at else None,
                        'updated_at': str(o.updated_at) if o.updated_at else None
                    }
                    for o in orders
                ]
            except Exception as e:
                logger.error(f"获取Cookie订单列表失败: {cookie_id} - {e}")
                return []

    def get_all_orders(self, limit: int = 1000):
        """获取所有订单列表"""
        with self._session() as session:
            try:
                orders = session.query(Order).order_by(
                    Order.created_at.desc()
                ).limit(limit).all()
                return [
                    {
                        'id': o.order_id, 'order_id': o.order_id,
                        'item_id': o.item_id, 'buyer_id': o.buyer_id,
                        'spec_name': o.spec_name, 'spec_value': o.spec_value,
                        'quantity': o.quantity, 'amount': o.amount,
                        'status': o.order_status, 'cookie_id': o.cookie_id,
                        'is_bargain': bool(o.is_bargain) if o.is_bargain is not None else False,
                        'created_at': str(o.created_at) if o.created_at else None,
                        'updated_at': str(o.updated_at) if o.updated_at else None
                    }
                    for o in orders
                ]
            except Exception as e:
                logger.error(f"获取所有订单列表失败: {e}")
                return []

    def delete_table_record(self, table_name: str, record_id: str):
        """删除指定表的指定记录"""
        with self._session() as session:
            try:
                self._validate_table_name(table_name)
                model_cls = self.TABLE_MODEL_MAP.get(table_name)
                if not model_cls:
                    return False

                # 确定主键字段
                primary_key_map = {
                    'orders': 'order_id', 'system_settings': 'key',
                    'cookie_status': 'cookie_id', 'ai_reply_settings': 'cookie_id',
                    'default_replies': 'cookie_id', 'ai_item_cache': 'item_id',
                }
                pk_field = primary_key_map.get(table_name, 'id')
                pk_col = getattr(model_cls, pk_field, None)

                if pk_col is None:
                    return False

                count = session.query(model_cls).filter(pk_col == record_id).delete()
                if count > 0:
                    session.commit()
                    logger.info(f"删除表记录成功: {table_name}.{record_id}")
                    return True
                logger.warning(f"删除表记录失败，记录不存在: {table_name}.{record_id}")
                return False
            except Exception as e:
                session.rollback()
                logger.error(f"删除表记录失败: {table_name}.{record_id} - {e}")
                return False

    def clear_table_data(self, table_name: str):
        """清空指定表的所有数据"""
        with self._session() as session:
            try:
                self._validate_table_name(table_name)
                model_cls = self.TABLE_MODEL_MAP.get(table_name)
                if not model_cls:
                    return False

                session.query(model_cls).delete()
                session.commit()
                logger.info(f"清空表数据成功: {table_name}")
                return True
            except Exception as e:
                session.rollback()
                logger.error(f"清空表数据失败: {table_name} - {e}")
                return False

    # ==================== 商品回复管理 ====================

    def get_item_replay(self, item_id: str) -> Optional[Dict[str, Any]]:
        """根据商品ID获取商品回复信息"""
        try:
            with self._session() as session:
                replay = session.query(ItemReplay).filter_by(item_id=item_id).first()
                if replay:
                    return {'reply_content': replay.reply_content or ''}
                return None
        except Exception as e:
            logger.error(f"获取商品回复失败: {e}")
            return None

    def get_item_reply(self, cookie_id: str, item_id: str) -> Optional[Dict[str, Any]]:
        """获取指定账号和商品的回复内容"""
        try:
            with self._session() as session:
                replay = session.query(ItemReplay).filter_by(
                    cookie_id=cookie_id, item_id=item_id
                ).first()
                if replay:
                    return {
                        'reply_content': replay.reply_content or '',
                        'created_at': str(replay.created_at) if replay.created_at else None,
                        'updated_at': str(replay.updated_at) if replay.updated_at else None
                    }
                return None
        except Exception as e:
            logger.error(f"获取指定商品回复失败: {e}")
            return None

    def update_item_reply(self, cookie_id: str, item_id: str, reply_content: str) -> bool:
        """更新指定cookie和item的回复内容"""
        try:
            with self._session() as session:
                replay = session.query(ItemReplay).filter_by(
                    cookie_id=cookie_id, item_id=item_id
                ).first()
                if replay:
                    replay.reply_content = reply_content
                else:
                    replay = ItemReplay(
                        item_id=item_id, cookie_id=cookie_id, reply_content=reply_content
                    )
                    session.add(replay)
                session.commit()
            return True
        except Exception as e:
            logger.error(f"更新商品回复失败: {e}")
            return False

    def get_itemReplays_by_cookie(self, cookie_id: str) -> List[Dict]:
        """获取指定Cookie的所有商品回复信息"""
        try:
            with self._session() as session:
                results = session.query(
                    ItemReplay, ItemInfo
                ).outerjoin(
                    ItemInfo, ItemReplay.item_id == ItemInfo.item_id
                ).filter(
                    ItemReplay.cookie_id == cookie_id
                ).order_by(ItemReplay.updated_at.desc()).all()

                items = []
                for r, i in results:
                    items.append({
                        'id': str(r.id),
                        'item_id': r.item_id, 'cookie_id': r.cookie_id,
                        'reply': r.reply_content,
                        'reply_content': r.reply_content,
                        'title': i.item_title if i else None,
                        'item_title': i.item_title if i else None,
                        'item_detail': i.item_detail if i else None,
                        'created_at': str(r.created_at) if r.created_at else None,
                        'updated_at': str(r.updated_at) if r.updated_at else None,
                    })
                return items
        except Exception as e:
            logger.error(f"获取Cookie商品信息失败: {e}")
            return []

    def delete_item_reply(self, cookie_id: str, item_id: str) -> bool:
        """删除指定cookie_id和item_id的商品回复"""
        try:
            with self._session() as session:
                count = session.query(ItemReplay).filter_by(
                    cookie_id=cookie_id, item_id=item_id
                ).delete()
                session.commit()
                return count > 0
        except Exception as e:
            logger.error(f"删除商品回复失败: {e}")
            return False

    def batch_delete_item_replies(self, items: List[Dict[str, str]]) -> Dict[str, int]:
        """批量删除商品回复"""
        success_count = 0
        failed_count = 0
        try:
            with self._session() as session:
                for item in items:
                    cookie_id = item.get('cookie_id')
                    item_id = item.get('item_id')
                    if not cookie_id or not item_id:
                        failed_count += 1
                        continue
                    count = session.query(ItemReplay).filter_by(
                        cookie_id=cookie_id, item_id=item_id
                    ).delete()
                    if count > 0:
                        success_count += 1
                    else:
                        failed_count += 1
                session.commit()
        except Exception as e:
            logger.error(f"批量删除商品回复失败: {e}")
            return {"success_count": 0, "failed_count": len(items)}

        return {"success_count": success_count, "failed_count": failed_count}

    # ==================== 风控日志管理 ====================

    def add_risk_control_log(self, cookie_id: str, event_type: str = 'slider_captcha',
                           event_description: str = None, processing_result: str = None,
                           processing_status: str = 'processing', error_message: str = None) -> bool:
        """添加风控日志记录"""
        try:
            with self._session() as session:
                log = RiskControlLog(
                    cookie_id=cookie_id, event_type=event_type,
                    event_description=event_description,
                    processing_result=processing_result,
                    processing_status=processing_status,
                    error_message=error_message
                )
                session.add(log)
                session.commit()
                return True
        except Exception as e:
            logger.error(f"添加风控日志失败: {e}")
            return False

    def update_risk_control_log(self, log_id: int, processing_result: str = None,
                              processing_status: str = None, error_message: str = None) -> bool:
        """更新风控日志记录"""
        try:
            with self._session() as session:
                log = session.query(RiskControlLog).filter_by(id=log_id).first()
                if not log:
                    return False

                if processing_result is not None: log.processing_result = processing_result
                if processing_status is not None: log.processing_status = processing_status
                if error_message is not None: log.error_message = error_message

                session.commit()
                return True
        except Exception as e:
            logger.error(f"更新风控日志失败: {e}")
            return False

    def get_risk_control_logs(self, cookie_id: str = None, limit: int = 100, offset: int = 0) -> List[Dict]:
        """获取风控日志列表"""
        try:
            with self._session() as session:
                query = session.query(RiskControlLog, CookieAccount).outerjoin(
                    CookieAccount, RiskControlLog.cookie_id == CookieAccount.id
                )
                if cookie_id:
                    query = query.filter(RiskControlLog.cookie_id == cookie_id)
                query = query.order_by(RiskControlLog.created_at.desc()).offset(offset).limit(limit)

                logs = []
                for r, c in query.all():
                    log_dict = {
                        'id': r.id, 'cookie_id': r.cookie_id,
                        'event_type': r.event_type,
                        'event_description': r.event_description,
                        'processing_result': r.processing_result,
                        'processing_status': r.processing_status,
                        'error_message': r.error_message,
                        'created_at': str(r.created_at) if r.created_at else None,
                        'updated_at': str(r.updated_at) if r.updated_at else None,
                        'cookie_name': c.id if c else None
                    }
                    logs.append(log_dict)
                return logs
        except Exception as e:
            logger.error(f"获取风控日志失败: {e}")
            return []

    def get_risk_control_logs_count(self, cookie_id: str = None) -> int:
        """获取风控日志总数"""
        try:
            with self._session() as session:
                query = session.query(func.count(RiskControlLog.id))
                if cookie_id:
                    query = query.filter(RiskControlLog.cookie_id == cookie_id)
                return query.scalar() or 0
        except Exception as e:
            logger.error(f"获取风控日志数量失败: {e}")
            return 0

    def delete_risk_control_log(self, log_id: int) -> bool:
        """删除风控日志记录"""
        try:
            with self._session() as session:
                count = session.query(RiskControlLog).filter_by(id=log_id).delete()
                session.commit()
                return count > 0
        except Exception as e:
            logger.error(f"删除风控日志失败: {e}")
            return False

    def clear_risk_control_logs(self, cookie_id: str = None) -> int:
        """批量清空风控日志"""
        try:
            with self._session() as session:
                query = session.query(RiskControlLog)
                if cookie_id:
                    query = query.filter(RiskControlLog.cookie_id == cookie_id)
                count = query.delete()
                session.commit()
                logger.info(f"批量清空风控日志: {count} 条")
                return count
        except Exception as e:
            logger.error(f"批量清空风控日志失败: {e}")
            return 0

    def cleanup_old_data(self, days: int = 90) -> dict:
        """清理过期的历史数据，防止数据库无限增长"""
        try:
            with self._session() as session:
                stats = {}
                cutoff = datetime.utcnow() - timedelta(days=days)

                # 清理AI对话历史
                try:
                    count = session.query(AIConversation).filter(
                        AIConversation.created_at < cutoff
                    ).delete(synchronize_session='fetch')
                    stats['ai_conversations'] = count
                    if count > 0:
                        logger.info(f"清理了 {count} 条过期的AI对话记录（{days}天前）")
                except Exception as e:
                    logger.warning(f"清理AI对话历史失败: {e}")
                    stats['ai_conversations'] = 0

                # 清理风控日志
                try:
                    count = session.query(RiskControlLog).filter(
                        RiskControlLog.created_at < cutoff
                    ).delete(synchronize_session='fetch')
                    stats['risk_control_logs'] = count
                    if count > 0:
                        logger.info(f"清理了 {count} 条过期的风控日志（{days}天前）")
                except Exception as e:
                    logger.warning(f"清理风控日志失败: {e}")
                    stats['risk_control_logs'] = 0

                # 清理AI商品缓存（最多保留30天）
                cache_days = min(days, 30)
                cache_cutoff = datetime.utcnow() - timedelta(days=cache_days)
                try:
                    count = session.query(AIItemCache).filter(
                        AIItemCache.last_updated < cache_cutoff
                    ).delete(synchronize_session='fetch')
                    stats['ai_item_cache'] = count
                    if count > 0:
                        logger.info(f"清理了 {count} 条过期的AI商品缓存（{cache_days}天前）")
                except Exception as e:
                    logger.warning(f"清理AI商品缓存失败: {e}")
                    stats['ai_item_cache'] = 0

                # 清理验证码记录（保留最近1天）
                one_day_ago = datetime.utcnow() - timedelta(days=1)
                try:
                    count = session.query(CaptchaCode).filter(
                        CaptchaCode.created_at < one_day_ago
                    ).delete(synchronize_session='fetch')
                    stats['captcha_codes'] = count
                    if count > 0:
                        logger.info(f"清理了 {count} 条过期的验证码记录")
                except Exception as e:
                    logger.warning(f"清理验证码记录失败: {e}")
                    stats['captcha_codes'] = 0

                # 清理邮箱验证记录（保留最近7天）
                seven_days_ago = datetime.utcnow() - timedelta(days=7)
                try:
                    count = session.query(EmailVerification).filter(
                        EmailVerification.created_at < seven_days_ago
                    ).delete(synchronize_session='fetch')
                    stats['email_verifications'] = count
                    if count > 0:
                        logger.info(f"清理了 {count} 条过期的邮箱验证记录")
                except Exception as e:
                    logger.warning(f"清理邮箱验证记录失败: {e}")
                    stats['email_verifications'] = 0

                session.commit()

                total_cleaned = sum(stats.values())
                stats['vacuum_executed'] = False
                stats['total_cleaned'] = total_cleaned
                return stats

        except Exception as e:
            logger.error(f"清理历史数据时出错: {e}")
            return {'error': str(e)}

    # 保留upgrade方法用于兼容（不执行任何操作，Alembic管理迁移）
    def upgrade_keywords_table_for_image_support(self, cursor=None):
        """升级keywords表以支持图片关键词（ORM版本不需要此操作）"""
        logger.info("ORM模式下不需要手动升级表结构，由SQLAlchemy自动管理")
        return True


# 全局单例
db_manager = DBManager()

# 确保进程结束时关闭数据库连接
import atexit
atexit.register(db_manager.close)
