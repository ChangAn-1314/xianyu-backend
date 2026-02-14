"""SQLAlchemy声明式数据模型

定义所有21+张数据库表的ORM模型，从db_manager.py的CREATE TABLE语句转换而来。
所有模型同时兼容SQLite和PostgreSQL。
"""

from sqlalchemy import (
    Column, Integer, String, Text, Boolean, Float, DateTime,
    ForeignKey, UniqueConstraint, CheckConstraint, Index
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from database import Base


# ==================== 用户和认证 ====================

class User(Base):
    """用户表"""
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String, unique=True, nullable=False)
    email = Column(String, unique=True, nullable=False)
    password_hash = Column(String, nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    # relationships
    cookies = relationship("CookieAccount", back_populates="user", cascade="all, delete-orphan")
    settings = relationship("UserSetting", back_populates="user", cascade="all, delete-orphan")


class EmailVerification(Base):
    """邮箱验证码表"""
    __tablename__ = 'email_verifications'

    id = Column(Integer, primary_key=True, autoincrement=True)
    email = Column(String, nullable=False)
    code = Column(String, nullable=False)
    expires_at = Column(Float, nullable=False)  # Unix时间戳
    used = Column(Boolean, default=False)
    type = Column(String, default='register')
    created_at = Column(DateTime, default=func.now())


class CaptchaCode(Base):
    """图形验证码表"""
    __tablename__ = 'captcha_codes'

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(String, nullable=False)
    code = Column(String, nullable=False)
    expires_at = Column(Float, nullable=False)  # Unix时间戳
    created_at = Column(DateTime, default=func.now())


# ==================== Cookie账号管理 ====================

class CookieAccount(Base):
    """Cookie账号表"""
    __tablename__ = 'cookies'

    id = Column(String, primary_key=True)
    value = Column(Text, nullable=False)
    user_id = Column(Integer, ForeignKey('users.id', ondelete='CASCADE'), nullable=False)
    auto_confirm = Column(Integer, default=1)
    remark = Column(String, default='')
    pause_duration = Column(Integer, default=10)
    username = Column(String, default='')
    password = Column(String, default='')
    show_browser = Column(Integer, default=0)
    created_at = Column(DateTime, default=func.now())

    # relationships
    user = relationship("User", back_populates="cookies")
    keywords = relationship("Keyword", back_populates="cookie", cascade="all, delete-orphan")
    status = relationship("CookieStatus", back_populates="cookie", uselist=False, cascade="all, delete-orphan")
    ai_settings = relationship("AIReplySetting", back_populates="cookie", uselist=False, cascade="all, delete-orphan")
    conversations = relationship("AIConversation", back_populates="cookie", cascade="all, delete-orphan")
    orders = relationship("Order", back_populates="cookie", cascade="all, delete-orphan")
    item_infos = relationship("ItemInfo", back_populates="cookie", cascade="all, delete-orphan")
    default_reply = relationship("DefaultReply", back_populates="cookie", uselist=False, cascade="all, delete-orphan")
    default_reply_records = relationship("DefaultReplyRecord", back_populates="cookie", cascade="all, delete-orphan")
    message_notifications = relationship("MessageNotification", back_populates="cookie", cascade="all, delete-orphan")
    risk_control_logs = relationship("RiskControlLog", back_populates="cookie", cascade="all, delete-orphan")


class CookieStatus(Base):
    """Cookie状态表"""
    __tablename__ = 'cookie_status'

    cookie_id = Column(String, ForeignKey('cookies.id', ondelete='CASCADE'), primary_key=True)
    enabled = Column(Boolean, default=True)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    # relationships
    cookie = relationship("CookieAccount", back_populates="status")


# ==================== 关键词管理 ====================

class Keyword(Base):
    """关键词自动回复表"""
    __tablename__ = 'keywords'

    # keywords表原始设计无主键，添加rowid作为隐式主键
    # SQLite自动有rowid，PostgreSQL需要显式主键
    id = Column(Integer, primary_key=True, autoincrement=True)
    cookie_id = Column(String, ForeignKey('cookies.id', ondelete='CASCADE'))
    keyword = Column(String)
    reply = Column(String)
    item_id = Column(String)
    type = Column(String, default='text')
    image_url = Column(String)

    # relationships
    cookie = relationship("CookieAccount", back_populates="keywords")


# ==================== AI回复 ====================

class AIReplySetting(Base):
    """AI回复配置表"""
    __tablename__ = 'ai_reply_settings'

    cookie_id = Column(String, ForeignKey('cookies.id', ondelete='CASCADE'), primary_key=True)
    ai_enabled = Column(Boolean, default=False)
    model_name = Column(String, default='qwen-plus')
    api_key = Column(String)
    base_url = Column(String, default='https://dashscope.aliyuncs.com/compatible-mode/v1')
    max_discount_percent = Column(Integer, default=10)
    max_discount_amount = Column(Integer, default=100)
    max_bargain_rounds = Column(Integer, default=3)
    custom_prompts = Column(Text)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    # relationships
    cookie = relationship("CookieAccount", back_populates="ai_settings")


class AIConversation(Base):
    """AI对话历史表"""
    __tablename__ = 'ai_conversations'

    id = Column(Integer, primary_key=True, autoincrement=True)
    cookie_id = Column(String, ForeignKey('cookies.id', ondelete='CASCADE'), nullable=False)
    chat_id = Column(String, nullable=False)
    user_id = Column(String, nullable=False)
    item_id = Column(String, nullable=False)
    role = Column(String, nullable=False)
    content = Column(Text, nullable=False)
    intent = Column(String)
    bargain_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=func.now())

    # relationships
    cookie = relationship("CookieAccount", back_populates="conversations")


class AIItemCache(Base):
    """AI商品信息缓存表"""
    __tablename__ = 'ai_item_cache'

    item_id = Column(String, primary_key=True)
    data = Column(Text, nullable=False)
    price = Column(Float)
    description = Column(Text)
    last_updated = Column(DateTime, default=func.now())


# ==================== 卡券和发货 ====================

class Card(Base):
    """卡券表"""
    __tablename__ = 'cards'
    __table_args__ = (
        CheckConstraint("type IN ('api', 'text', 'data', 'image')", name='ck_cards_type'),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    type = Column(String, nullable=False)
    api_config = Column(Text)
    text_content = Column(Text)
    data_content = Column(Text)
    image_url = Column(String)
    description = Column(Text)
    enabled = Column(Boolean, default=True)
    delay_seconds = Column(Integer, default=0)
    is_multi_spec = Column(Boolean, default=False)
    spec_name = Column(String)
    spec_value = Column(String)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False, default=1)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    # relationships
    user = relationship("User")
    delivery_rules = relationship("DeliveryRule", back_populates="card", cascade="all, delete-orphan")


class Order(Base):
    """订单表"""
    __tablename__ = 'orders'

    order_id = Column(String, primary_key=True)
    item_id = Column(String)
    buyer_id = Column(String)
    spec_name = Column(String)
    spec_value = Column(String)
    quantity = Column(String)
    amount = Column(String)
    order_status = Column(String, default='unknown')
    cookie_id = Column(String, ForeignKey('cookies.id', ondelete='CASCADE'))
    is_bargain = Column(Integer, default=0)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    # relationships
    cookie = relationship("CookieAccount", back_populates="orders")


class ItemInfo(Base):
    """商品信息表"""
    __tablename__ = 'item_info'
    __table_args__ = (
        UniqueConstraint('cookie_id', 'item_id', name='uq_item_info_cookie_item'),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    cookie_id = Column(String, ForeignKey('cookies.id', ondelete='CASCADE'), nullable=False)
    item_id = Column(String, nullable=False)
    item_title = Column(String)
    item_description = Column(Text)
    item_category = Column(String)
    item_price = Column(String)
    item_detail = Column(Text)
    is_multi_spec = Column(Boolean, default=False)
    multi_quantity_delivery = Column(Boolean, default=False)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    # relationships
    cookie = relationship("CookieAccount", back_populates="item_infos")


class DeliveryRule(Base):
    """自动发货规则表"""
    __tablename__ = 'delivery_rules'

    id = Column(Integer, primary_key=True, autoincrement=True)
    keyword = Column(String, nullable=False)
    card_id = Column(Integer, ForeignKey('cards.id', ondelete='CASCADE'), nullable=False)
    delivery_count = Column(Integer, default=1)
    enabled = Column(Boolean, default=True)
    description = Column(Text)
    delivery_times = Column(Integer, default=0)
    user_id = Column(Integer, ForeignKey('users.id'))
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    # relationships
    card = relationship("Card", back_populates="delivery_rules")
    user = relationship("User")


# ==================== 默认回复 ====================

class DefaultReply(Base):
    """默认回复配置表"""
    __tablename__ = 'default_replies'

    cookie_id = Column(String, ForeignKey('cookies.id', ondelete='CASCADE'), primary_key=True)
    enabled = Column(Boolean, default=False)
    reply_content = Column(Text)
    reply_image_url = Column(String)
    reply_once = Column(Boolean, default=False)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    # relationships
    cookie = relationship("CookieAccount", back_populates="default_reply")


class ItemReplay(Base):
    """指定商品回复表"""
    __tablename__ = 'item_replay'

    id = Column(Integer, primary_key=True, autoincrement=True)
    item_id = Column(String, nullable=False)
    cookie_id = Column(String, nullable=False)
    reply_content = Column(Text, nullable=False)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())


class DefaultReplyRecord(Base):
    """默认回复记录表（记录已回复的chat_id）"""
    __tablename__ = 'default_reply_records'
    __table_args__ = (
        UniqueConstraint('cookie_id', 'chat_id', name='uq_default_reply_records_cookie_chat'),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    cookie_id = Column(String, ForeignKey('cookies.id', ondelete='CASCADE'), nullable=False)
    chat_id = Column(String, nullable=False)
    replied_at = Column(DateTime, default=func.now())

    # relationships
    cookie = relationship("CookieAccount", back_populates="default_reply_records")


# ==================== 通知系统 ====================

class NotificationChannel(Base):
    """通知渠道表"""
    __tablename__ = 'notification_channels'
    __table_args__ = (
        CheckConstraint(
            "type IN ('qq','ding_talk','dingtalk','feishu','lark','bark','email','webhook','wechat','telegram')",
            name='ck_notification_channels_type'
        ),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    type = Column(String, nullable=False)
    config = Column(Text, nullable=False)
    enabled = Column(Boolean, default=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    # relationships
    user = relationship("User")
    message_notifications = relationship("MessageNotification", back_populates="channel", cascade="all, delete-orphan")


class MessageNotification(Base):
    """消息通知配置表"""
    __tablename__ = 'message_notifications'
    __table_args__ = (
        UniqueConstraint('cookie_id', 'channel_id', name='uq_message_notifications_cookie_channel'),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    cookie_id = Column(String, ForeignKey('cookies.id', ondelete='CASCADE'), nullable=False)
    channel_id = Column(Integer, ForeignKey('notification_channels.id', ondelete='CASCADE'), nullable=False)
    enabled = Column(Boolean, default=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    # relationships
    cookie = relationship("CookieAccount", back_populates="message_notifications")
    channel = relationship("NotificationChannel", back_populates="message_notifications")


# ==================== 系统设置 ====================

class SystemSetting(Base):
    """系统设置表"""
    __tablename__ = 'system_settings'

    key = Column(String, primary_key=True)
    value = Column(String, nullable=False)
    description = Column(Text)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())


class UserSetting(Base):
    """用户设置表"""
    __tablename__ = 'user_settings'
    __table_args__ = (
        UniqueConstraint('user_id', 'key', name='uq_user_settings_user_key'),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey('users.id', ondelete='CASCADE'), nullable=False)
    key = Column(String, nullable=False)
    value = Column(String, nullable=False)
    description = Column(Text)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    # relationships
    user = relationship("User", back_populates="settings")


# ==================== 风控日志 ====================

class RiskControlLog(Base):
    """风控日志表"""
    __tablename__ = 'risk_control_logs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    cookie_id = Column(String, ForeignKey('cookies.id', ondelete='CASCADE'), nullable=False)
    event_type = Column(String, nullable=False, default='slider_captcha')
    event_description = Column(Text)
    processing_result = Column(Text)
    processing_status = Column(String, default='processing')
    error_message = Column(Text)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    # relationships
    cookie = relationship("CookieAccount", back_populates="risk_control_logs")


class UserStatsRecord(Base):
    """用户统计记录（独立统计服务使用）"""
    __tablename__ = 'user_stats'

    id = Column(Integer, primary_key=True, autoincrement=True)
    anonymous_id = Column(String, unique=True, nullable=False)
    first_seen = Column(DateTime, default=func.now())
    last_seen = Column(DateTime, default=func.now())
    os = Column(String)
    version = Column(String)
    total_reports = Column(Integer, default=1)
