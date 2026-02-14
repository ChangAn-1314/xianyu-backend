"""数据库连接管理模块

通过SQLAlchemy提供统一的数据库抽象层，支持：
- SQLite（本地开发，默认）
- PostgreSQL + Neon Serverless（生产环境）

通过环境变量DATABASE_URL切换数据库，无需修改代码。
"""

import os
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, declarative_base


# 从环境变量读取数据库连接字符串，默认回退到本地SQLite
DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///data/xianyu_data.db')

# Neon/Heroku等平台可能使用 postgres:// 而非 postgresql://
if DATABASE_URL.startswith('postgres://'):
    DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)

# 根据数据库类型配置引擎参数
engine_kwargs = {}

if DATABASE_URL.startswith('postgresql'):
    # PostgreSQL (Neon Serverless) 配置
    engine_kwargs = {
        'pool_size': int(os.getenv('DB_POOL_SIZE', '5')),
        'max_overflow': int(os.getenv('DB_MAX_OVERFLOW', '10')),
        'pool_pre_ping': True,  # 自动检测断开的连接并重连
        'pool_recycle': 300,    # 5分钟回收连接，避免Neon空闲断开
    }
    # Neon连接需要SSL
    if 'neon' in DATABASE_URL:
        engine_kwargs['connect_args'] = {'sslmode': 'require'}
else:
    # SQLite 配置
    engine_kwargs = {
        'connect_args': {'check_same_thread': False},  # 允许多线程访问
    }

# 创建SQLAlchemy引擎
engine = create_engine(DATABASE_URL, **engine_kwargs)

# SQLite WAL模式优化（仅SQLite生效）
if DATABASE_URL.startswith('sqlite'):
    @event.listens_for(engine, "connect")
    def set_sqlite_pragma(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

# 创建Session工厂
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# 声明式基类，所有模型继承此类
Base = declarative_base()


def get_db():
    """获取数据库Session的依赖注入函数

    用法（FastAPI依赖注入）:
        @app.get("/items")
        def read_items(db: Session = Depends(get_db)):
            ...

    用法（手动管理）:
        db = next(get_db())
        try:
            ...
        finally:
            db.close()
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_engine():
    """获取当前数据库引擎实例"""
    return engine


def is_postgresql():
    """判断当前是否使用PostgreSQL数据库"""
    return DATABASE_URL.startswith('postgresql')
