"""
Alembic环境配置
闲鱼自动回复系统 - 数据库迁移管理

从DATABASE_URL环境变量读取数据库连接，支持SQLite和PostgreSQL。
"""
import os
import sys
from logging.config import fileConfig

from sqlalchemy import engine_from_config
from sqlalchemy import pool

from alembic import context

# 确保项目根目录在sys.path中，以便导入models
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 导入所有模型，确保autogenerate能检测到所有表
from models import Base  # noqa: E402

# Alembic Config对象
config = context.config

# 从DATABASE_URL环境变量动态设置数据库连接URL
database_url = os.getenv('DATABASE_URL')
if database_url:
    config.set_main_option('sqlalchemy.url', database_url)
else:
    # 回退到alembic.ini中配置的默认值（sqlite:///data/xianyu_data.db）
    db_path = os.getenv('DB_PATH', 'data/xianyu_data.db')
    config.set_main_option('sqlalchemy.url', f'sqlite:///{db_path}')

# 配置Python日志
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# 设置目标metadata用于autogenerate支持
target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """离线模式运行迁移（仅生成SQL脚本，不连接数据库）"""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        render_as_batch=True,  # SQLite不支持ALTER TABLE，使用batch模式
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """在线模式运行迁移（连接数据库并执行）"""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            render_as_batch=True,  # SQLite不支持ALTER TABLE，使用batch模式
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
