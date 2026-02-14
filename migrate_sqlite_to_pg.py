#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SQLite -> PostgreSQL 数据迁移工具
闲鱼自动回复系统 - 一次性将本地SQLite数据迁移到Neon PostgreSQL

用法:
    python migrate_sqlite_to_pg.py --source sqlite:///data/xianyu_data.db --target postgresql://user:pass@host/db
    python migrate_sqlite_to_pg.py --source data/xianyu_data.db --target postgresql://user:pass@host/db --dry-run
    python migrate_sqlite_to_pg.py  # 从环境变量 DB_PATH 和 DATABASE_URL 读取
"""

import os
import sys
import argparse
import time
from datetime import datetime
from typing import Dict, List, Any, Optional

from sqlalchemy import create_engine, inspect, text, MetaData
from sqlalchemy.orm import sessionmaker
from loguru import logger

# 确保项目根目录在sys.path中
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from models import Base

# 迁移表顺序（按外键依赖排序，被依赖的表在前）
TABLE_ORDER = [
    'users',
    'system_settings',
    'cookies',
    'cookie_status',
    'ai_reply_settings',
    'keywords',
    'ai_conversations',
    'ai_item_cache',
    'item_info',
    'item_replay',
    'cards',
    'delivery_rules',
    'default_replies',
    'default_reply_records',
    'notification_channels',
    'message_notifications',
    'email_verifications',
    'captcha_codes',
    'orders',
    'user_settings',
    'risk_control_logs',
]


def get_source_engine(source: str):
    """创建源数据库引擎（只读SQLite）"""
    if not source.startswith('sqlite'):
        # 如果传入的是文件路径而非URL
        source = f'sqlite:///{source}'
    engine = create_engine(source, echo=False)
    return engine


def get_target_engine(target: str):
    """创建目标数据库引擎（PostgreSQL）"""
    connect_args = {}
    if 'neon.tech' in target or 'neon' in target:
        connect_args['sslmode'] = 'require'

    engine = create_engine(
        target,
        echo=False,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        connect_args=connect_args,
    )
    return engine


def get_table_row_count(engine, table_name: str) -> int:
    """获取表的行数"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f'SELECT COUNT(*) FROM "{table_name}"'))
            return result.scalar() or 0
    except Exception:
        return 0


def get_table_data(engine, table_name: str) -> List[Dict[str, Any]]:
    """从源数据库读取表数据"""
    with engine.connect() as conn:
        result = conn.execute(text(f'SELECT * FROM "{table_name}"'))
        columns = result.keys()
        rows = []
        for row in result.fetchall():
            row_dict = dict(zip(columns, row))
            rows.append(row_dict)
        return rows


def convert_value(value: Any, col_name: str) -> Any:
    """转换数据类型以兼容PostgreSQL"""
    if value is None:
        return None

    # SQLite BOOLEAN (0/1整数) -> Python bool
    if isinstance(value, int) and col_name in (
        'used', 'is_active', 'auto_confirm', 'is_admin',
        'is_multi_spec', 'multi_spec_enabled',
        'multi_quantity_delivery_enabled', 'is_image',
    ):
        return bool(value)

    return value


def migrate_table(
    source_engine,
    target_engine,
    table_name: str,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """迁移单个表的数据

    Returns:
        {'table': str, 'source_count': int, 'migrated': int, 'status': str, 'error': str|None}
    """
    result = {
        'table': table_name,
        'source_count': 0,
        'migrated': 0,
        'status': 'pending',
        'error': None,
    }

    try:
        # 检查源表是否存在
        src_inspector = inspect(source_engine)
        if table_name not in src_inspector.get_table_names():
            result['status'] = 'skipped'
            result['error'] = '源表不存在'
            return result

        # 读取源数据
        rows = get_table_data(source_engine, table_name)
        result['source_count'] = len(rows)

        if len(rows) == 0:
            result['status'] = 'empty'
            return result

        if dry_run:
            result['status'] = 'dry_run'
            return result

        # 转换数据类型
        converted_rows = []
        for row in rows:
            converted = {}
            for col, val in row.items():
                converted[col] = convert_value(val, col)
            converted_rows.append(converted)

        # 写入目标数据库
        TargetSession = sessionmaker(bind=target_engine)
        session = TargetSession()

        try:
            # 清空目标表（避免主键冲突）
            session.execute(text(f'DELETE FROM "{table_name}"'))

            # 批量插入
            if converted_rows:
                # 构建INSERT语句
                columns = list(converted_rows[0].keys())
                col_str = ', '.join(f'"{c}"' for c in columns)
                param_str = ', '.join(f':{c}' for c in columns)
                insert_sql = text(f'INSERT INTO "{table_name}" ({col_str}) VALUES ({param_str})')

                # 分批插入（每批500条）
                batch_size = 500
                for i in range(0, len(converted_rows), batch_size):
                    batch = converted_rows[i:i + batch_size]
                    session.execute(insert_sql, batch)

            session.commit()
            result['migrated'] = len(converted_rows)
            result['status'] = 'success'

        except Exception as e:
            session.rollback()
            result['status'] = 'error'
            result['error'] = str(e)
        finally:
            session.close()

    except Exception as e:
        result['status'] = 'error'
        result['error'] = str(e)

    return result


def reset_sequences(target_engine, table_results: List[Dict]):
    """重置PostgreSQL自增序列到正确的值"""
    try:
        with target_engine.connect() as conn:
            for res in table_results:
                if res['status'] != 'success' or res['migrated'] == 0:
                    continue
                table_name = res['table']
                try:
                    # 查找该表的自增列（通常是id）
                    result = conn.execute(text(f"""
                        SELECT column_name FROM information_schema.columns 
                        WHERE table_name = :table AND column_default LIKE 'nextval%%'
                    """), {'table': table_name})
                    seq_cols = [row[0] for row in result.fetchall()]

                    for col in seq_cols:
                        # 重置序列到当前最大值+1
                        conn.execute(text(f"""
                            SELECT setval(
                                pg_get_serial_sequence('"{table_name}"', '{col}'),
                                COALESCE((SELECT MAX("{col}") FROM "{table_name}"), 0) + 1,
                                false
                            )
                        """))
                    conn.commit()
                except Exception as e:
                    logger.warning(f"重置 {table_name} 序列失败: {e}")
    except Exception as e:
        logger.warning(f"重置序列整体失败: {e}")


def print_report(results: List[Dict], elapsed: float, dry_run: bool):
    """打印迁移报告"""
    print("\n" + "=" * 70)
    print(f"  数据迁移报告 {'(DRY RUN - 未实际写入)' if dry_run else ''}")
    print("=" * 70)
    print(f"{'表名':<30} {'源记录数':>10} {'已迁移':>10} {'状态':>10}")
    print("-" * 70)

    total_source = 0
    total_migrated = 0
    errors = []

    for res in results:
        total_source += res['source_count']
        total_migrated += res['migrated']
        status_display = {
            'success': 'OK',
            'empty': '空表',
            'skipped': '跳过',
            'dry_run': '预览',
            'error': '错误',
            'pending': '待处理',
        }.get(res['status'], res['status'])

        print(f"{res['table']:<30} {res['source_count']:>10} {res['migrated']:>10} {status_display:>10}")

        if res['error']:
            errors.append((res['table'], res['error']))

    print("-" * 70)
    print(f"{'合计':<30} {total_source:>10} {total_migrated:>10}")
    print(f"\n耗时: {elapsed:.2f} 秒")

    if errors:
        print(f"\n错误详情 ({len(errors)} 个):")
        for table, error in errors:
            print(f"  - {table}: {error}")

    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description='SQLite -> PostgreSQL 数据迁移工具',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python migrate_sqlite_to_pg.py --source data/xianyu_data.db --target postgresql://user:pass@host/db
  python migrate_sqlite_to_pg.py --dry-run  # 从环境变量读取，仅预览
        """,
    )
    parser.add_argument(
        '--source', '-s',
        default=None,
        help='源SQLite数据库路径或URL (默认从DB_PATH环境变量读取)',
    )
    parser.add_argument(
        '--target', '-t',
        default=None,
        help='目标PostgreSQL连接URL (默认从DATABASE_URL环境变量读取)',
    )
    parser.add_argument(
        '--dry-run', '-n',
        action='store_true',
        help='仅检查不写入数据',
    )
    parser.add_argument(
        '--tables',
        nargs='*',
        default=None,
        help='仅迁移指定表 (空格分隔)',
    )

    args = parser.parse_args()

    # 确定源数据库
    source = args.source or os.getenv('DB_PATH', 'data/xianyu_data.db')
    if not source.startswith('sqlite'):
        source_path = source
        source = f'sqlite:///{source}'
    else:
        source_path = source.replace('sqlite:///', '')

    # 确定目标数据库
    target = args.target or os.getenv('DATABASE_URL')
    if not target:
        print("错误: 未指定目标数据库。使用 --target 参数或设置 DATABASE_URL 环境变量。")
        sys.exit(1)

    if 'sqlite' in target:
        print("错误: 目标数据库不能是SQLite。请指定PostgreSQL连接URL。")
        sys.exit(1)

    # 检查源文件
    if not os.path.exists(source_path):
        print(f"错误: 源数据库文件不存在: {source_path}")
        sys.exit(1)

    print(f"源数据库: {source_path}")
    print(f"目标数据库: {target[:50]}...")
    print(f"模式: {'DRY RUN (仅预览)' if args.dry_run else '实际迁移'}")
    print()

    # 创建引擎
    source_engine = get_source_engine(source)
    target_engine = get_target_engine(target)

    # 在目标数据库创建表结构
    if not args.dry_run:
        print("正在目标数据库创建表结构...")
        Base.metadata.create_all(bind=target_engine)
        print("表结构创建完成。\n")

    # 确定要迁移的表
    tables = args.tables or TABLE_ORDER

    # 开始迁移
    start_time = time.time()
    results = []

    for table_name in tables:
        logger.info(f"迁移表: {table_name}")
        result = migrate_table(source_engine, target_engine, table_name, args.dry_run)
        results.append(result)

        status_icon = {
            'success': '+', 'empty': '-', 'skipped': '~',
            'dry_run': '?', 'error': 'X',
        }.get(result['status'], ' ')
        print(f"  [{status_icon}] {table_name}: {result['source_count']} 条记录"
              + (f" -> {result['migrated']} 已迁移" if result['migrated'] > 0 else "")
              + (f" ({result['error']})" if result['error'] else ""))

    # 重置PostgreSQL自增序列
    if not args.dry_run:
        print("\n正在重置自增序列...")
        reset_sequences(target_engine, results)

    elapsed = time.time() - start_time

    # 打印报告
    print_report(results, elapsed, args.dry_run)

    # 验证
    if not args.dry_run:
        print("\n正在验证迁移结果...")
        mismatch = False
        for res in results:
            if res['status'] == 'success':
                target_count = get_table_row_count(target_engine, res['table'])
                if target_count != res['source_count']:
                    print(f"  警告: {res['table']} 源={res['source_count']} 目标={target_count}")
                    mismatch = True
        if not mismatch:
            print("  所有表数据量验证通过。")

    # 清理
    source_engine.dispose()
    target_engine.dispose()


if __name__ == '__main__':
    main()
