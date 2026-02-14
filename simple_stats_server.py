#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简单的用户统计服务器
只统计有多少人在使用闲鱼自动回复系统
"""

from fastapi import FastAPI
from pydantic import BaseModel
from typing import Dict, Any
from datetime import datetime
import uvicorn
from pathlib import Path
from contextlib import contextmanager

from sqlalchemy import create_engine, func, text
from sqlalchemy.orm import sessionmaker
from models import Base, UserStatsRecord

app = FastAPI(title="闲鱼自动回复系统用户统计", version="1.0.0")

# 数据库文件路径
DB_DIR = Path(__file__).parent / "data"
DB_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DB_DIR / "user_stats.db"

# 独立的 SQLAlchemy 引擎（用户统计专用）
stats_engine = create_engine(f"sqlite:///{DB_PATH}", connect_args={"check_same_thread": False})
StatsSession = sessionmaker(bind=stats_engine)


@contextmanager
def get_stats_session():
    """获取统计数据库会话"""
    session = StatsSession()
    try:
        yield session
    finally:
        session.close()


class UserStats(BaseModel):
    """用户统计数据模型"""
    anonymous_id: str
    timestamp: str
    project: str
    info: Dict[str, Any]


def init_database():
    """初始化数据库"""
    Base.metadata.create_all(bind=stats_engine, tables=[UserStatsRecord.__table__])
    # 创建索引（SQLAlchemy 模型中未定义的额外索引）
    with stats_engine.connect() as conn:
        conn.execute(text('CREATE INDEX IF NOT EXISTS idx_anonymous_id ON user_stats(anonymous_id)'))
        conn.execute(text('CREATE INDEX IF NOT EXISTS idx_last_seen ON user_stats(last_seen)'))
        conn.commit()


def save_user_stats(data: UserStats):
    """保存用户统计数据"""
    with get_stats_session() as session:
        try:
            info = data.info
            os_info = info.get('os', 'unknown')
            version = info.get('version', '2.2.0')

            existing = session.query(UserStatsRecord).filter_by(
                anonymous_id=data.anonymous_id
            ).first()

            if existing:
                existing.last_seen = func.now()
                existing.total_reports = (existing.total_reports or 0) + 1
                existing.os = os_info
                existing.version = version
            else:
                record = UserStatsRecord(
                    anonymous_id=data.anonymous_id,
                    os=os_info, version=version
                )
                session.add(record)

            session.commit()
            return True

        except Exception as e:
            session.rollback()
            print(f"保存用户统计失败: {e}")
            return False


@app.post('/statistics')
async def receive_user_stats(data: UserStats):
    """接收用户统计数据"""
    try:
        success = save_user_stats(data)
        
        if success:
            print(f"收到用户统计: {data.anonymous_id}")
            return {"status": "success", "message": "用户统计已收到"}
        else:
            return {"status": "error", "message": "保存统计数据失败"}
            
    except Exception as e:
        print(f"处理用户统计失败: {e}")
        return {"status": "error", "message": "处理统计数据失败"}


@app.get('/stats')
async def get_user_stats():
    """获取用户统计摘要"""
    with get_stats_session() as session:
        try:
            # 总用户数
            total_users = session.query(func.count(UserStatsRecord.id)).scalar() or 0

            # 今日活跃用户
            daily_active = session.execute(
                text("SELECT COUNT(*) FROM user_stats WHERE DATE(last_seen) = DATE('now')")
            ).scalar() or 0

            # 本周活跃用户
            weekly_active = session.execute(
                text("SELECT COUNT(*) FROM user_stats WHERE DATE(last_seen) >= DATE('now', '-7 days')")
            ).scalar() or 0

            # 操作系统分布
            os_rows = session.query(
                UserStatsRecord.os, func.count(UserStatsRecord.id)
            ).group_by(UserStatsRecord.os).order_by(func.count(UserStatsRecord.id).desc()).all()
            os_distribution = dict(os_rows)

            # 版本分布
            ver_rows = session.query(
                UserStatsRecord.version, func.count(UserStatsRecord.id)
            ).group_by(UserStatsRecord.version).order_by(func.count(UserStatsRecord.id).desc()).all()
            version_distribution = dict(ver_rows)

            return {
                "total_users": total_users,
                "daily_active_users": daily_active,
                "weekly_active_users": weekly_active,
                "os_distribution": os_distribution,
                "version_distribution": version_distribution,
                "last_updated": datetime.now().isoformat()
            }

        except Exception as e:
            return {"error": f"获取统计失败: {e}"}


@app.get('/stats/recent')
async def get_recent_users():
    """获取最近活跃的用户（匿名）"""
    with get_stats_session() as session:
        try:
            records = session.query(UserStatsRecord).order_by(
                UserStatsRecord.last_seen.desc()
            ).limit(20).all()

            return {
                "recent_users": [
                    {
                        "anonymous_id": r.anonymous_id[:8] + "****",
                        "first_seen": str(r.first_seen) if r.first_seen else None,
                        "last_seen": str(r.last_seen) if r.last_seen else None,
                        "os": r.os,
                        "version": r.version,
                        "total_reports": r.total_reports
                    }
                    for r in records
                ]
            }

        except Exception as e:
            return {"error": f"获取最近用户失败: {e}"}


@app.get('/')
async def root():
    """根路径"""
    return {
        "message": "闲鱼自动回复系统用户统计服务器",
        "description": "只统计有多少人在使用这个系统",
        "endpoints": {
            "POST /statistics": "接收用户统计数据",
            "GET /stats": "获取用户统计摘要",
            "GET /stats/recent": "获取最近活跃用户"
        }
    }


if __name__ == "__main__":
    # 初始化数据库
    init_database()
    print("用户统计数据库初始化完成")
    
    # 启动服务器
    print("启动用户统计服务器...")
    print("访问 http://localhost:8081/stats 查看统计信息")
    uvicorn.run(app, host="0.0.0.0", port=8081)
