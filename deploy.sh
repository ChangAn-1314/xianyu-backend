#!/bin/bash
set -e

echo "========================================"
echo "  闲鱼自动回复系统 - 阿里云 ECS 部署脚本"
echo "========================================"

# 1. 更新系统包
echo "[1/6] 更新系统包..."
apt-get update -y
apt-get install -y curl git ca-certificates

# 2. 安装 Docker
echo "[2/6] 安装 Docker..."
if ! command -v docker &> /dev/null; then
    curl -fsSL https://get.docker.com | sh
    systemctl enable docker
    systemctl start docker
    echo "Docker 安装完成"
else
    echo "Docker 已安装，跳过"
fi
docker --version

# 3. 克隆后端仓库
echo "[3/6] 克隆后端仓库..."
cd /root
if [ -d "xianyu-backend" ]; then
    echo "仓库已存在，拉取最新代码..."
    cd xianyu-backend
    git pull
else
    git clone https://github.com/ChangAn-1314/xianyu-backend.git
    cd xianyu-backend
fi

# 4. 创建 .env 文件
echo "[4/6] 创建 .env 文件..."
cat > .env << 'ENVEOF'
# 数据库连接 (Neon PostgreSQL)
DATABASE_URL=postgresql://neondb_owner:npg_PmW2NjJiw6XZ@ep-morning-heart-ai2474q9-pooler.c-4.us-east-1.aws.neon.tech/neondb?sslmode=require

# API 服务器配置
API_HOST=0.0.0.0
API_PORT=8080

# CORS 允许的源（前端 Vercel 地址）
CORS_ORIGINS=https://xianyu-frontend.vercel.app

# Docker 环境标识
DOCKER_ENV=true

# 闲鱼 Cookie（需要手动填入）
# COOKIES_STR=你的闲鱼Cookie
ENVEOF
echo ".env 文件创建完成"

# 5. Docker 构建
echo "[5/6] Docker 构建镜像（可能需要几分钟）..."
docker build -t xianyu-backend .

# 6. 停止旧容器并启动新容器
echo "[6/6] 启动容器..."
docker stop xianyu-backend 2>/dev/null || true
docker rm xianyu-backend 2>/dev/null || true
docker run -d \
    --name xianyu-backend \
    --env-file .env \
    -p 8080:8080 \
    --restart unless-stopped \
    xianyu-backend

echo "========================================"
echo "  部署完成！"
echo "  后端地址: http://$(curl -s ifconfig.me):8080"
echo "  健康检查: curl http://localhost:8080/health"
echo "========================================"
