#!/bin/bash
# 启动脚本

echo "🚀 启动 Telegram 私信机器人..."

# 检查虚拟环境
if [ ! -d "venv" ]; then
    echo "📦 创建虚拟环境..."
    python3 -m venv venv
fi

# 激活虚拟环境
source venv/bin/activate

# 安装依赖
echo "📥 安装依赖..."
pip install -q -r requirements.txt

# 检查 .env 文件
if [ ! -f ".env" ]; then
    echo "⚠️  未找到 .env 文件，从模板创建..."
    cp .env.example .env
    echo "❗ 请编辑 .env 文件并填入配置信息"
    exit 1
fi

# 初始化数据库
echo "🔧 初始化数据库..."
python3 init_db.py

# 启动 bot
echo "▶️  启动机器人..."
python3 bot.py
