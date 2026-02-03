#!/usr/bin/env python3
"""
初始化数据库脚本
"""
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base

# 加载环境变量
load_dotenv()

# 从 bot.py 导入模型
import sys
sys.path.insert(0, os.path.dirname(__file__))

from bot import Base, Config

def init_database():
    """初始化数据库"""
    print("🔧 初始化数据库...")
    
    # 确保目录存在
    Config.ensure_directories()
    
    # 创建数据库引擎
    engine = create_engine(Config.DATABASE_URL)
    
    # 创建所有表
    Base.metadata.create_all(engine)
    
    print("✅ 数据库初始化完成！")
    print(f"📊 数据库位置: {Config.DATABASE_URL}")


if __name__ == '__main__':
    init_database()
