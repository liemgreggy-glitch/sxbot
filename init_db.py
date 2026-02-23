#!/usr/bin/env python3
"""
初始化数据库脚本
"""
import os
from dotenv import load_dotenv
from pymongo import MongoClient
import caiji

# 加载环境变量
load_dotenv()


def init_db(mongo_uri, db_name):
    """初始化 MongoDB 数据库并创建索引"""
    client = MongoClient(mongo_uri)
    db = client[db_name]
    caiji.init_collection_indexes(db)
    return db


if __name__ == '__main__':
    mongo_uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
    db_name = os.getenv('MONGODB_DATABASE', 'telegram_bot')
    print("🔧 初始化数据库...")
    client = MongoClient(mongo_uri)
    try:
        db = client[db_name]
        caiji.init_collection_indexes(db)
        print(f"✅ 数据库初始化完成！")
        print(f"📊 数据库: {db_name} @ {mongo_uri}")
    finally:
        client.close()
