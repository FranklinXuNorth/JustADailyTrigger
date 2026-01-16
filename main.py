import os
import requests
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# 配置：这里填你的数据库名和集合名
DB_NAME = "slime_vivarium"
COLLECTION_NAME = "urls"

def get_urls_from_db():
    """
    从 MongoDB 获取 URL 列表
    结构假设: 数据库中有一个文档，内容类似 {"site1": "http...", "site2": "http..."}
    """
    mongo_uri = os.environ.get("MONGODB_URI")
    if not mongo_uri:
        raise ValueError("❌ 错误: 环境变量 MONGODB_URI 未设置")

    client = None
    try:
        # 1. 连接 MongoDB
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ping') # 测试连接
        
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        
        # 2. 获取唯一的那个文档 (find_one)
        # 如果你有很多文档，这里只会取第一条。建议保持集合里只有这一条配置数据。
        config_doc = collection.find_one()
        
        if not config_doc:
            print("⚠️ 数据库为空，未找到配置文档")
            return []

        url_list = []
        
        # 3. 遍历字典 (Dict)
        for key, value in config_doc.items():
            # 排除 MongoDB 自动生成的 _id 字段
            if key == "_id":
                continue
            
            # 简单的校验：必须是字符串且以 http 开头
            if isinstance(value, str) and value.startswith("http"):
                print(f"🔎 发现目标 [{key}]: {value}")
                url_list.append(value)
            else:
                # 忽略非 URL 的字段 (比如你可能以后会加 updated_at 之类的字段)
                pass
        
        return url_list

    except ConnectionFailure:
        print("❌ 无法连接到 MongoDB 服务器")
        return []
    except Exception as e:
        print(f"❌ 数据库读取发生未知错误: {e}")
        return []
    finally:
        if client:
            client.close()

def main():
    """
    主程序：遍历列表并激活 API
    """
    print("🚀 开始执行每日激活任务 (Dict 版)...")
    
    target_urls = get_urls_from_db()
    
    if not target_urls:
        print("⚠️ 列表为空或未找到有效 URL，任务结束。")
        return

    print(f"📋 待激活 URL 总数: {len(target_urls)}\n")

    success_count = 0
    for url in target_urls:
        try:
            # 发送请求
            response = requests.get(url, timeout=10)
            
            if response.status_code < 400:
                print(f"✅ [成功] {url} - Status: {response.status_code}")
                success_count += 1
            else:
                print(f"⚠️ [异常] {url} - Status: {response.status_code}")
                
        except Exception as e:
            print(f"❌ [失败] {url} - Error: {e}")

    print(f"\n🎉 任务完成! 成功激活: {success_count}/{len(target_urls)}")

if __name__ == "__main__":
    main()