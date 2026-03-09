import os
import sqlite3
from datetime import datetime, timedelta, timezone
import googleapiclient.discovery

# --- 配置区 ---

# 从 GitHub Secrets 读取 API Key
API_KEY = os.getenv("YOUTUBE_API_KEY")
# 数据库文件名
DB_FILE = "youtube_data.db"
# 要监控的 YouTube 视频分类 ID
VIDEO_CATEGORY_IDS = {
    "Music": "10",
    "Gaming": "20",
    "Science & Technology": "28",
}
# 筛选逻辑：只保留最近 7 天内发布的视频
DAYS_TO_FILTER = 7

# --- 数据库操作 ---

def init_db():
    """初始化数据库，创建视频数据表"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS videos (
            id TEXT PRIMARY KEY,
            title TEXT,
            channelTitle TEXT,
            publishedAt TEXT,
            viewCount INTEGER,
            likeCount INTEGER,
            commentCount INTEGER,
            category_id TEXT,
            fetched_at TEXT
        )
    ''')
    conn.commit()
    conn.close()
    print("数据库初始化完成。")

def insert_videos(videos_data):
    """将视频数据批量插入数据库"""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # 使用 INSERT OR REPLACE，如果视频已存在（基于主键id），则更新数据
    cursor.executemany('''
        INSERT OR REPLACE INTO videos (id, title, channelTitle, publishedAt, viewCount, likeCount, commentCount, category_id, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', videos_data)
    
    conn.commit()
    conn.close()

# --- YouTube API 操作 ---

def fetch_popular_videos():
    """从 YouTube API 获取各分类的热门视频列表"""
    youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=API_KEY)
    
    all_videos = []
    
    for category_name, category_id in VIDEO_CATEGORY_IDS.items():
        print(f"正在获取分类 '{category_name}' (ID: {category_id}) 的热门视频...")
        request = youtube.videos().list(
            part="snippet,statistics",
            chart="mostPopular",
            regionCode="US",  # 获取美国区域的热门视频作为参考
            videoCategoryId=category_id,
            maxResults=20  # 每个分类获取前 20 个热门视频
        )
        response = request.execute()
        
        for item in response.get("items", []):
            all_videos.append((item, category_id))
            
    print(f"API 数据获取完成，共拉取 {len(all_videos)} 条原始视频数据。")
    return all_videos

# --- 主逻辑 ---

def main():
    """主执行函数"""
    if not API_KEY:
        print("错误：未找到 YOUTUBE_API_KEY 环境变量。请确保已在 GitHub Secrets 中配置。")
        return

    init_db()
    
    raw_videos = fetch_popular_videos()
    
    videos_to_insert = []
    seven_days_ago = datetime.now(timezone.utc) - timedelta(days=DAYS_TO_FILTER)
    
    print(f"开始筛选发布于 {seven_days_ago.strftime('%Y-%m-%d')} 之后的视频...")

    for video, category_id in raw_videos:
        snippet = video.get("snippet", {})
        statistics = video.get("statistics", {})
        
        published_at_str = snippet.get("publishedAt")
        # 将 YouTube API 返回的 ISO 8601 格式时间字符串转换为 datetime 对象
        published_at_dt = datetime.fromisoformat(published_at_str.replace('Z', '+00:00'))
        
        # 筛选发布时间在最近7天内的视频
        if published_at_dt >= seven_days_ago:
            video_data = (
                video.get("id"),
                snippet.get("title"),
                snippet.get("channelTitle"),
                snippet.get("publishedAt"),
                int(statistics.get("viewCount", 0)),
                int(statistics.get("likeCount", 0)),
                int(statistics.get("commentCount", 0)),
                category_id,
                datetime.now(timezone.utc).isoformat() # 记录获取数据的时间
            )
            videos_to_insert.append(video_data)
            
    if videos_to_insert:
        insert_videos(videos_to_insert)
        print(f"筛选完成，成功向数据库存入 {len(videos_to_insert)} 条最近7天内的热门视频。")
    else:
        print("筛选完成，没有找到最近7天内发布的新热门视频。")

if __name__ == "__main__":
    main()
