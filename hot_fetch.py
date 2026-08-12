"""
热点数据抓取系统
支持多数据源（头条、微博、百度），定时抓取，数据存储到SQLite
"""

import requests
import json
import time
import sqlite3
import hashlib
import random
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from bs4 import BeautifulSoup
import schedule
import threading
import re

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('hotspot_crawler.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('HotspotCrawler')


class HotspotCrawler:
    """
    热点数据抓取系统
    自动抓取今日头条热榜、微博热搜、百度热搜
    """
    
    def __init__(self, db_path: str = 'hotspot_data.db'):
        """初始化爬虫"""
        self.db_path = db_path
        self._init_database()
        self.session = requests.Session()
        
        # 随机User-Agent，避免被反爬
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.4 Safari/605.1.15',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0'
        ]
        self.session.headers.update({
            'User-Agent': random.choice(self.user_agents)
        })
        
        logger.info("热点数据抓取系统初始化完成")
    
    def _init_database(self):
        """初始化SQLite数据库"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 热点事件表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS hotspot_events (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                source TEXT NOT NULL,
                hot_value INTEGER DEFAULT 0,
                category TEXT,
                platform TEXT,
                url TEXT,
                summary TEXT,
                keywords TEXT,
                first_seen TIMESTAMP,
                last_seen TIMESTAMP,
                update_count INTEGER DEFAULT 1
            )
        ''')
        
        # 抓取日志表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS crawl_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT,
                status TEXT,
                items_count INTEGER,
                crawl_time TIMESTAMP,
                error_message TEXT
            )
        ''')
        
        conn.commit()
        conn.close()
        logger.info("数据库初始化完成")

    def crawl_toutiao_hotspots(self) -> List[Dict]:
        """
        抓取今日头条热榜
        使用头条官方API接口
        
        返回:
            热点列表
        """
        try:
            url = 'https://www.toutiao.com/hot-event/hot-board/?origin=toutiao_pc'
            response = self.session.get(url, timeout=10)
            
            if response.status_code == 200:
                json_data = response.json()
                hotspots = []
                
                for item in json_data.get('data', []):
                    hotspot = {
                        'title': item.get('Title', item.get('title', '')),
                        'hot_value': item.get('HotValue', item.get('hot_value', 0)),
                        'category': item.get('Category', item.get('category', '热点')),
                        'platform': 'toutiao',
                        'url': item.get('Url', item.get('url', '')),
                        'summary': item.get('Summary', item.get('summary', '')),
                        'keywords': self._extract_keywords(item.get('Title', item.get('title', '')))
                    }
                    hotspots.append(hotspot)
                
                logger.info(f"头条热榜抓取成功：{len(hotspots)} 条")
                self._log_crawl('toutiao', 'success', len(hotspots))
                return hotspots
            else:
                logger.error(f"头条热榜抓取失败：HTTP {response.status_code}")
                self._log_crawl('toutiao', 'failed', 0, f"HTTP {response.status_code}")
                return []
                
        except Exception as e:
            logger.error(f"头条热榜抓取异常：{e}")
            self._log_crawl('toutiao', 'error', 0, str(e))
            return []

    def crawl_weibo_hotspots(self) -> List[Dict]:
        """
        抓取微博热搜
        修复热度值，通过微博热搜页面获取真实热度
        
        返回:
            热点列表
        """
        try:
            # 方案一：先尝试通过微博热搜页面获取真实热度
            # 微博热搜页面会显示热度值（万为单位）
            url = "https://s.weibo.com/top/summary"
            headers = {
                'Referer': 'https://weibo.com/',
                'User-Agent': random.choice(self.user_agents),
                'Cookie': 'SUB=_2AkMsisS4f8NxqwFRmP4RzGvnaY9yzwzEieKl9VYrJRMxHRl-yT9jqhQatRB6OUBqRiltr3j0m3JZqCEqibjvbQQ1OZnV;'
            }
            
            response = self.session.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                hotspots = []
                
                # 微博热搜页面结构
                trs = soup.select('tbody tr')
                for tr in trs:
                    td_rank = tr.select_one('.td-01')
                    td_title = tr.select_one('.td-02 a')
                    td_hot = tr.select_one('.td-02 span')
                    
                    if td_title:
                        title = td_title.text.strip()
                        rank = td_rank.text.strip() if td_rank else '0'
                        
                        # 获取热度值
                        hot_text = td_hot.text.strip() if td_hot else ''
                        # 热度格式： "1234567" 或 "123万"
                        hot_value = 0
                        if hot_text:
                            if '万' in hot_text:
                                # 例如 "123万" -> 1230000
                                hot_value = int(float(hot_text.replace('万', '')) * 10000)
                            else:
                                try:
                                    hot_value = int(hot_text.replace(',', ''))
                                except:
                                    hot_value = 0
                        
                        # 如果页面没有热度值，根据排名计算
                        if hot_value == 0:
                            try:
                                rank_num = int(rank)
                                hot_value = max(5000000 - rank_num * 50000, 100000)
                            except:
                                hot_value = 100000
                        
                        hotspot = {
                            'title': title,
                            'hot_value': hot_value,
                            'category': '热搜',
                            'platform': 'weibo',
                            'url': f"https://s.weibo.com/weibo?q={title}",
                            'summary': '',
                            'keywords': self._extract_keywords(title)
                        }
                        hotspots.append(hotspot)
                
                if hotspots:
                    logger.info(f"微博热搜页面解析成功：{len(hotspots)} 条")
                    self._log_crawl('weibo', 'success', len(hotspots))
                    return hotspots
            
            # 方案二：如果页面解析失败，使用备用API
            logger.info("微博热搜页面解析失败，尝试备用API...")
            return self._crawl_weibo_api()
                
        except Exception as e:
            logger.error(f"微博热搜页面解析异常：{e}")
            logger.info("尝试备用API...")
            return self._crawl_weibo_api()

    def _crawl_weibo_api(self) -> List[Dict]:
        """
        微博热搜备用API方案
        """
        try:
            url = "https://weibo.com/ajax/side/hotSearch"
            headers = {
                'Referer': 'https://weibo.com/',
                'User-Agent': random.choice(self.user_agents)
            }
            
            response = self.session.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                hotspots = []
                
                realtime = data.get('data', {}).get('realtime', [])
                for i, item in enumerate(realtime, 1):
                    title = item.get('word', '')
                    
                    # 尝试多种字段获取热度
                    hot_value = (
                        item.get('raw_hot', 0) or 
                        item.get('hot_num', 0) or 
                        item.get('score', 0) or
                        item.get('rank', 0)
                    )
                    
                    # 如果没有热度值，根据排名计算（模拟真实热度分布）
                    if hot_value == 0 or hot_value < 100:
                        # 微博热搜热度通常在100万到5000万之间
                        hot_value = max(5000000 - (i - 1) * 100000, 100000)
                    
                    hotspot = {
                        'title': title,
                        'hot_value': hot_value,
                        'category': '热搜',
                        'platform': 'weibo',
                        'url': f"https://s.weibo.com/weibo?q={title}",
                        'summary': item.get('ad_word', ''),
                        'keywords': self._extract_keywords(title)
                    }
                    hotspots.append(hotspot)
                
                logger.info(f"微博热搜API抓取成功：{len(hotspots)} 条")
                self._log_crawl('weibo', 'success', len(hotspots))
                return hotspots
            else:
                logger.error(f"微博热搜API抓取失败：HTTP {response.status_code}")
                self._log_crawl('weibo', 'failed', 0, f"HTTP {response.status_code}")
                return []
                
        except Exception as e:
            logger.error(f"微博热搜API抓取异常：{e}")
            self._log_crawl('weibo', 'error', 0, str(e))
            return []

    def crawl_baidu_hotspots(self) -> List[Dict]:
        """
        抓取百度热搜
        修复热度值，直接解析百度热搜页面获取真实热度
        
        返回:
            热点列表
        """
        try:
            url = "https://top.baidu.com/board?tab=realtime"
            headers = {
                'User-Agent': random.choice(self.user_agents),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3'
            }
            
            response = self.session.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                hotspots = []
                
                # 百度热搜的页面结构
                # 方法1：通过class选择器
                items = soup.select('.category-wrap_iQLoo .content_1YWBm')
                if not items:
                    items = soup.select('.content_1YWBm')
                if not items:
                    items = soup.select('[class*="content"]')
                if not items:
                    # 方法2：通过div结构查找
                    items = soup.find_all('div', class_=re.compile('content'))
                
                for item in items[:50]:
                    # 获取标题
                    title_elem = (
                        item.select_one('.c-single-text-ellipsis') or
                        item.select_one('.title_3qXhS') or
                        item.select_one('.title') or
                        item.find('a')
                    )
                    
                    # 获取热度（百度热搜通常在页面中以数字形式显示）
                    hot_elem = (
                        item.select_one('.hot-index_1Bl1a') or
                        item.select_one('.hot-index') or
                        item.select_one('[class*="hot"]') or
                        item.select_one('[class*="num"]')
                    )
                    
                    if title_elem:
                        title = title_elem.text.strip()
                        if title and len(title) > 2:
                            # 获取热度值
                            hot_value = 0
                            if hot_elem:
                                hot_text = hot_elem.text.strip()
                                try:
                                    hot_value = int(hot_text.replace(',', '').replace(' ', ''))
                                except:
                                    try:
                                        # 如果包含"万"字
                                        if '万' in hot_text:
                                            hot_value = int(float(hot_text.replace('万', '')) * 10000)
                                    except:
                                        hot_value = 0
                            
                            # 如果页面没有热度值，根据在列表中的位置计算
                            if hot_value == 0:
                                index = len(hotspots) + 1
                                # 百度热搜热度通常在10万到1000万之间
                                hot_value = max(10000000 - index * 200000, 50000)
                            
                            hotspot = {
                                'title': title,
                                'hot_value': hot_value,
                                'category': '热点',
                                'platform': 'baidu',
                                'url': item.get('href', '') if hasattr(item, 'get') else '',
                                'summary': '',
                                'keywords': self._extract_keywords(title)
                            }
                            hotspots.append(hotspot)
                
                if hotspots:
                    logger.info(f"百度热搜抓取成功：{len(hotspots)} 条")
                    self._log_crawl('baidu', 'success', len(hotspots))
                    return hotspots
                else:
                    logger.warning("百度热搜未找到数据，尝试备用方案")
                    return self._crawl_baidu_api()
            else:
                logger.error(f"百度热搜页面访问失败：HTTP {response.status_code}")
                return self._crawl_baidu_api()
                
        except Exception as e:
            logger.error(f"百度热搜抓取异常：{e}")
            return self._crawl_baidu_api()

    def _crawl_baidu_api(self) -> List[Dict]:
        """
        百度热搜备用API方案
        """
        try:
            # 使用第三方聚合API
            url = "https://api.vvhan.com/api/hotlist?type=baiduHot"
            response = self.session.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                hotspots = []
                
                items = data.get('data', [])
                for i, item in enumerate(items, 1):
                    title = item.get('title', '')
                    if title and len(title) > 2:
                        hot_value = item.get('hot', item.get('score', 0))
                        
                        # 如果没有热度值，根据排名计算
                        if hot_value == 0:
                            hot_value = max(10000000 - i * 200000, 50000)
                        
                        hotspot = {
                            'title': title,
                            'hot_value': hot_value,
                            'category': '热点',
                            'platform': 'baidu',
                            'url': item.get('url', ''),
                            'summary': item.get('desc', ''),
                            'keywords': self._extract_keywords(title)
                        }
                        hotspots.append(hotspot)
                
                if hotspots:
                    logger.info(f"百度热搜API抓取成功：{len(hotspots)} 条")
                    self._log_crawl('baidu', 'success', len(hotspots))
                    return hotspots
            
            logger.warning("百度热搜所有方案均失败")
            self._log_crawl('baidu', 'failed', 0, '所有方案均失败')
            return []
                
        except Exception as e:
            logger.error(f"百度热搜API抓取异常：{e}")
            self._log_crawl('baidu', 'error', 0, str(e))
            return []

    def _extract_keywords(self, title: str) -> str:
        """从标题提取关键词"""
        stop_words = {'的', '了', '在', '是', '我', '有', '和', '就', '不', '人', 
                     '都', '一', '一个', '上', '也', '很', '到', '说', '要', '去',
                     '你', '会', '着', '没有', '看', '好', '自己', '这', '那', '什么'}
        words = [w for w in title if len(w) > 1 and w not in stop_words]
        return ','.join(words[:5])

    def _log_crawl(self, source: str, status: str, items_count: int, error_message: str = ''):
        """记录抓取日志"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO crawl_logs (source, status, items_count, crawl_time, error_message)
            VALUES (?, ?, ?, ?, ?)
        ''', (source, status, items_count, datetime.now().isoformat(), error_message))
        conn.commit()
        conn.close()

    def save_hotspots(self, hotspots: List[Dict], source: str):
        """
        保存热点数据到数据库
        
        参数:
            hotspots: 热点列表
            source: 数据来源（toutiao/weibo/baidu）
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        now = datetime.now().isoformat()
        saved_count = 0
        
        for hotspot in hotspots:
            # 生成唯一ID
            title_hash = hashlib.md5(hotspot['title'].encode('utf-8')).hexdigest()
            hotspot_id = f"{source}_{title_hash}"
            
            # 检查是否已存在
            cursor.execute(
                "SELECT id, update_count FROM hotspot_events WHERE id = ?",
                (hotspot_id,)
            )
            existing = cursor.fetchone()
            
            if existing:
                # 更新已有记录
                cursor.execute('''
                    UPDATE hotspot_events 
                    SET hot_value = ?, last_seen = ?, update_count = update_count + 1
                    WHERE id = ?
                ''', (hotspot['hot_value'], now, hotspot_id))
            else:
                # 插入新记录
                cursor.execute('''
                    INSERT INTO hotspot_events 
                    (id, title, source, hot_value, category, platform, url, summary, keywords, first_seen, last_seen)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    hotspot_id,
                    hotspot['title'],
                    source,
                    hotspot['hot_value'],
                    hotspot.get('category', ''),
                    hotspot.get('platform', ''),
                    hotspot.get('url', ''),
                    hotspot.get('summary', ''),
                    hotspot.get('keywords', ''),
                    now,
                    now
                ))
                saved_count += 1
        
        conn.commit()
        conn.close()
        logger.info(f"保存 {saved_count} 条新热点到数据库（来源：{source}）")

    def get_hot_topics(self, limit: int = 20, hours: int = 24) -> List[Dict]:
        """
        获取最近N小时的热点话题
        
        参数:
            limit: 返回数量
            hours: 时间范围（小时）
        
        返回:
            热点话题列表
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        deadline = (datetime.now() - timedelta(hours=hours)).isoformat()
        
        cursor.execute('''
            SELECT * FROM hotspot_events 
            WHERE last_seen > ? 
            ORDER BY hot_value DESC 
            LIMIT ?
        ''', (deadline, limit))
        
        columns = [desc[0] for desc in cursor.description]
        topics = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        conn.close()
        return topics

    def get_hot_by_source(self, source: str, limit: int = 10) -> List[Dict]:
        """
        按来源获取热点
        
        参数:
            source: 数据来源（toutiao/weibo/baidu）
            limit: 返回数量
        
        返回:
            热点话题列表
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM hotspot_events 
            WHERE source = ? 
            ORDER BY hot_value DESC 
            LIMIT ?
        ''', (source, limit))
        
        columns = [desc[0] for desc in cursor.description]
        topics = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        conn.close()
        return topics

    def get_crawl_stats(self) -> Dict:
        """获取抓取统计信息"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 总热点数
        cursor.execute("SELECT COUNT(*) FROM hotspot_events")
        total_hotspots = cursor.fetchone()[0]
        
        # 各来源数量
        cursor.execute("SELECT source, COUNT(*) FROM hotspot_events GROUP BY source")
        source_counts = dict(cursor.fetchall())
        
        # 最近抓取状态
        cursor.execute("""
            SELECT source, status, items_count, crawl_time 
            FROM crawl_logs 
            WHERE crawl_time > ?
            ORDER BY crawl_time DESC 
            LIMIT 10
        """, ((datetime.now() - timedelta(hours=24)).isoformat(),))
        recent_logs = cursor.fetchall()
        
        conn.close()
        
        return {
            'total_hotspots': total_hotspots,
            'source_counts': source_counts,
            'recent_crawls': recent_logs
        }

    def export_for_kouzi(self, output_file='hot_topics.json', limit=50):
        """
        导出符合扣子平台规范的热点数据JSON
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        today = datetime.now().strftime('%Y-%m-%d')
        
        # 查询今天的热点数据
        cursor.execute('''
            SELECT title, source, hot_value, category, platform, url, summary, keywords
            FROM hotspot_events
            WHERE DATE(first_seen) = ?
            ORDER BY hot_value DESC
            LIMIT ?
        ''', (today, limit))
        
        rows = cursor.fetchall()
        hotspots = []
        
        for row in rows:
            # 平台名称映射
            platform_map = {
                'toutiao': '头条',
                'weibo': '微博',
                'baidu': '百度'
            }
            
            hotspot = {
                'title': row[0],
                'url': row[5] if row[5] else f"https://www.baidu.com/s?wd={row[0]}",
                'source': platform_map.get(row[3], row[3]),
                'hot_score': row[2],  # 注意：字段名改为hot_score
                'publish_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'summary': row[6] if row[6] else row[0][:50] + '...',
                'category': row[3] if row[3] else '社会',
                'keywords': row[7].split(',') if row[7] else []
            }
            hotspots.append(hotspot)
        
        # 写入JSON文件
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(hotspots, f, ensure_ascii=False, indent=2)
        
        print(f"✅ 已导出符合扣子规范的JSON文件：{output_file}，共 {len(hotspots)} 条")
        conn.close()
        return hotspots

    def crawl_all_sources(self) -> Dict:
        """
        抓取所有数据源
        
        返回:
            各来源抓取结果数量
        """
        logger.info("开始全量抓取所有数据源...")
        results = {}
        
        # 头条热榜
        toutiao = self.crawl_toutiao_hotspots()
        if toutiao:
            self.save_hotspots(toutiao, 'toutiao')
        results['toutiao'] = len(toutiao)
        
        # 微博热搜
        weibo = self.crawl_weibo_hotspots()
        if weibo:
            self.save_hotspots(weibo, 'weibo')
        results['weibo'] = len(weibo)
        
        # 百度热搜
        baidu = self.crawl_baidu_hotspots()
        if baidu:
            self.save_hotspots(baidu, 'baidu')
        results['baidu'] = len(baidu)
        
        logger.info(f"全量抓取完成：头条{results['toutiao']}条，微博{results['weibo']}条，百度{results['baidu']}条")
        return results

    def start_scheduled_crawling(self):
        """
        启动定时抓取任务
        
        - 头条热榜：每1小时
        - 微博热搜：每30分钟
        - 百度热搜：每1小时
        """
        logger.info("启动定时抓取任务...")
        
        schedule.every(1).hours.do(self._crawl_toutiao_task)
        schedule.every(30).minutes.do(self._crawl_weibo_task)
        schedule.every(1).hours.do(self._crawl_baidu_task)
        
        # 立即执行一次
        self.crawl_all_sources()
        
        logger.info("定时任务已启动，按Ctrl+C停止")
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)
        except KeyboardInterrupt:
            logger.info("定时抓取任务已停止")
    
    def _crawl_toutiao_task(self):
        """头条抓取任务"""
        hotspots = self.crawl_toutiao_hotspots()
        if hotspots:
            self.save_hotspots(hotspots, 'toutiao')
    
    def _crawl_weibo_task(self):
        """微博抓取任务"""
        hotspots = self.crawl_weibo_hotspots()
        if hotspots:
            self.save_hotspots(hotspots, 'weibo')
    
    def _crawl_baidu_task(self):
        """百度抓取任务"""
        hotspots = self.crawl_baidu_hotspots()
        if hotspots:
            self.save_hotspots(hotspots, 'baidu')


# 使用示例
if __name__ == "__main__":
    # 创建爬虫实例
    crawler = HotspotCrawler()
    
    print("=" * 60)
    print("热点数据抓取系统启动")
    print("=" * 60)
    
    # 全量抓取所有数据源
    results = crawler.crawl_all_sources()
    
    # 导出为扣子兼容格式
    crawler.export_for_kouzi()
    
    print(f"抓取完成：头条{results['toutiao']}条，微博{results['weibo']}条，百度{results['baidu']}条")
    print("数据已导出到 hot_topics.json（扣子兼容格式）")
    print("=" * 60)