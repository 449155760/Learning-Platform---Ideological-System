import requests
import urllib3
import time
import re
import csv
import os
import ssl
from bs4 import BeautifulSoup
from neo4j import GraphDatabase
from requests.adapters import HTTPAdapter

# --- 全局配置与屏蔽 ---
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# --- SSL 兼容性适配器 (解决政务网站 SSL 报错) ---
class TLSAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        ctx.set_ciphers('DEFAULT@SECLEVEL=1')
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        kwargs['ssl_context'] = ctx
        return super(TLSAdapter, self).init_poolmanager(*args, **kwargs)


# --- 1. Neo4j 数据库操作类 ---
class PartyKnowledgeGraph:
    def __init__(self, uri, auth):
        self.driver = GraphDatabase.driver(uri, auth=auth)

    def close(self):
        self.driver.close()

    def import_detailed_data(self, item):
        with self.driver.session() as session:
            session.execute_write(self._create_rich_nodes, item)

    @staticmethod
    def _create_rich_nodes(tx, item):
        tx.run("MERGE (r:Ideology {name: '习近平新时代中国特色社会主义思想'})")
        tx.run("""
            MATCH (r:Ideology {name: '习近平新时代中国特色社会主义思想'})
            MERGE (c:Category {name: $cat_name})
            MERGE (r)-[:CONTAINS]->(c)
        """, cat_name=item['category'])
        tx.run("""
            MATCH (c:Category {name: $cat_name})
            MERGE (a:Article {title: $title})
            SET a.url = $url, a.content = $content, a.update_time = $update_time
            MERGE (c)-[:HAS_CONTENT]->(a)
        """, cat_name=item['category'], title=item['title'], url=item['url'],
               content=item['content'], update_time=item['update_time'])


# --- 2. 深度详情爬取逻辑 ---
def get_article_detail(session, url):
    """进入二级页面，智能抓取正文或专题导语"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'}
    if not url.startswith('http'):
        url = "https://www.12371.cn" + url

    try:
        res = session.get(url, headers=headers, timeout=15, verify=False)
        res.encoding = 'utf-8'
        soup = BeautifulSoup(res.text, 'html.parser')

        # 策略 1：寻找文章正文容器
        article_selectors = ['.dyw1027new-catalogue-content', '.word', '#font_area', '.con', '.text_area']
        for s in article_selectors:
            target = soup.select_one(s)
            if target:
                return target.get_text(separator="\n", strip=True)

        # 策略 2：寻找专题页导语 (针对领域思想专题)
        intro_selectors = ['.dyw1058-brief p', '.brief p', '.intro', '.main_content p']
        for s in intro_selectors:
            elements = soup.select(s)
            if elements:
                # 过滤掉“详细资料”等干扰字符
                text = "\n".join([e.get_text(strip=True) for e in elements if "详细" not in e.get_text()])
                if len(text) > 30: return text

        # 策略 3：Meta 描述兜底
        meta = soup.find("meta", attrs={"name": "description"})
        if meta:
            return "[摘要] " + meta.get("content", "")

        return "未提取到详细正文内容"
    except Exception as e:
        return f"爬取详情失败: {str(e)}"


# --- 3. 主爬虫逻辑 ---
def run_spider():
    # 配置
    base_url = "https://www.12371.cn/special/xxzd/hxnr/"
    db_config = {"uri": "bolt://localhost:7687", "auth": ("neo4j", "Ljy449155760")}

    # 建立会话
    session = requests.Session()
    session.mount('https://', TLSAdapter())

    print(f"Step 1: 正在获取一级目录...")
    try:
        res = session.get(base_url, timeout=15, verify=False)
        res.encoding = 'utf-8'
        soup = BeautifulSoup(res.text, 'html.parser')

        nodes_to_process = []
        configs = [
            ('.dyw1058-list-01 ul li a', "核心内容"),
            ('.dyw1058-ind02 ul li a', "领域思想"),
            ('.dyw1058-ind03 ul li a', "重要思想论断")
        ]

        for selector, cat in configs:
            for a in soup.select(selector):
                nodes_to_process.append({
                    "category": cat,
                    "title": a.get_text(strip=True).replace('\n', ''),
                    "url": a.get('href')
                })

        print(f"Step 2: 目录抓取成功 ({len(nodes_to_process)} 条)，开始深入详情页...")

        kg = PartyKnowledgeGraph(db_config["uri"], db_config["auth"])
        final_data = []

        for i, item in enumerate(nodes_to_process, 1):
            print(f"   进度 [{i}/{len(nodes_to_process)}] 正在深挖: {item['title']}")
            item['content'] = get_article_detail(session, item['url'])
            item['update_time'] = time.strftime('%Y-%m-%d %H:%M:%S')

            # 实时存入 Neo4j
            kg.import_detailed_data(item)
            final_data.append(item)
            time.sleep(1)  # 安全间隔

        kg.close()
        return final_data, base_url

    except Exception as e:
        print(f"致命错误: {e}")
        return [], base_url


# --- 4. CSV 保存 ---
def save_csv(url, data_list):
    clean_url = re.sub(r'[/:?&=]', '_', url.replace("https://", ""))
    file_name = f"Final_Deep_Data_{time.strftime('%Y%m%d_%H%M%S')}.csv"

    headers = ['序号', '分类', '标题', '详细资料', '原始链接', '采集时间']
    with open(file_name, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for i, item in enumerate(data_list, 1):
            # 存入 CSV 前简单清洗详情，防止换行导致乱码（虽然 utf-8-sig 较好，但建议清洗）
            clean_content = item['content'].replace('\r', '').replace('\n', ' ')
            writer.writerow([i, item['category'], item['title'], clean_content, item['url'], item['update_time']])
    print(f"✅ CSV 文件已生成: {os.path.abspath(file_name)}")


# --- 执行入口 ---
if __name__ == "__main__":
    results, source = run_spider()
    if results:
        save_csv(source, results)
        print("\n" + "*" * 30)
        print("🎉 项目运行圆满完成！")
        print("1. 本地 CSV 已生成（包含深度正文）")
        print("2. Neo4j 已同步更新（包含 content 属性）")
        print("*" * 30)