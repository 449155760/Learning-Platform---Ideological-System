import requests
import urllib3
import time
import re
import csv
import os
from bs4 import BeautifulSoup
from neo4j import GraphDatabase

# --- 屏蔽 SSL 安全警告 ---
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- 1. Neo4j 连接配置 ---
URI = "bolt://localhost:7687"
AUTH = ("neo4j", "Ljy449155760")


class PartyKnowledgeGraph:
    def __init__(self, uri, auth):
        self.driver = GraphDatabase.driver(uri, auth=auth)

    def close(self):
        self.driver.close()

    def import_data(self, data_list):
        with self.driver.session() as session:
            session.execute_write(self._create_nodes_and_rels, data_list)

    @staticmethod
    def _create_nodes_and_rels(tx, data_list):
        tx.run("MERGE (r:Ideology {name: '习近平新时代中国特色社会主义思想'})")
        for item in data_list:
            # 创建分类关系
            tx.run("""
                MERGE (c:Category {name: $cat_name})
                WITH c
                MATCH (r:Ideology {name: '习近平新时代中国特色社会主义思想'})
                MERGE (r)-[:CONTAINS]->(c)
            """, cat_name=item['category'])
            # 创建文章节点
            tx.run("""
                MATCH (c:Category {name: $cat_name})
                MERGE (a:Article {title: $title})
                SET a.url = $url
                MERGE (c)-[:HAS_CONTENT]->(a)
            """, cat_name=item['category'], title=item['title'], url=item['url'])


# --- 2. 新增：保存为 CSV 文件函数 ---
def save_to_csv(url, data_list):
    # 1. 清理网址字符作为文件名
    clean_url = re.sub(r'[/:?&=]', '_', url.replace("https://", "").replace("http://", ""))
    # 2. 生成时间戳
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    file_name = f"{clean_url}_{timestamp}.csv"

    # 定义 CSV 表头
    headers = ['序号', '所属分类', '知识点标题', '原始链接', '采集时间']

    try:
        # 使用 utf-8-sig 编码，确保 Excel 打开不乱码
        with open(file_name, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(headers)

            curr_time = time.strftime('%Y-%m-%d %H:%M:%S')
            for index, item in enumerate(data_list, 1):
                writer.writerow([
                    index,
                    item['category'],
                    item['title'],
                    item['url'],
                    curr_time
                ])
        print(f"✅ CSV 记录文件已生成: {os.path.abspath(file_name)}")
    except Exception as e:
        print(f"❌ 写入 CSV 失败: {e}")


# --- 3. 网页爬虫模块 ---
def scrape_party_site():
    target_url = "https://www.12371.cn/special/xxzd/hxnr/"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    }

    print(f"正在抓取网页数据: {target_url}")

    try:
        # verify=False 解决您之前的 SSL 报错问题
        response = requests.get(target_url, headers=headers, timeout=15, verify=False)
        response.encoding = 'utf-8'

        if response.status_code != 200:
            print(f"访问失败，状态码: {response.status_code}")
            return []

        soup = BeautifulSoup(response.text, 'html.parser')
        results = []

        # 分类抓取逻辑
        # 1. 核心内容
        for a in soup.select('.dyw1058-list-01 ul li a'):
            results.append({"category": "核心内容", "title": a.get_text(strip=True), "url": a.get('href')})
        # 2. 领域思想
        for a in soup.select('.dyw1058-ind02 ul li a'):
            results.append({"category": "领域思想", "title": a.get_text(strip=True).strip(), "url": a.get('href')})
        # 3. 重要思想论断
        for a in soup.select('.dyw1058-ind03 ul li a'):
            results.append({"category": "重要思想论断", "title": a.get_text(strip=True).strip(), "url": a.get('href')})

        # 保存为 CSV
        if results:
            save_to_csv(target_url, results)

        return results

    except Exception as e:
        print(f"抓取异常: {e}")
        return []


# --- 4. 执行主程序 ---
if __name__ == "__main__":
    data = scrape_party_site()

    if data:
        print(f"成功解析 {len(data)} 条数据，开始导入 Neo4j...")
        kg = PartyKnowledgeGraph(URI, AUTH)
        try:
            kg.import_data(data)
            print("🚀 数据已成功同步至图数据库和 CSV 文件！")
        finally:
            kg.close()
    else:
        print("未获取到数据，请检查网络或 SSL 设置。")