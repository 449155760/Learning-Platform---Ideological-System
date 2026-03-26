import requests
import urllib3
import time
import re
import csv
import json
import os
from bs4 import BeautifulSoup
from neo4j import GraphDatabase
from openai import OpenAI

# --- 基础配置 ---
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- 1. 配置信息 (根据实际修改) ---
DEEPSEEK_API_KEY = "sk-1c3015b0b0e34bafbe72c78f69ea32f5"
NEO4J_URI = "bolt://localhost:7687"
NEO4J_AUTH = ("neo4j", "Ljy449155760")

client = OpenAI(api_key=DEEPSEEK_API_KEY, base_url="https://api.deepseek.com")


class PartyKGBuilder:
    def __init__(self, uri, auth):
        self.driver = GraphDatabase.driver(uri, auth=auth)

    def close(self):
        self.driver.close()

    def import_to_neo4j(self, item):
        """将单个文章及其所有 Point 节点存入 Neo4j"""
        with self.driver.session() as session:
            session.execute_write(self._create_nodes, item)

    @staticmethod
    def _create_nodes(tx, item):
        # 1. 创建层级结构
        tx.run("MERGE (r:Ideology {name: '习近平新时代中国特色社会主义思想'})")
        tx.run("""
            MATCH (r:Ideology {name: '习近平新时代中国特色社会主义思想'})
            MERGE (c:Category {name: $cat})
            MERGE (r)-[:CONTAINS]->(c)
            MERGE (a:Article {title: $title})
            SET a.summary = $summary, a.url = $url
            MERGE (c)-[:HAS_CONTENT]->(a)
        """, cat=item['category'], title=item['title'], summary=item['summary'], url=item['url'])

        # 2. 为每个要点创建独立节点并连线
        for i, p_text in enumerate(item['points'], 1):
            tx.run("""
                MATCH (a:Article {title: $title})
                MERGE (p:Point {content: $p_content})
                SET p.index = $idx, p.source = $title
                MERGE (a)-[:HAS_POINT {order: $idx}]->(p)
            """, title=item['title'], p_content=p_text, idx=i)


# --- 2. DeepSeek 结构化提取 ---
def llm_extract_points(title, content):
    """调用 DeepSeek 提炼核心摘要和具体要点清单"""
    prompt = f"""
    任务：从给定的政务文本中提取知识点。
    要求输出 JSON 格式：
    1. summary: 用一句话概括“{title}”的核心定义。
    2. points: 这是一个列表。请提取文中的具体条目（如1. 2. 3. 或 坚持...、明确...）。
       如果文中没有明显数字编号，请根据语义提炼出最核心的3-5个短句。

    文本内容：{content[:2500]}
    """
    try:
        response = client.chat.completions.create(
            model="deepseek-chat",
            messages=[{"role": "user", "content": prompt}],
            response_format={'type': 'json_object'},
            timeout=60
        )
        return json.loads(response.choices[0].message.content)
    except Exception as e:
        print(f"   ❌ AI 提取失败 ({title}): {e}")
        return {"summary": "提取失败", "points": []}


# --- 3. 爬虫逻辑 ---
def get_main_list():
    url = "https://www.12371.cn/special/xxzd/hxnr/"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36...'}
    res = requests.get(url, headers=headers, verify=False)
    res.encoding = 'utf-8'
    soup = BeautifulSoup(res.text, 'html.parser')

    nodes = []
    # 抓取三个主要板块
    selectors = [
        ('.dyw1058-list-01 a', "核心内容"),
        ('.dyw1058-ind02 a', "领域思想"),
        ('.dyw1058-ind03 a', "重要思想论断")
    ]
    for sel, cat in selectors:
        for a in soup.select(sel):
            nodes.append({
                "title": a.get_text(strip=True).replace('\n', ''),
                "url": a.get('href') if a.get('href').startswith('http') else "https://www.12371.cn" + a.get('href'),
                "category": cat
            })
    return nodes


def run_pipeline():
    # 1. 获取目录
    raw_nodes = get_main_list()
    print(f"✅ 找到 {len(raw_nodes)} 个待处理课题。")

    builder = PartyKGBuilder(NEO4J_URI, NEO4J_AUTH)
    all_record_data = []  # 用于存入 CSV

    # 2. 深度处理 (示例处理前 10 条，可自行修改为全量)
    for i, node in enumerate(raw_nodes[:10], 1):
        print(f"🚀 [{i}/{len(raw_nodes)}] 正在深度处理: {node['title']}")

        try:
            # 抓取详情页正文
            detail_res = requests.get(node['url'], timeout=15, verify=False)
            detail_res.encoding = 'utf-8'
            # 优先匹配正文容器
            d_soup = BeautifulSoup(detail_res.text, 'html.parser')
            content = ""
            target = d_soup.select_one('.dyw1027new-catalogue-content') or d_soup.select_one(
                '.word') or d_soup.select_one('.con')
            content = target.get_text(strip=True) if target else d_soup.get_text(strip=True)

            # AI 提炼
            structured = llm_extract_points(node['title'], content)
            node.update(structured)

            # 写入 Neo4j
            builder.import_to_neo4j(node)

            # 准备 CSV 扁平化数据：每个 point 占一行
            for idx, p in enumerate(node['points'], 1):
                all_record_data.append({
                    "分类": node['category'],
                    "文章标题": node['title'],
                    "文章摘要": node['summary'],
                    "要点序号": idx,
                    "具体要点内容": p,
                    "来源链接": node['url']
                })

            time.sleep(1)  # 频率控制
        except Exception as e:
            print(f"   ⚠️ 跳过 {node['title']}, 原因: {e}")

    builder.close()
    return all_record_data


# --- 4. CSV 持久化 ---
def save_to_point_csv(data):
    if not data: return
    file_name = f"Point_Level_Data_{time.strftime('%Y%m%d_%H%M%S')}.csv"
    keys = data[0].keys()
    with open(file_name, 'w', newline='', encoding='utf-8-sig') as f:
        dict_writer = csv.DictWriter(f, fieldnames=keys)
        dict_writer.writeheader()
        dict_writer.writerows(data)
    print(f"\n📂 细粒度 CSV 已生成: {os.path.abspath(file_name)}")


if __name__ == "__main__":
    final_data = run_pipeline()
    save_to_point_csv(final_data)
    print("\n✨ 任务圆满完成！每个 Point 已独立成点并记录。")