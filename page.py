import requests
from bs4 import BeautifulSoup
from neo4j import GraphDatabase

# --- 1. Neo4j 连接配置 (根据你的 AuraDB 或本地配置修改) ---
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
        # 创建根节点
        tx.run("MERGE (r:Ideology {name: '习近平新时代中国特色社会主义思想'})")

        for item in data_list:
            # 创建分类节点（一级分类，如经济思想、法治思想）
            tx.run("""
                MERGE (c:Category {name: $cat_name})
                WITH c
                MATCH (r:Ideology {name: '习近平新时代中国特色社会主义思想'})
                MERGE (r)-[:CONTAINS]->(c)
            """, cat_name=item['category'])

            # 创建文章/专题节点
            tx.run("""
                MATCH (c:Category {name: $cat_name})
                MERGE (a:Article {title: $title})
                SET a.url = $url
                MERGE (c)-[:HAS_CONTENT]->(a)
            """, cat_name=item['category'], title=item['title'], url=item['url'])


# --- 2. 网页爬虫模块 ---
def scrape_party_site(html_content=None):
    # 如果是本地调试，可以直接传入你给我的 HTML 字符串
    # 实际运行时请使用 requests.get(url)
    soup = BeautifulSoup(html_content, 'html.parser')
    results = []

    # 解析核心体系模块 (十个明确等)
    # 对应源代码中的 .dyw1058-list-01
    core_section = soup.select_one('.dyw1058-list-01')
    if core_section:
        cat_name = "核心内容"
        for li in core_section.select('ul li a'):
            results.append({
                "category": cat_name,
                "title": li.get_text(strip=True),
                "url": li.get('href')
            })

    # 解析各领域思想 (经济、法治、文化等)
    # 对应源代码中的 .dyw1058-ind02
    domain_section = soup.select('.dyw1058-ind02 ul li a')
    for a in domain_section:
        # 清洗文本，去除换行符
        title = a.get_text(strip=True).replace('\n', '')
        results.append({
            "category": "领域思想",
            "title": title,
            "url": a.get('href')
        })

    # 解析重要思想模块 (党的建设、自我革命等)
    # 对应源代码中的 .dyw1058-ind03
    important_thought_section = soup.select('.dyw1058-ind03 ul li a')
    for a in important_thought_section:
        results.append({
            "category": "重要思想论断",
            "title": a.get_text(strip=True).replace('\n', ''),
            "url": a.get('href')
        })

    return results


# --- 3. 执行主程序 ---
if __name__ == "__main__":
    # 假设你已经把网页源码存入变量 html_doc
    # 或者直接使用 requests 抓取
    # html_doc = requests.get("https://www.12371.cn/special/xxzd/hxnr/").text

    # 这里模拟使用你提供的源码
    with open("page.html", "r", encoding="utf-8") as f:
        html_doc = f.read()

    print("开始解析网页数据...")
    extracted_data = scrape_party_site(html_doc)
    print(f"解析完成，共提取到 {len(extracted_data)} 条知识节点。")

    print("开始导入 Neo4j...")
    kg_app = PartyKnowledgeGraph(URI, AUTH)
    try:
        kg_app.import_data(extracted_data)
        print("数据导入成功！请打开 Neo4j 查看图谱。")
    finally:
        kg_app.close()