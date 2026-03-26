import os
import json
import time
import re
from openai import OpenAI
from neo4j import GraphDatabase

# --- 配置区 ---
DEEPSEEK_API_KEY = "YOUR_API_KEY" # 请填入你的真实 API KEY
NEO4J_URI = "bolt://localhost:7687"
NEO4J_AUTH = ("neo4j", "Ljy449155760")
TXT_FILE_PATH = "中国共产党简史.txt"

client = OpenAI(api_key=DEEPSEEK_API_KEY, base_url="https://api.deepseek.com")

class HistoryKGBuilder:
    def __init__(self, uri, auth):
        self.driver = GraphDatabase.driver(uri, auth=auth)

    def close(self):
        self.driver.close()

    def save_node_and_points(self, chapter_title, summary, points):
        with self.driver.session() as session:
            session.execute_write(self._create_kg, chapter_title, summary, points)

    @staticmethod
    def _create_kg(tx, title, summary, points):
        # 创建章节主节点，并加上 system: 'history' 标签
        tx.run("""
            MERGE (c:Chapter {title: $title})
            SET c.summary = $summary, c.system = 'history'
        """, title=title, summary=summary)

        # 创建细分的知识要点节点，同样加上 system: 'history'
        for i, p in enumerate(points):
            tx.run("""
                MATCH (c:Chapter {title: $title})
                MERGE (p:Point {content: $content})
                SET p.index = $idx, p.system = 'history'
                MERGE (c)-[r:HAS_DETAIL]->(p)
                SET r.system = 'history'
            """, title=title, content=p, idx=i + 1)

# --- AI 提取函数 (保持不变) ---
def extract_struct_data(text_segment):
    prompt = f"""
    你是一个历史专家。请分析以下《中国共产党简史》的片段，提取结构化信息：
    1. chapter_title: 这段内容所属的章节或核心事件标题。
    2. summary: 用一句话概括这段历史的核心意义。
    3. points: 提取3-5个关键知识点（如时间、地点、决策、意义），每条简短。

    返回 JSON 格式：
    {{"chapter_title": "xxx", "summary": "xxx", "points": ["...", "..."]}}

    文本内容：{text_segment[:3000]}
    """
    try:
        response = client.chat.completions.create(
            model="deepseek-chat",
            messages=[{"role": "user", "content": prompt}],
            response_format={'type': 'json_object'}
        )
        return json.loads(response.choices[0].message.content)
    except:
        return None

# --- 主程序 (保持不变) ---
def run_processing():
    builder = HistoryKGBuilder(NEO4J_URI, NEO4J_AUTH)

    with open(TXT_FILE_PATH, 'r', encoding='utf-8') as f:
        full_text = f.read()

    chapters = re.split(r'(第[一二三四五六七八九十]+章)', full_text)
    history_index = []
    timestamp = time.strftime("%Y%m%d_%H%M%S")

    print(f"开始处理，预计分析 {len(chapters) // 2} 个章节...")

    for i in range(1, len(chapters), 2):
        chap_name = chapters[i]
        chap_content = chapters[i + 1] if i + 1 < len(chapters) else ""

        print(f"正在通过 DeepSeek 细分: {chap_name}")
        res = extract_struct_data(chap_content)

        if res:
            builder.save_node_and_points(res['chapter_title'], res['summary'], res['points'])
            history_index.append(res)

        time.sleep(1)

    index_filename = f"history_{timestamp}.json"
    with open(index_filename, 'w', encoding='utf-8') as f:
        json.dump(history_index, f, ensure_ascii=False, indent=4)

    print(f"✅ 处理完成！\n1. 图谱已更新到 Neo4j\n2. 索引文件已保存至: {index_filename}")
    builder.close()

if __name__ == "__main__":
    run_processing()