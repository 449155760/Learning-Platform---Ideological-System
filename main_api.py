from fastapi import FastAPI
from neo4j import GraphDatabase
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# --- 1. 解决跨域问题 (非常重要：否则前端 HTML 无法访问此后端) ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- 2. Neo4j 连接配置 ---
URI = "bolt://localhost:7687"
AUTH = ("neo4j", "Ljy449155760")
driver = GraphDatabase.driver(URI, auth=AUTH)


@app.get("/get_graph")
def get_graph():
    """查询数据库并构建前端所需格式"""
    nodes = []
    links = []
    node_ids = set()

    with driver.session() as session:
        # 查询 思想 -> 分类 -> 文章 -> 要点 的全量关系
        # 使用 LIMIT 200 防止节点过多导致前端卡死
        query = "MATCH (n)-[r]->(m) RETURN n, r, m LIMIT 200"
        results = session.run(query)

        for record in results:
            for node in [record['n'], record['m']]:
                if node.id not in node_ids:
                    # 获取标签作为分类（用于前端染色）
                    label = list(node.labels)[0] if node.labels else "Unknown"

                    # 确定显示名称：优先取 title，其次取 name，最后截取 content
                    display_name = node.get('title') or node.get('name') or node.get('content', '')[:10]

                    nodes.append({
                        "id": str(node.id),
                        "name": display_name,
                        "category": label,
                        "value": node.get('content') or node.get('summary') or ""
                    })
                    node_ids.add(node.id)

            # 构建连线
            links.append({
                "source": str(record['n'].id),
                "target": str(record['m'].id),
                "value": record['r'].type
            })

    return {"nodes": nodes, "links": links}


@app.on_event("shutdown")
def shutdown_event():
    driver.close()