import pickle

file_path = "queries.sql"
queries = []

with open(file_path, 'r') as file:
    query = ""
    for line in file:
        line = line.strip()
        if line.startswith('--') or not line:
            continue  # 跳过注释行和空行
        if line.endswith(';'):
            query += line
            queries.append(query)
            query = ""
        else:
            query += line + " "

# 打印标记后的SQL语句
for i, query in enumerate(queries, start=1):
    print(f"Query {i}: {query}")

with open("queries.pickle", "wb") as f:
    pickle.dump(queries, f)
