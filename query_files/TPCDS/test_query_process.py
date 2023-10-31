import pickle

processed_queries = []
file_path = "queries.pickle"
with open(file_path, 'rb') as file:
    queries = pickle.load(file)
for query in queries:
    # Query 预处理
    query = query.replace("limit 100", "")
    query = query.replace("limit 20", "")
    query = query.replace("limit 10", "")
    query = query.strip()
    processed_queries.append(query)

print(processed_queries[0])