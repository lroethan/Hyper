def _retrieve_query_texts():
    query_files = [
        open(f"./TPCDS_{file_number}.txt", "r")
        for file_number in range(1, 100)
    ]

    finished_queries = []
    for query_file in query_files:
        queries = query_file.readlines()[:1]
        queries = _preprocess_queries(queries)

        finished_queries.append(queries)

        query_file.close()

    assert len(finished_queries) == 99

    print(finished_queries[0])


def _preprocess_queries(queries):
    processed_queries = []
    for query in queries:
        query = query.replace("limit 100", "")
        query = query.replace("limit 20", "")
        query = query.replace("limit 10", "")
        query = query.strip()

        processed_queries.append(query)

    return processed_queries


_retrieve_query_texts()