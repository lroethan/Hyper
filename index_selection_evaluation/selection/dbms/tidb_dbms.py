import logging
import re
import time
from typing import List

import pandas as pd
import pymysql

from ..database_connector import DatabaseConnector


def is_separate_line(line):
    line = line.strip()
    if len(line) == 0:
        return False
    for c in line:
        if c != "+" and c != "-":
            return False
    return True


def trim_and_split_explain_result(explain_result):
    lines = explain_result.split("\n")
    idx = [0, 0, 0]
    p = 0
    for i in range(len(lines)):
        if is_separate_line(lines[i]):
            idx[p] = i
            p += 1
            if p == 3:
                break
    if p != 3:
        raise Exception("invalid explain result")

    return lines[idx[0] : idx[2] + 1]


def split_rows(rows):
    results = []
    for row in rows:
        cols = row.split("|")
        cols = [c.strip() for c in cols[1:-1]]
        results.append(cols)
    return results


def parse_text(explain_text):
    explain_lines = trim_and_split_explain_result(explain_text)
    rows = split_rows(explain_lines[3 : len(explain_lines) - 1])
    result = {}
    for row in rows:
        db_name = row[0]
        table_name = row[1]
        column_name = row[3]
        num_dv = row[6]
        num_nv = row[7]
        column_size = row[8]
        result[table_name + "#" + column_name] = {
            "num_dv": num_dv,
            "num_nv": num_nv,
            "column_size": column_size,
        }

    return json.dumps(result)


def hist_tuple_to_dict(hist_tuple):
    """
    {'customer#C_CUSTKEY': [149568, 0, 8.0], 'customer#C_NAME': [148224, 0, 19.0]}
    """
    result = {}
    for tpl in hist_tuple:
        key = tpl[1] + "#" + tpl[3]
        key = key.lower()
        value = [tpl[6], tpl[7], tpl[8]]
        result[key] = value
    return result


def meta_tuple_to_dict(hist_tuple):
    """
    {'customer': 150000, 'lineitem': 8143998, 'nation': 25, 'orders': 1500000, 'part': 200000, 'partsupp': 800000, 'region': 5, 'supplier': 10000}
    """
    result = {}
    for tpl in hist_tuple:
        key = tpl[1]
        value = tpl[-1]
        result[key] = value
    return result


class TiDBDatabaseConnector(DatabaseConnector):
    def __init__(self, db_name, autocommit=False):
        DatabaseConnector.__init__(self, db_name, autocommit=autocommit)
        self.db_system = "TiDB"
        if db_name is None:
            db_name = "test"
        self._connection = None
        self.db_name = db_name
        self._create_connection()
        self._create_hist_and_meta()

        self.set_random_seed()
        logging.debug("TiDB connector created: {}".format(db_name))

    def _create_connection(self):
        if self._connection:
            self.close()
        self._connection = pymysql.connect(
            host="127.0.0.1",
            port=4000,
            user="root",
            password="",
            database="{}".format(self.db_name),
            local_infile=True,
        )
        self._cursor = self._connection.cursor()

    def create_connection(self):
        if self._connection:
            self.close()
        self._connection = pymysql.connect(
            host="127.0.0.1",
            port=4000,
            user="root",
            password="",
            database="{}".format(self.db_name),
            local_infile=True,
        )
        self._cursor = self._connection.cursor()


    def _create_hist_and_meta(self):
        """
        Since this connection specifies a certain database, this function will get stats_histograms and save them in the connection for the calculation of index storage
        """
        o_stats_histograms = self.exec_fetch("show stats_histograms;", one=False)
        o_stats_meta = self.exec_fetch("show stats_meta;", one=False)

        self.stats_histogram = hist_tuple_to_dict(o_stats_histograms)
        self.stats_meta = meta_tuple_to_dict(o_stats_meta)

    def enable_simulation(self):
        pass  # Do nothing

    def database_names(self):
        result = self.exec_fetch("show databases", False)
        return [x[0] for x in result]

    def update_query_text(self, text):
        return text  # Do nothing

    def _add_alias_subquery(self, query_text):
        return query_text  # Do nothing

    def create_database(self, database_name):
        self.exec_only("create database {}".format(database_name))
        logging.info("Database {} created".format(database_name))

    def import_data(self, table, path, delimiter="|"):
        load_sql = f"load data local infile '{path}' into table {table} fields terminated by '{delimiter}'"
        logging.info(f"load data: {load_sql}")
        self.exec_only(load_sql)

    def get_index_size(self, candidate):
        """
        This function is the base for storage cost calculation
        """
        table_name = candidate.split("#")[0]
        col_name = candidate.split("#")[1]

        cols = []
        if col_name == "tiflash":
            cols = [col for col in self.stats_histogram if col.startswith(table_name)]
        else:
            cols = col_name.split(",")
            cols = [table_name + "#" + col for col in cols]

        size = 0
        for col in cols:
            size += self.stats_histogram[col][-1] * self.stats_meta[table_name]

        if col_name == "tiflash":
            return size / 3  # tiflash : tikv = 1:3

        return size

    def drop_database(self, database_name):
        statement = f"DROP DATABASE {database_name};"
        self.exec_only(statement)

        logging.info(f"Database {database_name} dropped")

    def create_statistics(self):
        logging.info("TiDB: Run `analyze`")
        for table_name, table_type in self.exec_fetch("show full tables", False):
            if table_type != "BASE TABLE":
                logging.info(f"skip analyze {table_name} {table_type}")
                continue
            analyze_sql = "analyze table " + table_name
            logging.info(f"run {analyze_sql}")
            self.exec_only(analyze_sql)

            # Let the TiDB load all stats into memory
            cols = [
                col[0]
                for col in self.exec_fetch(
                    f"select column_name from information_schema.columns where table_schema='{self.db_name}' and table_name='{table_name}'",
                    False,
                )
            ]
            sql = f"explain select * from {table_name} where " + " and ".join(cols)
            self.exec_only(sql)

    def set_random_seed(self, value=0.17):
        pass  # Do nothing

    def supports_index_simulation(self):
        return True

    def show_simulated_index(self, table_name):
        sql = f"show create table {table_name}"
        result = self.exec_fetch(sql)
        hypo_indexes = []
        for line in result[1].split("\n"):
            if "HYPO INDEX" in line:
                tmp = line.split("`")
                idx_name = tmp[1]
                hypo = f"{table_name}.{idx_name}"
                hypo_indexes.append(hypo)
        return hypo_indexes

    def _simulate_tiflash(self, table_name):
        statement = f"alter table {table_name} set hypo tiflash replica 1"
        self.exec_only(statement)

    def _delete_ti_flash(self, table_name):
        statement = f"alter table {table_name} set hypo tiflash replica 0"
        self.exec_only(statement)

    def _simulate_index(self, index):
        """
        Candidate index is in the format of table_name#col1,col2,col3
        identifier is in the format of tablename.hypo_table_name_col1_col2_col3_idx
        note the difference between candidate index and identifier
        """
        schema = index.split("#")
        table_name = schema[0]
        idx_cols = schema[1]
        if idx_cols == "tiflash":
            self._simulate_tiflash(table_name)
            return (f"{table_name}.tiflash", index)

        sql_idx_cols = idx_cols.replace(",", "_")
        idx_name = f"hypo_{table_name}_{sql_idx_cols}_idx"
        if len(idx_name) >= 60:
            idx_name = idx_name[:60]

        statement = f"create index {idx_name} type hypo " f"on {table_name} ({idx_cols})"
        self.exec_fetch(statement)
        print("[action] ", index)
        return (
            f"{table_name}.{idx_name}",
            index,
        )  # return identifier and candidate index

    def _drop_simulated_index(self, ident):
        table_name = ident.split(".")[0]
        idx_name = ident.split(".")[1]
        if idx_name == "tiflash":
            self._delete_ti_flash(table_name)
            return

        self.exec_only(f"drop hypo index {idx_name} on {table_name}")

    def create_index(self, index):
        raise Exception("use what-if API")

    def drop_indexes(self):
        return  # Do nothing since we use what-if API

    def exec_query(self, query, timeout=None, cost_evaluation=False):
        # run this query and return the actual execution time
        raise Exception("use what-if API")

    def _cleanup_query(self, query):
        for query_statement in query.split(";"):
            if "drop view" in query_statement:
                self.exec_only(query_statement)

    def _get_cost(self, query):
        query_plan = self._get_plan(query)
        cost = query_plan[0][2]
        return float(cost)

    def _get_plan(self, query):
        query_text = self._prepare_query(query)
        statement = f"explain format='verbose' {query_text}"
        query_plan = self.exec_fetch(statement, False)
        for line in query_plan:
            if "stats:pseudo" in line[5]:
                print("plan with pseudo stats " + str(query_plan))
        self._cleanup_query(query)
        return query_plan

    def execute_create_hypo(self, index):
        return self._simulate_index(index)

    def execute_delete_hypo(self, ident):
        # ident 是指 表名.列名
        return self._drop_simulated_index(ident)

    def get_queries_cost(self, query_list):
        cost_list: List[float] = list()
        for i, query in enumerate(query_list):
            query_plan = self._get_plan(query)
            cost = query_plan[0][2]
            cost_list.append(float(cost))
        return cost_list

    def get_tables(self):
        result = self.exec_fetch("show tables", False)
        return [x[0] for x in result]

    def delete_indexes(self):
        # Delete all hypo PD
        tables = self.get_tables()
        for table in tables:
            # 1. Delete all hypo tiflash
            statement = f"alter table {table} set hypo tiflash replica 0"
            self.exec_only(statement)
            # 2. Delete all hypo index
            indexes = self.show_simulated_index(table)
            for index in indexes:
                self.execute_delete_hypo(index)
                
    def all_simulated_indexes(self):
        res = []
        tables = self.get_tables()
        for table in tables:
            indexes = self.show_simulated_index(table)
            for index in indexes:
                res.append(index)
                
        return res
    
    # For TiDBCostEvaluation
    # hypopg_oid should have a transformation
    def estimate_index_size(self, hypopg_oid):
        return self.get_index_size(hypopg_oid)
        

    def get_storage_cost(self, oid_list):
        costs = list()
        for i, oid in enumerate(oid_list):
            cost_long = 0
            costs.append(cost_long)
            # print(cost_long)
        return costs

