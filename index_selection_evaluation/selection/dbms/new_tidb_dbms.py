import json
import logging
import re
import time
from typing import List

import pandas as pd
import pymysql

from ..database_connector import DatabaseConnector


class TiDBDatabaseConnector2(DatabaseConnector):
    def __init__(self, db_name, autocommit=False):
        DatabaseConnector.__init__(self, db_name, autocommit=autocommit)
        self.db_system = "TiDB"
        if db_name is None:
            db_name = "test"
        self._connection = None
        self.db_name = db_name
        self._create_connection()
        
        # TODO：外部存一个
        self.hypo2info = {}
        self.hypo_oid = 0
        # {hypoid : {表名：xx，列名列表:[xx,xx,xxx], index_name: xxxx, size:xxx}}
        
        # TODO: see cost_evaluation.py for some cache opration?
        logging.debug("TiDB connector created: {}".format(db_name))

    def _create_connection(self):
        """
        _connection, _cursor
        """
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
        
    def gen_oid(self):
        oid = self.hypo_oid
        self.hypo_oid += 1
        return oid

    def hypo_create_secondary_index(self, table_name:str, cols:List[str]) -> [int, bool]:
        """
        Return hypoid
        """
        oid = self.gen_oid()
        idx_name = "hypo_%d" % (oid)
        col_str = ", ".join(cols)
        stmt = f"create index %s type hypo on %s (%s)" % (idx_name, table_name, col_str)
        self.exec_fetch(stmt)
        self.hypo2info[oid] = {"table_name": table_name, 
                               "index_name": idx_name, 
                               "columns": cols, 
                               "size": 0} # TODO: estimate the size
        print("[action] create hypo index %s" % (idx_name))
        return oid
    
    def hypo_create_columnstore_index(self, table_name:str) -> [int, bool]:
        """
        Return hypoid
        """
        oid = self.gen_oid()
        stmt = f"alter table %s set hypo tiflash replica 1" % (table_name)
        self.exec_fetch(stmt)
        self.hypo2info[oid] = {"table_name": table_name,
                               "is_tiflash": True}
        return oid
    
    def real_create_secondary_index(self, table_name:str, cols:List[str]) -> bool:
        """
        Return 是否建立成功，这个是用来最后真实的建立索引之后看建立索引之后 time delay 上的效果的，很少调用
        """
        pass    
    
    def real_create_columnstore_index(self, table_name:str) -> bool:
        """
        Return 是否建立成功，这个是用来最后真实的建立索引之后看建立索引之后 time delay 上的效果的，很少调用
        """
        pass   
    
    def real_delete_all_physical_designs(self):
        """
        很少执行
        """
        pass
    
    def hypo_delete_all_physical_designs(self):
        """
        更新 _conn 即可
        """
        pass 
    
    def hypo_delete_single_physical_design(self, hypo_id:int) -> bool:
        """
        给 hypoid 这里可以统一处理 tikv hypo 和 Tiflash hypo
        """
        hypo = self.hypo2info[hypo_id]
        if "is_tiflash" in hypo and hypo["is_tiflash"]:
            stmt = f"alter table %s set hypo tiflash replica 0" % (hypo["table_name"])
        else:
            stmt = f"drop hypo index %s on %s" % (hypo["index_name"], hypo["table_name"])
        self.exec_fetch(stmt)
        del self.hypo2info[hypo_id]
    
    def hypo_get_count(self) -> int:
        """
        len(hypo2info)
        """
        return len(self.hypo2info)
    
    def get_plan_single(slef, query:str) -> str:
        pass
    
    def get_cost_single(self, query:str) -> int:
        pass
    
    def get_cost_workload(self, workload:List[str]) -> int:
        pass
    
    def get_storage_single(self, hypo_id:str) -> int:
        pass
    
    def get_storage_whole(self, hypo_id_list) -> int:
        pass
    
    def import_data(self, table, path, delimiter="|"):
        load_sql = f"load data local infile '{path}' into table {table} fields terminated by '{delimiter}'"
        logging.info(f"load data: {load_sql}")
        self.exec_only(load_sql)

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

    def drop_database(self, database_name):
        statement = f"DROP DATABASE {database_name};"
        self.exec_only(statement)

        logging.info(f"Database {database_name} dropped")

    def create_statistics(self):
        # raise NotImplementedError
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

    def exec_query(self, query, timeout=None, cost_evaluation=False):
        # run this query and return the actual execution time
        raise Exception("use what-if API")

    def _cleanup_query(self, query):
        for query_statement in query.split(";"):
            if "drop view" in query_statement:
                self.exec_only(query_statement)

    # def _get_cost(self, query):
    #     query_plan = self._get_plan(query)
    #     cost = query_plan[0][2]
    #     return float(cost)

    # def _get_plan(self, query):
    #     query_text = self._prepare_query(query)
    #     statement = f"explain format='verbose' {query_text}"
    #     query_plan = self.exec_fetch(statement, False)
    #     for line in query_plan:
    #         if "stats:pseudo" in line[5]:
    #             print("plan with pseudo stats " + str(query_plan))
    #     self._cleanup_query(query)
    #     return query_plan

    # def execute_create_hypo(self, index):
    #     return self._simulate_index(index, )

    # def execute_delete_hypo(self, ident):
    #     # ident 是指 表名.列名
    #     return self._drop_simulated_index(ident)

    # def get_queries_cost(self, query_list):
    #     cost_list: List[float] = list()
    #     for i, query in enumerate(query_list):
    #         query_plan = self._get_plan(query)
    #         cost = query_plan[0][2]
    #         cost_list.append(float(cost))
    #     return cost_list

    def get_tables(self):
        result = self.exec_fetch("show tables", False)
        return [x[0] for x in result]

    # def delete_indexes(self):
    #     # Delete all hypo PD
    #     tables = self.get_tables()
    #     for table in tables:
    #         # 1. Delete all hypo tiflash
    #         statement = f"alter table {table} set hypo tiflash replica 0"
    #         self.exec_only(statement)
    #         # 2. Delete all hypo index
    #         indexes = self.show_simulated_index(table)
    #         for index in indexes:
    #             self.execute_delete_hypo(index)

    # def all_simulated_indexes(self):
    #     res = []
    #     tables = self.get_tables()
    #     for table in tables:
    #         indexes = self.show_simulated_index(table)
    #         for index in indexes:
    #             res.append(index)
    #     return res

    # For TiDBCostEvaluation
    # hypo_oid should have a transformation
    # def estimate_index_size(self, hypo_oid):
    #     return self.get_index_size(hypo_oid)

    # @staticmethod
    # def get_storage_cost(oid_list):
    #     costs = list()
    #     for i, oid in enumerate(oid_list):
    #         cost_long = 0
    #         costs.append(cost_long)
    #         # print(cost_long)
    #     return costs

    # def _get_cost(self, query):
    #     query_plan = self._get_plan(query)
    #     total_cost = query_plan["Total Cost"]
    #     return total_cost

    # def get_raw_plan(self, query):
    #     query_text = self._prepare_query(query)
    #     statement = f"explain (format json) {query_text}"
    #     query_plan = self.exec_fetch(statement)[0]
    #     self._cleanup_query(query)
    #     return query_plan

    # def _get_plan(self, query):
    #     query_text = self._prepare_query(query)
    #     statement = f"explain (format json) {query_text}"
    #     query_plan = self.exec_fetch(statement)[0][0]["Plan"]
    #     self._cleanup_query(query)
    #     return query_plan