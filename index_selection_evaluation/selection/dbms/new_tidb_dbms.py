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
        
        self.hypo2info = {}
        self.hypo_oid = 0
        
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
        self.exec_only(stmt)
        self.hypo2info[oid] = {"table_name": table_name, 
                               "index_name": idx_name, 
                               "columns": cols}
        print("[action] create hypo index %s" % (idx_name))
        return oid
    
    def hypo_create_columnstore_index(self, table_name:str) -> [int, bool]:
        """
        Return hypoid
        """
        oid = self.gen_oid()
        stmt = f"alter table %s set hypo tiflash replica 1" % (table_name)
        self.exec_only(stmt)
        self.hypo2info[oid] = {"table_name": table_name,
                               "is_tiflash": True}
        print("[action] create hypo tiflash %s" % (table_name))
        return oid
    
    def real_create_secondary_index(self, table_name:str, cols:List[str]) -> bool:
        """
        Return 是否建立成功，这个是用来最后真实的建立索引之后看建立索引之后 time delay 上的效果的，很少调用
        """
        oid = self.gen_oid()
        idx_name = "hypo_%d" % (oid)
        col_str = ", ".join(cols)
        stmt = f"create index %s on %s (%s)" % (idx_name, table_name, col_str)
        self.exec_only(stmt)
        print("[action] create real index %s" % (idx_name))
    
    def real_create_columnstore_index(self, table_name:str) -> bool:
        """
        Return 是否建立成功，这个是用来最后真实的建立索引之后看建立索引之后 time delay 上的效果的，很少调用
        """
        stmt = f"alter table %s set hypo tiflash replica 1" % (table_name)
        self.exec_only(stmt)
        print("[action] create real tiflash %s" % (table_name))
    
    def real_delete_all_physical_designs(self):
        """
        很少执行
        """
        self.real_delete_all_physical_indexes()
        self.real_delete_all_physical_tiflashes()

    def real_delete_all_physical_indexes(self):
        stmt = f"select distinct table_name, index_name from INFORMATION_SCHEMA.STATISTICS where table_schema='%s'" % (self.db_name)
        indexes = []
        for table_name, index_name in self.exec_fetch(stmt, False):
            indexes.append((table_name, index_name))
        for table_name, index_name in indexes:
            drop_stmt = f"alter table %s.%s drop index %s" % (self.db_name, table_name, index_name)
            self.exec_only(drop_stmt)
    
    def real_delete_all_physical_tiflashes(self):
        stmt = f"select table_name from information_schema.tiflash_replica where table_schema='%s'" % (self.db_name)
        tables = []
        for table_name in self.exec_fetch(stmt, False):
            tables.append(table_name)
        for table_name in tables:
            drop_stmt = f"alter table %s.%s set tiflash replica 0" % (self.db_name, table_name)
            self.exec_only(drop_stmt)
    
    def hypo_delete_all_physical_designs(self):
        """
        更新 _conn 即可
        """
        oids = []
        for oid in self.hypo2info:
            oids.append(oid)
        for oid in oids:
            self.hypo_delete_single_physical_design(oid)
    
    def hypo_delete_single_physical_design(self, hypo_id:int) -> bool:
        """
        给 hypoid 这里可以统一处理 tikv hypo 和 Tiflash hypo
        """
        hypo = self.hypo2info[hypo_id]
        if "is_tiflash" in hypo and hypo["is_tiflash"]:
            stmt = f"alter table %s set hypo tiflash replica 0" % (hypo["table_name"])
            print("[action] drop hypo tiflash %s" % hypo["table_name"])
        else:
            stmt = f"drop hypo index %s on %s" % (hypo["index_name"], hypo["table_name"])
            print("[action] drop hypo index %s %s" % (hypo["table_name"], hypo["index_name"]))
        self.exec_fetch(stmt)
        del self.hypo2info[hypo_id]
    
    def hypo_get_count(self) -> int:
        """
        len(hypo2info)
        """
        return len(self.hypo2info)
    
    def get_plan_single(self, query):
        # query_text = self._prepare_query(query)
        statement = f"explain format='verbose' {query}"
        query_plan = self.exec_fetch(statement, False)
        for line in query_plan:
            if "stats:pseudo" in line[5]:
                print("plan with pseudo stats " + str(query_plan))
        # self._cleanup_query(query)
        return query_plan
    
    def get_cost_single(self, query:str) -> int:
        query_plan = self.get_plan_single(query)
        cost = query_plan[0][2]
        return float(cost)
    
    def get_cost_workload(self, workload:List[str]) -> int:
        cost = 0
        for q in workload:
            cost += self.get_cost_single(q)
        return cost
    
    def get_storage_single(self, hypo_id:int) -> int:
        hypo = self.hypo2info[hypo_id]
        if "is_tiflash" in hypo:
            return self.get_hypo_tiflash_storage(hypo_id)
        else:
            return self.get_hypo_index_storage(hypo_id)
    
    def get_storage_whole(self, hypo_id_list) -> int:
        storage_cost = 0
        for id in hypo_id_list:
            storage_cost += self.get_storage_single(id)
        return storage_cost
    
    def get_hypo_index_storage(self, hypo_id):
        hypo = self.hypo2info[hypo_id]
        col_size = 0
        for col in hypo["columns"]:
            col_size += self.get_column_avg_size(self.db_name, hypo["table_name"], col)
        rows = self.get_total_rows(self.db_name, hypo["table_name"])
        return rows * col_size
    
    def get_hypo_tiflash_storage(self, hypo_id):
        hypo = self.hypo2info[hypo_id]
        col_size = self.get_table_avg_size(self.db_name, hypo["table_name"])
        rows = self.get_total_rows(self.db_name, hypo["table_name"])
        compression_rate = 0.35
        return rows * col_size * compression_rate

    def get_column_avg_size(self, db_name, tbl_name, col_name):
        stmt = f"show stats_histograms where db_name='%s' and table_name='%s' and column_name='%s' and is_index=0" % (db_name, tbl_name, col_name)
        avg_size_in_byte = self.exec_fetch(stmt, True)[8]
        return float(avg_size_in_byte)
    
    def get_table_avg_size(self, db_name, tbl_name):
        stmt = f"show stats_histograms where db_name='%s' and table_name='%s' and is_index=0" % (db_name, tbl_name)
        rows = self.exec_fetch(stmt, one=False)
        col_size = 0
        for r in rows:
            col_size += float(r[8])
        return col_size

    def get_total_rows(self, db_name, tbl_name):
        stmt = f"show stats_meta where db_name='%s' and table_name='%s'" % (db_name, tbl_name)
        row_count = self.exec_fetch(stmt, True)[5]
        return float(row_count)
    
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

    def get_tables(self):
        result = self.exec_fetch("show tables", False)
        return [x[0] for x in result]
