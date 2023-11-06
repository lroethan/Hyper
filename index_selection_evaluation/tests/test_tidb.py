import unittest
from time import sleep

from selection.dbms.new_tidb_dbms import TiDBDatabaseConnector2
from selection.index import Index
from selection.table_generator import TableGenerator
from selection.workload import Column, Query, Table

class TestTiDB(unittest.TestCase):
    def test_tidb_connection(self):
        db = TiDBDatabaseConnector2("test")
        db.close()

    def test_tidb_creating(self):
        db = TiDBDatabaseConnector2("test")
        db.exec_fetch("drop table if exists t")
        db.exec_fetch("create table t (a int, b int, c int)")
        oid = db.hypo_create_secondary_index("t", ["a", "b", "c"])
        print("OID: ", oid)
        db.hypo_delete_single_physical_design(oid)
        oid = db.hypo_create_columnstore_index("t")
        db.hypo_delete_single_physical_design(oid)

        db.hypo_create_secondary_index("t", ["a", "b", "c"])
        db.hypo_create_secondary_index("t", ["a", "b", "c"])
        db.hypo_create_secondary_index("t", ["a", "b", "c"])
        db.hypo_create_columnstore_index("t")
        db.hypo_delete_all_physical_designs()
        
        db.exec_fetch("drop table t")
        db.close()

    def test_tidb_storage(self):
        db = TiDBDatabaseConnector2("test")
        # db.exec_only("drop table if exists tt")
        # db.exec_only("create table tt (a int, b int, c int)")
        # db.exec_only("insert into tt values (1, 1, 1), (2, 2, 2)")
        # db.exec_only("analyze table tt")
        oid = db.hypo_create_secondary_index("tt", ["a", "b"])
        storage = db.get_storage_single(oid)
        print(">> ", storage)

        oid = db.hypo_create_secondary_index("tt", ["a", "b", "c"])
        storage = db.get_storage_single(oid)
        print(">> ", storage)

        oid = db.hypo_create_columnstore_index("tt")
        storage = db.get_storage_single(oid)
        print(">> ", storage)

        # db.exec_fetch("drop table tt")
        pass