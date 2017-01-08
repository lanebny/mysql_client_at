"""
This module wraps all the MySQL-specific logic. It is the only one 
that imports connector-python. It implements a single connection class
with connect and execute methods. Clients acquire connection objects
by calling a class-level factory method, which manages a cache of 
per-database connections
"""

from __future__ import print_function
from collections import namedtuple

import mysql.connector as MYSQL

class MySqlConnection(object):

    connections = {}
    current_connection = None
    ExecuteResults = namedtuple("ExecuteResults", ["rows_returned", "rows_affected", "rows"])

    def __init__(self, dbname, user, password, host, port, autocommit, **kwargs):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.autocommit = autocommit
        self.mysql_connection = None
        self.cursor = None
        self.is_connected = False
        self.can_connect = True
        self.error_msg = None

    def _connect(self):
        try:
            self.mysql_connection = MYSQL.connection.MySQLConnection(database=self.dbname,
                                                                     user=self.user,
                                                                     password=self.password,
                                                                     host=self.host,
                                                                     port=self.port,
                                                                     autocommit=self.autocommit)
            self.mysql_connection.connect()
            self.is_connected = True
        except Exception as err:
            self.error_msg = str(err)
            self.can_connect = False
            raise

    def execute_statement(self, statement_text):
        """
        Execute the fully expanded statement and return a tuple containing
        the number of affected rows (for non SELECT), the number of rows
        returned (for SELECT), and a list of sequences, starting with the
        column names and followed by the row values
        """
        cls = type(self)
        if not self.is_connected:
            raise RuntimeError("Not connected")
        if self.cursor is None:
            self.cursor = self.mysql_connection.cursor()
        rows_affected = 0
        rows = []
        self.cursor.execute(statement_text)
        if not self.cursor.with_rows:
            rows_affected = self.cursor.rowcount
        else:
            rows.append(self.cursor.column_names)
            rows += list(self.cursor)
        return cls.ExecuteResults(len(rows), rows_affected, rows)

    @classmethod
    def connect(cls, dbname, **kwargs):
        connection = cls.connections.get(dbname, None)
        if connection is None:
            connection = MySqlConnection(dbname, **kwargs)
            cls.connections[dbname] = connection
            connection._connect()
        cls.current_connection = connection
        return connection if connection.is_connected else None

    @classmethod
    def get_current_connection(cls):
        return cls.current_connection
