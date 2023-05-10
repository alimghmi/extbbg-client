from db import mssql


class Tickers:
    def __init__(self, table_name, columns=None, where=None):
        self.df = None
        self.parsed = None
        self.table_name = table_name
        self.columns = columns
        self.where = where

    def fetch(self):
        self.load_table()
        self.parse()
        return self.parsed

    def load_table(self):
        c = mssql.MSSQLDatabase()
        self.df = c.select_table(self.table_name, self.columns, self.where)

    def parse(self):
        pass
