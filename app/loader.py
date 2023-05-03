from db import mssql


class Tickers:

    def __init__(self, table_name, columns=None):
        self.df = None
        self.parsed = None
        self.table_name = table_name
        self.columns = columns

    def fetch(self):
        self.load_table()
        self.parse()
        return self.parsed

    def load_table(self):
        c = mssql.MSSQLDatabase()
        self.df = c.select_table(self.table_name, self.columns)

    def parse(self):
        pass
