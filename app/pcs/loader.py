from app import loader


class Tickers(loader.Tickers):
    def __init__(self, table_name, columns=None, where=None):
        super().__init__(table_name, columns, where)

    def parse(self):
        records = self.df.to_dict("records")
        self.parsed = {list(row.values())[0]: list(row.values())[1] for row in records}
