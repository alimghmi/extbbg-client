from app import loader


class Tickers(loader.Tickers):
    def __init__(self, table_name, columns=None):
        super().__init__(table_name, columns)

    def parse(self):
        records = self.df.to_dict("records")
        self.parsed = {list(row.values())[0]: list(row.values())[1] for row in records}
