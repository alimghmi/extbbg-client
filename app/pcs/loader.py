from app.loader import Tickers


class PCSTickers(Tickers):
    
    def parse(self):
        records = self.df.to_dict('records')
        self.parsed = {row.values[0]: row.values[1] for row in records}