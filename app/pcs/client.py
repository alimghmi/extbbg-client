import datetime
from app import client


class Client(client.Client):
    def __init__(self, credential, config):
        super().__init__(credential, config)
        self.priority = {}

    def request(self, universe, field, trigger):
        payload = {
            "@type": "DataRequest",
            "identifier": None,
            "title": self.config['app_name'],
            "description": self.config['description'],
            "universe": universe,
            "fieldList": field,
            "trigger": trigger,
            "formatting": {
                "@type": "MediaType",
                "outputMediaType": "application/json",
            },
            "terminalIdentity": {
                "@type": "BlpTerminalIdentity",
                "userNumber": 29504171,
                "serialNumber": 271249,
                "workStation": 1,
            },
        }
        return self._request(payload)

    def save(self):
        if not self.status or not len(self.dataframe):
            self.log.info("Dataframe NOT found")
            return False

        self._process_dataframe()
        self._save_dataframe_to_database()
        return True

    def _process_dataframe(self):
        self._add_new_columns()
        self._remove_unnecessary_columns()
        self._reformat_columns()

    def _add_new_columns(self):
        self.dataframe["crncy"] = None
        self.dataframe["timestamp_created_utc"] = datetime.datetime.utcnow()

    def _remove_unnecessary_columns(self):
        columns = [
            'IDENTIFIER',
            'RC',
            'LAST_TRADE_TIME',
            'LAST_TRADE_DATE',
            'LAST_UPDATE',
            'LAST_UPDATE_DT',
            'PX_OPEN',
            'PX_HIGH',
            'PX_LOW',
            'PX_LAST',
            'PX_BID',
            'PX_ASK',
            'PX_VOLUME',
            'crncy',
            'HIGH_52WEEK',
            'LOW_52WEEK'
        ]

        for c in self.dataframe.columns:
            if c not in columns:
                self.dataframe = self.dataframe.drop(columns=[c])
        
        self.dataframe = self.dataframe[columns]

    def _reformat_columns(self):
        get_priority = lambda x: (self.priority[x] if x in self.priority else None)
        self.dataframe["LAST_UPDATE"] = self.dataframe.apply(self._reformat_last_update, axis=1
        )
        self.dataframe["lastTrade"] = (
            self.dataframe["LAST_TRADE_DATE"] + " " + self.dataframe["LAST_TRADE_TIME"]
        )
        self.dataframe['timestamp_read_utc'] = self.dataframe.apply(self.to_date, axis=1)
        self.dataframe["priority"] = self.dataframe["IDENTIFIER"].apply(get_priority)
        del self.dataframe["lastTrade"]

    def _save_dataframe_to_database(self):
        table = self.config['output_table']
        self.log.info(f"Inserting data to {table}")
        self.dataframe.to_csv(f'{table}.sample.csv')

    def generate_identifier_values(self, tickers):
        result = []
        template = self.create_identifier_template('ISIN' if self.config['is_identifier_isin'] else 'TICKER')

        for key, value in tickers.items():
            tickers_list = value.replace(' ', '').split(',')
            for index, ticker in enumerate(tickers_list):
                identifier_value = f'{key}@{ticker}'
                identifier = template.copy()
                identifier['identifierValue'] = identifier_value
                self.priority[identifier_value] = index + 1
                result.append(identifier)

        return result

    def parse_tickers(self, tickers):
        return self.generate_identifier_values(tickers)

    @staticmethod
    def _parse_date(date_str, formats):
        for fmt in formats:
            try:
                return datetime.datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        return None

    @staticmethod
    def to_date(row):
        x, y = row['LAST_UPDATE'], row['lastTrade']
        date_str = x if isinstance(x, str) else y if isinstance(y, str) else None

        if date_str is None:
            return None

        date_str = date_str.replace('T', ' ')
        date = Client._parse_date(date_str, ["%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"])

        return date.strftime("%Y-%m-%d %H:%M:%S") if date else None

    @staticmethod
    def _reformat_last_update(row):
        x, y = row['LAST_UPDATE'], row['LAST_UPDATE_DT']

        if not isinstance(x, str) and not isinstance(y, str):
            return None

        if ':' not in x:
            return Client._parse_date(x, ["%Y%m%d"])
        else:
            date = Client._parse_date(x, ["%Y-%m-%d %H:%M:%S"])
            if not date:
                date = Client._parse_date(f'{y} {x}', ["%Y-%m-%d %H:%M:%S"])

        return date.strftime("%Y-%m-%d %H:%M:%S") if date else None