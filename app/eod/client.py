import datetime

from app import client
from db import mssql


class Client(client.Client):
    def __init__(self, credential, config):
        super().__init__(credential, config)

    def request(self, universe, field, trigger):
        payload = {
            "@type": "DataRequest",
            "identifier": None,
            "title": self.config["app_name"],
            "description": self.config["description"],
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
        self._remove_unnecessary_columns()
        self._reformat_columns()

    def _remove_unnecessary_columns(self):
        columns = [
            "DL_REQUEST_ID",
            "DL_REQUEST_NAME",
            "DL_SNAPSHOT_START_TIME",
            "DL_SNAPSHOT_TZ",
        ]
        for c in columns:
            if c in self.dataframe.columns:
                self.dataframe = self.dataframe.drop(columns=[c])

    def _reformat_columns(self):
        self.dataframe["LAST_UPDATE"] = self.dataframe.apply(
            self._reformat_last_update, axis=1
        )
        self.dataframe["LAST_TRADE"] = (
            self.dataframe["LAST_TRADE_DATE"] + " " + self.dataframe["LAST_TRADE_TIME"]
        )
        self.dataframe["timestamp_read_utc"] = self.dataframe.apply(
            self.to_date, axis=1
        )
        self.dataframe["timestamp_created_utc"] = datetime.datetime.utcnow()
        del self.dataframe["LAST_TRADE"]

    def _save_dataframe_to_database(self):
        table = self.config["output_table"]
        self.log.info(f"Inserting data to {table}")
        conn = mssql.MSSQLDatabase()
        conn.insert_table(self.dataframe, table)
        # self.dataframe.to_csv(f'{table}.sample.csv')

    def generate_identifier_values(self, tickers):
        result = []
        template = self.create_identifier_template(
            "ISIN" if self.config["is_identifier_isin"] else "TICKER"
        )

        for ticker in tickers:
            identifier = template.copy()
            identifier["identifierValue"] = ticker
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
        x, y = row["LAST_UPDATE"], row["LAST_TRADE"]
        date_str = x if isinstance(x, str) else y if isinstance(y, str) else None

        if date_str is None:
            return None

        date_str = date_str.replace("T", " ")
        date = Client._parse_date(
            date_str, ["%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"]
        )

        return date.strftime("%Y-%m-%d %H:%M:%S") if date else None

    @staticmethod
    def _reformat_last_update(row):
        x, y = row["LAST_UPDATE"], row["LAST_UPDATE_DT"]

        if not isinstance(x, str) and not isinstance(y, str):
            return None

        if ":" not in x:
            return Client._parse_date(x, ["%Y%m%d"])
        else:
            date = Client._parse_date(x, ["%Y-%m-%d %H:%M:%S"])
            if not date:
                date = Client._parse_date(f"{y} {x}", ["%Y-%m-%d %H:%M:%S"])

        return date.strftime("%Y-%m-%d %H:%M:%S") if date else None
