import datetime

from app import client
from db import mssql


class Client(client.Client):
    def __init__(self, credential, config):
        super().__init__(credential, config)
        self.priority = {}

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
        self._add_new_columns()
        self._remove_unnecessary_columns()
        self._reformat_columns()

    def _add_new_columns(self):
        self.dataframe["crncy"] = None

    def _reformat_columns(self):
        get_priority = lambda x: (self.priority[x] if x in self.priority else None)
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
        self.dataframe["priority"] = self.dataframe["IDENTIFIER"].apply(get_priority)
        del self.dataframe["LAST_TRADE"]

    def _save_dataframe_to_database(self):
        table = self.config["output_table"]
        self.log.info(f"Inserting data to {table}")
        conn = mssql.MSSQLDatabase()
        conn.insert_table(self.dataframe, table)

    def generate_identifier_values(self, tickers):
        result = []
        template = self.create_identifier_template(
            "ISIN" if self.config["is_identifier_isin"] else "TICKER"
        )

        for key, value in tickers.items():
            tickers_list = value.replace(" ", "").split(",")
            for index, ticker in enumerate(tickers_list):
                identifier_value = f"{key}@{ticker}"
                identifier = template.copy()
                identifier["identifierValue"] = identifier_value
                self.priority[identifier_value] = index + 1
                result.append(identifier)

        return result
