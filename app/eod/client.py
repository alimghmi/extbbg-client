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
