import datetime

from app import client
from db import mssql


class Client(client.Client):
    def __init__(self, credential, config):
        super().__init__(credential, config)

    def _process_dataframe(self):
        self._remove_unnecessary_columns()
        self._reformat_columns()

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
