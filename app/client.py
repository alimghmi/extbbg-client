import datetime
import gzip
import json
import logging
import os
import pprint
import uuid
from urllib.parse import urljoin

import pandas as pd
import requests

from beap.beap_auth import BEAPAdapter, Credentials, download
from beap.sseclient import SSEClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s:%(lineno)s]: %(message)s",
)


class Client:
    HOST = "https://api.bloomberg.com"
    LISTENER_TIMEOUT_MIN = 45

    def __init__(self, credential, config):
        """
        Initialize the Client class.

        Args:
            credential (str): Path to the credential file.
        """
        self.status = False
        self.dataframe = None
        self.config = config
        self.session_id = self.random_id()
        self.log = logging.getLogger(__name__)
        self.credential = Credentials.from_file(credential)
        self.initialize_sse_client()

    def initialize_sse_client(self):
        """
        Initialize the SSEClient for listening to events.
        """
        self.adapter = BEAPAdapter(self.credential)
        self.session = requests.Session()
        self.session.mount("https://", self.adapter)
        try:
            self.sse_client = SSEClient(
                urljoin(self.HOST, "/eap/notifications/sse"), self.session
            )
            self.account_url = self.get_catalog()
        except requests.exceptions.HTTPError as err:
            self.log.error(err)

    def listen(self, file=None):
        """
        Listen to events from the Bloomberg API and process them.
        """
        if file:
            self.log.info("Reply was downloaded")
            self.log.info("Prasing the downloaded json")
            with gzip.open(file + ".gz", "rt", encoding="utf-8") as f:
                json_data = json.load(f)

            self.dataframe = pd.json_normalize(json_data)
            self.status = True
            return self.dataframe

        request_id = "r" + self.session_id
        reply_timeout = datetime.timedelta(minutes=self.LISTENER_TIMEOUT_MIN)
        expiration_timestamp = datetime.datetime.utcnow() + reply_timeout
        while datetime.datetime.utcnow() < expiration_timestamp:
            event = self.sse_client.read_event()

            if event.is_heartbeat():
                self.log.info("Received heartbeat event, keep waiting for events")
                continue

            self.log.info("Received reply delivery notification event: %s", event)
            event_data = json.loads(event.data)

            try:
                distribution = event_data["generated"]
                reply_url = distribution["@id"]

                distribution_id = distribution["identifier"]
                catalog = distribution["snapshot"]["dataset"]["catalog"]
                reply_catalog_id = catalog["identifier"]
            except KeyError:
                self.log.info("Received other event type, continue waiting")
            else:
                is_required_reply = "{}.json".format(request_id) == distribution_id
                is_same_catalog = reply_catalog_id == self.catalog_id

                if not is_required_reply or not is_same_catalog:
                    self.log.info("Some other delivery occurred - continue waiting")
                    continue

                output_file_path = os.path.join(
                    os.path.abspath(os.getcwd()), distribution_id
                )

                headers = {"Accept-Encoding": "gzip"}
                download_response = download(
                    self.session, reply_url, output_file_path, headers=headers
                )
                self.log.info("Reply was downloaded")
                self.log.info("Prasing the downloaded json")
                with gzip.open(output_file_path + ".gz", "rt", encoding="utf-8") as f:
                    json_data = json.load(f)

                self.dataframe = pd.json_normalize(json_data)
                self.status = True
                return self.dataframe
        else:
            self.log.info("Reply NOT delivered, try to increase waiter loop timeout")

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

    def _request(self, payload):
        """
        Send a data request to the Bloomberg API with the given payload.
        """
        request_id = f"r{self.session_id}"
        payload["identifier"] = request_id
        self.log.info("Request component payload:\n%s", pprint.pformat(payload))
        requests_url = urljoin(self.account_url, "requests/")
        response = self.session.post(requests_url, json=payload)

        if response.status_code != requests.codes.created:
            self.log.error("Unexpected response status code: %s", response.status_code)
            raise RuntimeError("Unexpected response")

        request_location = response.headers["Location"]
        request_url = urljoin(self.HOST, request_location)

        self.log.info(
            "%s resource has been successfully created at %s", request_id, request_url
        )

        return request_url, request_id

    def get_trigger(self):
        trigger_url = urljoin(self.account_url, "triggers/executeNow")
        return trigger_url

    def get_catalog(self):
        catalogs_url = urljoin(self.HOST, "/eap/catalogs/")
        response = self.session.get(catalogs_url)

        if not response.ok:
            self.log.error("Unexpected response status code: %s", response.status_code)
            raise RuntimeError("Unexpected response")

        catalogs = response.json()["contains"]
        for catalog in catalogs:
            if catalog["subscriptionType"] == "scheduled":
                self.catalog_id = catalog["identifier"]
                break
        else:
            self.log.error("Scheduled catalog not in %r", response.json()["contains"])
            raise RuntimeError("Scheduled catalog not found")

        account_url = urljoin(self.HOST, "/eap/catalogs/{c}/".format(c=self.catalog_id))
        self.log.info("Scheduled catalog URL: %s", account_url)
        return account_url

    def create_universe(self, tickers):
        """
        Create a universe with the given title and tickers.
        """
        tickers = self.parse_tickers(tickers)
        universe_id = "u" + self.session_id
        universe_payload = {
            "@type": "Universe",
            "identifier": universe_id,
            "title": self.config["app_name"],
            "description": self.config["description"],
            "contains": tickers,
        }

        self.log.info(
            "Universe component payload:\n:%s", pprint.pformat(universe_payload)
        )

        universes_url = urljoin(self.account_url, "universes/")
        response = self.session.post(universes_url, json=universe_payload)

        if response.status_code != requests.codes.created:
            self.log.error("Unexpected response status code: %s", response.status_code)
            raise RuntimeError("Unexpected response")

        universe_location = response.headers["Location"]
        universe_url = urljoin(self.HOST, universe_location)
        self.log.info("Universe successfully created at %s", universe_url)
        return universe_url

    def save(self):
        if not self.status or not len(self.dataframe):
            self.log.info("Dataframe NOT found")
            return False

        self._process_dataframe()
        self._save_dataframe_to_database()
        return True

    def _process_dataframe(self):
        pass

    def parse_tickers(self, tickers):
        pass

    def create_field(self, fields):
        fieldlist_id = "f" + self.session_id
        fieldlist_payload = {
            "@type": "DataFieldList",
            "identifier": fieldlist_id,
            "title": self.config["app_name"],
            "description": self.config["description"],
            "contains": fields,
        }

        self.log.info(
            "Field list component payload:\n %s", pprint.pformat(fieldlist_payload)
        )

        fieldlists_url = urljoin(self.account_url, "fieldLists/")
        response = self.session.post(fieldlists_url, json=fieldlist_payload)

        if response.status_code != requests.codes.created:
            self.log.error("Unexpected response status code: %s", response.status_code)
            raise RuntimeError("Unexpected response")

        fieldlist_location = response.headers["Location"]
        fieldlist_url = urljoin(self.HOST, fieldlist_location)
        self.log.info("Field list successfully created at %s", fieldlist_url)
        return fieldlist_url

    def _remove_unnecessary_columns(self):
        columns = self.config["delete_columns"]
        for c in columns:
            if c in self.dataframe.columns:
                self.dataframe = self.dataframe.drop(columns=[c])

    @staticmethod
    def random_id():
        """
        Generate a random session ID.
        """
        uid = str(uuid.uuid1())[:6]
        return datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S") + uid

    @staticmethod
    def create_identifier_template(identifier_type):
        return {
            "@type": "Identifier",
            "identifierType": identifier_type,
            "identifierValue": None,
        }

    def generate_identifier_values(self, tickers):
        pass

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
        date_str = x if x is not None else y if y is not None else None
        if date_str is None:
            return None

        date_str = str(date_str).replace("T", " ")
        date = Client._parse_date(
            date_str, ["%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y%m%d"]
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
