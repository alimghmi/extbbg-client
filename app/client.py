import datetime
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

    def __init__(self, credential):
        """
        Initialize the Client class.

        Args:
            credential (str): Path to the credential file.
        """
        self.status = False
        self.dataframe = None
        self.session_id = self.random_id()
        self.account_url = self.get_catalog()
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
        except requests.exceptions.HTTPError as err:
            self.log.error(err)

    def listen(self):
        """
        Listen to events from the Bloomberg API and process them.
        """
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
                is_required_reply = "{}.csv".format(request_id) == distribution_id
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
                self.dataframe = pd.read_csv(
                    output_file_path + ".gz",
                    compression="gzip",
                    header=0,
                    sep=",",
                    quotechar='"',
                    error_bad_lines=False,
                )
                self.status = True
                break
        else:
            self.log.info("Reply NOT delivered, try to increase waiter loop timeout")

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

    def get_tigger(self):
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

    def create_universe(self, title, description, tickers):
        """
        Create a universe with the given title and tickers.
        """
        tickers = self.parse_tickers(tickers)
        universe_id = "u" + self.session_id
        universe_payload = {
            "@type": "Universe",
            "identifier": universe_id,
            "title": title,
            "description": description,
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

    def __save__(self):
        pass

    def parse_tickers(self, tickers):
        pass

    @staticmethod
    def random_id():
        """
        Generate a random session ID.
        """
        uid = str(uuid.uuid1())[:6]
        return datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S") + uid
