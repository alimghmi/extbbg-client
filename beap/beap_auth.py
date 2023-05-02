"""
User credential and auth token module
See [README](../README.html) for more details.

All materials Copyright 2021 Bloomberg Finance L.P. For use, distribution
and/or publication solely in accordance with applicable license agreements
entered into between Bloomberg Finance L.P. or its affiliates and end user.
All rights not expressly granted thereunder are hereby strictly reserved.
"""

import binascii
import datetime
import errno
import io
import json
import logging
import os
import sys
import time
import uuid

# Cope with python2/3 differences
try:
    import http.client as http_lib
except ImportError:
    import httplib as http_lib

try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

import jwt

import pkg_resources

import requests
import requests.adapters
import requests.packages
from urllib3.util.retry import Retry

LOG = logging.getLogger(__name__)

DAYS_IN_MONTH = 30
EXPIRE_WARNING_THRESHOLD = datetime.timedelta(days=DAYS_IN_MONTH)
REGION = "default"
FILES_ENCODING = "utf-8"
JWT_LIFETIME = 25
JWT_MAX_CLOCK_SKEW = 180

PYTHON = sys.version_info

MIN_V3 = (3, 5)
MIN_V2 = (2, 7)

VERSION_WARNING = """Only the following Python versions are supported:
Python2: >= {},
Python3: >= {}
""".format(
    MIN_V2, MIN_V3
)

assert MIN_V2 <= PYTHON < (3, 0) or MIN_V3 <= PYTHON, VERSION_WARNING


class BEAPAuthError(Exception):
    """
    Base exception class for connectivity errors.
    """


class BEAPValidationError(BEAPAuthError):
    """
    Indicates that one or more of the user provided parameters were incorrect.
    """


class Credentials(object):
    """
    Class to encapsulate the client ID and secret, and methods to generate the
    JWT.
    """

    def __init__(self, client_id, client_secret):
        """
        Initialise the object directly with the client's credentials.

        :param client_id: client id created at console.bloomberg.com for
        your BEAP account
        :type client_id: str
        :param client_secret: client secret created at console.bloomberg.com
        for your BEAP account
        :type client_secret: str
        """
        self.client_id = client_id
        self.client_secret = client_secret

    @classmethod
    def from_dict(cls, credentials_data):
        """
        Create an object from a dict that has 'client_id' and 'client_secret'
        keys.

        :param credentials_data: credentials data
        :type credentials_data: collections.Mapping
        :return: created Credentials instance
        :rtype: Credentials
        """
        try:
            client_id = credentials_data["client_id"]
            client_secret = credentials_data["client_secret"]
            expire_time = credentials_data["expiration_date"]
        except KeyError as key:
            message = "Credentials missing key: '{}'".format(key)
            LOG.error(message)
            raise BEAPValidationError(message)

        LOG.info("Client id: %s", client_id)

        cls._check_expiration(expire_time)
        client_secret = binascii.unhexlify(client_secret)
        return cls(client_id=client_id, client_secret=client_secret)

    @classmethod
    def from_file(cls, file_path):
        """
        Create an object from file.

        The file is assumed to contain a JSON dict with 'client_id' and
        'client_secret' keys.

        :param file_path: The path of the file to load the client credentials
        from
        :type file_path: str
        """
        try:
            with io.open(file_path, encoding=FILES_ENCODING) as credential_file:
                decoded_credential = json.load(credential_file)
        except IOError:
            LOG.error("Cannot open credential file %r", file_path)
            raise
        except ValueError:
            LOG.error("Cannot read credential file %r", file_path)
            raise
        else:
            LOG.info("Credential file %r loaded", file_path)
            return cls.from_dict(decoded_credential)

    @classmethod
    def _check_expiration(cls, expires_at):
        """
        Check expiration time to notify users that their credentials have
        expired or are about to expire.

        :param expires_at: non-parsed expiration time value
        :type expires_at: str
        """
        try:
            expires_at = int(expires_at)
        except ValueError:
            message = "Bad credentials expiration date format: '{}'".format(expires_at)
            LOG.error(message)
            raise BEAPValidationError(message)

        expires_at = datetime.datetime.fromtimestamp(expires_at / 1000)
        now = datetime.datetime.utcnow()
        expires_in = expires_at - now
        if expires_at < now:
            LOG.warning("Credentials expired %s ago", abs(expires_in))
        elif expires_in < EXPIRE_WARNING_THRESHOLD:
            LOG.warning("Credentials expiring in %s", expires_in)

    def generate_token(self, path, method, host, region=REGION):
        """
        Generates a single-use BEAP compliant JWT access token that is valid for
        25 seconds.

        :param path: Path of the endpoint that the JWT will be used to access
        :type path: str
        :param method: The HTTPMethod that the token will be used with. For a
                       list of available methods see `HTTPMethods` above
        :type method: str
        :param host: The BEAP host being accessed
        :type host: str
        :param region: The account region
        :type region: str
        :returns: The generated access token
        :rtype: str
        :raises BEAPValidationError: If any of the parameters are invalid
        """
        now = time.time()
        payload = {
            "iss": self.client_id,
            "iat": int(now - JWT_MAX_CLOCK_SKEW),
            "nbf": int(now - JWT_MAX_CLOCK_SKEW),
            "exp": int(now + JWT_MAX_CLOCK_SKEW + JWT_LIFETIME),
            "region": region,
            "path": path,
            "method": method,
            "host": host,
            "jti": str(uuid.uuid4()),
        }
        key = self.client_secret

        return jwt.encode(payload, key)


class BEAPAdapter(requests.adapters.HTTPAdapter):
    """
    Requests adapter for connectivity group token signing.

    Note: this class automatically signs JWT tokens on HTTP redirects either.
    Note: this class provides defaults for retry and exponential back-off that
    can be overridden by constructor parameters.
    """

    def __init__(
        self,
        credentials,
        api_version="2",
        retry_max_attempt_number=3,
        retry_backoff_factor=1,
        *args,
        **kwargs
    ):
        """
        Initialize the connectivity adapter directly with a ``Credentials``
        tuple.

        :param credentials: A (client_id, client_secret) pair
        :type credentials: ``Credentials``
        :param retry_max_attempt_number: Maximum number of retry attempts on 429 response
        :type retry_max_attempt_number: int
        :param retry_backoff_factor: Multiplier factor for exponential back-off strategy:
               {delay} = {backoff retry_backoff_factor} * (2 ** ({number of total retries} - 1))
        :type retry_backoff_factor: int
        """
        logging.getLogger("urllib3.util.retry").setLevel(logging.DEBUG)
        retry_strategy = Retry(
            total=retry_max_attempt_number,
            status_forcelist=[429],
            backoff_factor=retry_backoff_factor,
        )
        super(BEAPAdapter, self).__init__(max_retries=retry_strategy, *args, **kwargs)
        self.credentials = credentials
        self.api_version = api_version

    def send(self, request, **kwargs):
        """
        Inject JWT tokens in every outgoing HTTP request.

        :param request: HTTP request about to be send to BEAP
        :type request: requests.Request
        :param kwargs: 'requests' library parameters, such as method and url.
        :type kwargs: dict
        :return: HTTP response for provided request
        :type: requests.Response
        """
        url = urlparse(request.url)
        token = self.credentials.generate_token(url.path, request.method, url.hostname)
        request.headers["JWT"] = token
        request.headers["api-version"] = self.api_version

        LOG.info(
            "Request being sent to HTTP server: %s, %s, %s",
            request.method,
            request.url,
            request.headers,
        )

        response = super(BEAPAdapter, self).send(request, **kwargs)

        latest_api_version = response.headers.get("latest-api-version")
        if latest_api_version and self.api_version != latest_api_version:
            LOG.info(
                "You are using HAPI version %s; " "however, version %s is available",
                self.api_version,
                latest_api_version,
            )

        if response.status_code in (
            requests.codes.forbidden,
            requests.codes.unauthorized,
        ):
            LOG.error(
                "Either supplied credentials are invalid or expired, "
                "or the requesting IP address is not on the allowlist."
            )

        if LOG.isEnabledFor(logging.INFO):
            if response.is_redirect:
                LOG.info(
                    "Redirecting to %s://%s%s",
                    url.scheme,
                    url.hostname,
                    response.headers.get("Location"),
                )
            else:
                LOG.info("Request: %s, %s", request.method, request.url)
                status_message = http_lib.responses[response.status_code]

                LOG.info(
                    "Response status: %s - %s", response.status_code, status_message
                )
                LOG.info(
                    "Response x-request-id: %s", response.headers.get("x-request-id")
                )

                stream = kwargs.get("stream")

                if not stream and LOG.isEnabledFor(logging.INFO):
                    LOG.info("Response content: %s", response.text)

        return response


def download(session_, url_, out_path, chunk_size=2048, stream=True, headers=None):
    """
    Function to download the data to an output directory.

    This function allows user to specify the output location of this download
    and works for a single endpoint.

    Add 'Accept-Encoding: gzip' header to reduce download time.
    Note that the vast majority of dataset files exceed 100MB in size,
    so compression will speed up downloading significantly.

    Set 'chunk_size' to a larger byte size to speed up download process on
    larger downloads.
    """
    headers = headers or {"Accept-Encoding": "gzip"}
    with session_.get(url_, stream=stream, headers=headers) as response_:
        response_.raise_for_status()

        parent_path = os.path.dirname(out_path)
        try:
            os.makedirs(parent_path)
        except OSError as err:
            if err.errno != errno.EEXIST:
                LOG.exception("Could not create output directory %s", parent_path)
                raise

        if "gzip" in response_.headers["Content-Encoding"]:
            out_path = "{out}.gz".format(out=out_path)

        with open(out_path, "wb") as out_file:
            LOG.info("Loading file from: %s (can take a while) ...", url_)
            for chunk in response_.raw.stream(chunk_size, decode_content=False):
                out_file.write(chunk)

            LOG.info(
                "\tContent-Disposition: %s", response_.headers["Content-Disposition"]
            )
            LOG.info("\tContent-Encoding: %s", response_.headers["Content-Encoding"])
            LOG.info("\tContent-Length: %s bytes", response_.headers["Content-Length"])
            LOG.info("\tContent-Type: %s", response_.headers["Content-Type"])
            LOG.info("\tFile downloaded to: %s", out_path)
            return response_
