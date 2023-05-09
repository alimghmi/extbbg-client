import datetime

from app import client
from db import mssql


class Client(client.Client):
    def __init__(self, credential, config):
        super().__init__(credential, config)
