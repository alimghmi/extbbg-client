import os
import json
import uuid
import pprint
import logging
import requests
import datetime
import pandas as pd
from urllib.parse import urljoin

from database.mssql import MSSQLDatabase
from beap.sseclient import SSEClient
from beap.beap_auth import Credentials, BEAPAdapter, download


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(name)s:%(lineno)s]: %(message)s',
)


class Beap:

    HOST = 'https://api.bloomberg.com'
    
    def __init__(self, credential: str, isin_mode: bool = False):
        self.status = False
        self.dataframe = None
        self.priority = {}
        self.log = logging.getLogger(__name__)
        self.credential = Credentials.from_file(credential)
        self.adapter = BEAPAdapter(self.credential)
        self.session = requests.Session()
        self.session.mount('https://', self.adapter)
        self.isin_mode = isin_mode
    
        self.account_url = self.__get_catalog__()
        self.idpostfix = self.__get_id__()
    
        try:
            self.sse_client = SSEClient(urljoin(self.HOST, '/eap/notifications/sse'), self.session)
        except requests.exceptions.HTTPError as err:
            self.log.error(err)


    def __run__(self):
        pass


    def __get_catalog__(self):
        catalogs_url = urljoin(self.HOST, '/eap/catalogs/')
        response = self.session.get(catalogs_url)
        
        if not response.ok:
            self.log.error('Unexpected response status code: %s', response.status_code)
            raise RuntimeError('Unexpected response')
        
        catalogs = response.json()['contains']
        for catalog in catalogs:
            if catalog['subscriptionType'] == 'scheduled':
                self.catalog_id = catalog['identifier']
                break
        else:
            self.log.error('Scheduled catalog not in %r', response.json()['contains'])
            raise RuntimeError('Scheduled catalog not found')

        account_url = urljoin(self.HOST, '/eap/catalogs/{c}/'.format(c=self.catalog_id))
        self.log.info("Scheduled catalog URL: %s", account_url)
        return account_url


    def __create_universe__(self, title: str, ids: list):
        ids = self.__handle_tickers__(ids)
        
        universe_id = 'u' + self.idpostfix
        universe_payload = {
            '@type': 'Universe',
            'identifier': universe_id,
            'title': title,
            'description': 'description',
            'contains': ids
        }

        self.log.info('Universe component payload:\n:%s',
                pprint.pformat(universe_payload))
        
        universes_url = urljoin(self.account_url, 'universes/')
        response = self.session.post(universes_url, json=universe_payload)
        
        if response.status_code != requests.codes.created:
            self.log.error('Unexpected response status code: %s', response.status_code)
            raise RuntimeError('Unexpected response')
        
        universe_location = response.headers['Location']
        universe_url = urljoin(self.HOST, universe_location)
        self.log.info('Universe successfully created at %s', universe_url)
        return universe_url


    def __create_fieldlist__(self, title: str):
        fieldlist_id = 'f' + self.idpostfix
        fieldlist_payload = {
            '@type': 'DataFieldList',
            'identifier': fieldlist_id,
            'title': title,
            'description': 'description',
            'contains': [
                {'mnemonic': 'LAST_TRADE_TIME'},
                {'mnemonic': 'LAST_TRADE_DATE'},
                {'mnemonic': 'LAST_UPDATE'},
                {'mnemonic': 'LAST_UPDATE_DT'},
                {'mnemonic': 'PX_OPEN'},
                {'mnemonic': 'PX_HIGH'},
                {'mnemonic': 'PX_LOW'},
                {'mnemonic': 'PX_LAST'},
                {'mnemonic': 'PX_BID'},
                {'mnemonic': 'PX_ASK'},
                {'mnemonic': 'PX_VOLUME'},
                # {'mnemonic': 'CRNCY'},
                {'mnemonic': 'HIGH_52WEEK'},
                {'mnemonic': 'LOW_52WEEK'},

            ],
        }

        self.log.info('Field list component payload:\n %s',
                pprint.pformat(fieldlist_payload))
            
        fieldlists_url = urljoin(self.account_url, 'fieldLists/')
        response = self.session.post(fieldlists_url, json=fieldlist_payload)
        
        if response.status_code != requests.codes.created:
            self.log.error('Unexpected response status code: %s', response.status_code)
            raise RuntimeError('Unexpected response')
        
        fieldlist_location = response.headers['Location']
        fieldlist_url = urljoin(self.HOST, fieldlist_location)
        self.log.info('Field list successfully created at %s', fieldlist_url)
        return fieldlist_url
    

    def __create_trigger__(self):
        trigger_url = urljoin(self.account_url, 'triggers/executeNow')
        return trigger_url


    def __request__(self, title: str, universe: str, fieldlist: str, trigger: str):
        request_id = 'r' + self.idpostfix
        request_payload = {
            '@type': 'DataRequest',
            'identifier': request_id,
            'title': title,
            'description': 'description',
            'universe': universe,
            'fieldList': fieldlist,
            'trigger': trigger,
            'formatting': {
                '@type': 'MediaType',
                'outputMediaType': 'text/csv',
            },
            "terminalIdentity": {
                "@type": "BlpTerminalIdentity",
                "userNumber": 29504171,
                "serialNumber": 271249,
                "workStation": 1
            }
        }
    
        self.log.info('Request component payload:\n%s', pprint.pformat(request_payload))
        requests_url = urljoin(self.account_url, 'requests/')
        response = self.session.post(requests_url, json=request_payload)

        if response.status_code != requests.codes.created:
            self.log.error('Unexpected response status code: %s', response.status_code)
            raise RuntimeError('Unexpected response')
        
        request_location = response.headers['Location']
        request_url = urljoin(self.HOST, request_location)
        
        self.log.info('%s resource has been successfully created at %s',
                request_id,
                request_url)
        
        return request_url, request_id

    
    def __listener__(self):
        request_id = 'r' + self.idpostfix
        reply_timeout = datetime.timedelta(minutes=45)
        expiration_timestamp = datetime.datetime.utcnow() + reply_timeout
        while datetime.datetime.utcnow() < expiration_timestamp:
            event = self.sse_client.read_event()

            if event.is_heartbeat():
                self.log.info('Received heartbeat event, keep waiting for events')
                continue
        
            self.log.info('Received reply delivery notification event: %s', event)
            event_data = json.loads(event.data)
        
            try:
                distribution = event_data['generated']
                reply_url = distribution['@id']
        
                distribution_id = distribution['identifier']
                catalog = distribution['snapshot']['dataset']['catalog']
                reply_catalog_id = catalog['identifier']
            except KeyError:
                self.log.info("Received other event type, continue waiting")
            else:
                is_required_reply = '{}.csv'.format(request_id) == distribution_id
                is_same_catalog = reply_catalog_id == self.catalog_id
                
                if not is_required_reply or not is_same_catalog:
                    self.log.info("Some other delivery occurred - continue waiting")
                    continue
                
                output_file_path = os.path.join(os.path.abspath(os.getcwd()), distribution_id)
                      
                headers = {'Accept-Encoding': 'gzip'}
                download_response = download(self.session,
                                            reply_url, 
                                            output_file_path,
                                            headers = headers)
                self.log.info('Reply was downloaded')
                self.dataframe = pd.read_csv(output_file_path + '.gz', compression='gzip', header=0, sep=',', quotechar='"', error_bad_lines=False)
                self.status = True
                break
        else:
            self.log.info('Reply NOT delivered, try to increase waiter loop timeout')


    def __save__(self):
        def __get_priority__(x):
            if x in self.priority:
                return self.priority[x]
            else:
                return None

        if not self.status:
            self.log.info('Dataframe NOT found')
            return False

        order = ["recordIdentifier"
                    ,"rc"
                    ,"lastTradeTime"
                    ,"lastTradeDate"
                    ,"lastUpdate"
                    ,"lastUpdateDt"
                    ,"pxOpen"
                    ,"pxHigh"
                    ,"pxLow"
                    ,"pxLast"
                    ,"pxBid"
                    ,"pxAsk"
                    ,"pxVolume"
                    ,"crncy"
                    ,"high52Week"
                    ,"low52Week"]
            
        self.dataframe['crncy'] = None
        self.dataframe = self.dataframe[order]

        self.dataframe['lastUpdate'] = self.dataframe.apply(self.__reformat_lastupdate_dt__, axis=1)
        self.dataframe['lastTrade'] = self.dataframe['lastTradeDate'] + ' ' + self.dataframe['lastTradeTime']
        self.dataframe['timestamp_read_utc'] = self.dataframe.apply(self.__to_date__, axis=1)
        self.dataframe['timestamp_created_utc'] = datetime.datetime.utcnow()
        self.dataframe['priority'] = self.dataframe['recordIdentifier'].apply(__get_priority__)
        del self.dataframe['lastTrade']

        conn = MSSQLDatabase()
        schema = 'etl'
        name = 'extbbg_pcs'
        
        # self.dataframe.to_csv('output.sample.csv')
        self.log.info(f'Inserting data to {schema}.{name}')
        conn.__df2db__(self.dataframe, name, schema, if_exists='append')
        return True


    def __handle_tickers__(self, tickers):
        result = []
        if self.isin_mode:
            template = {
                '@type': 'Identifier',
                'identifierType': 'ISIN',
                'identifierValue': None,
            }
        else:
            template = {
                '@type': 'Identifier',
                'identifierType': 'TICKER',
                'identifierValue': None,
            }

        for k, v in tickers.items():
            pcs = v.replace(' ','').split(',')
            for i, pc in enumerate(pcs):
                isin = f'{k}@{pc}'
                x = template.copy()
                x['identifierValue'] = isin
                self.priority[isin] = i + 1
                result.append(x)
        
        return result 


    @staticmethod
    def __get_id__():
        uid = str(uuid.uuid1())[:6]
        return datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S') + uid


    @staticmethod
    def __reformat_lastupdate_dt__(x):
        a = x['lastUpdate']
        b = x['lastUpdateDt']

        if not isinstance(a, str) and not isinstance(b, str):
            return None

        a, b = str(a), str(b)
        if ':' not in a:
            try:
                lastupdate = datetime.datetime.strptime(a, "%Y%m%d")
            except ValueError:
                return None
        else:
            try:
                lastupdate = datetime.datetime.strptime(a, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                try:
                    lastupdate = f'{b} {a}'
                    lastupdate = datetime.datetime.strptime(lastupdate, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    return None

        return lastupdate.strftime("%Y-%m-%d %H:%M:%S")

        
    @staticmethod
    def __to_date__(x):
        a = x['lastUpdate']
        b = x['lastTrade']
        if not isinstance(a, str) and not isinstance(b, str):
            return None
        else:
            if not isinstance(a, str):
                x = b
            else:
                x = a

        if 'T' in x:
            x = x.replace('T', ' ')

        try:
            date = datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f")
        except ValueError:
            date = datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S")
                    
        return date.strftime("%Y-%m-%d %H:%M:%S")