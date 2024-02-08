from httpx import Client
import json
from airflow.models import Variable
import logging


class CryptoClient:
    DEFAULT_URL = 'https://min-api.cryptocompare.com/data/v2/'

    def __init__(self, client: Client, until_ts):
        self._client = client
        self._until_ts = until_ts

    def get_historical_pair(
            self,
            from_symbol: str,
            to_symbol: str,
            until_ts=None,
            exchange='CCCAGG',
            timeframe='histominute',
            data_limit=9,
    ):
        until_ts = until_ts if until_ts else self._until_ts
        api_key = Variable.get('cryptocompare_api')

        params = {'fsym': from_symbol,
                  'tsym': to_symbol,
                  'limit': data_limit,
                  'toTs': until_ts,
                  'e': exchange,
                  'api_key': api_key,
                  }

        response = self._client.get(url=timeframe, params=params)
        # logging.info(self._client.build_request(method='GET', url=timeframe, params=params))
        logging.info('API_Response ' + str(json.load(response)['Response']))

        return {'pair': f'{from_symbol.lower()}{to_symbol.lower()}',
                'data': json.load(response).get('Data').get('Data')}

    @classmethod
    def connect(cls, base_url=None, until_ts=None, api_key=None):
        base_url = base_url if base_url else cls.DEFAULT_URL

        return cls(Client(base_url=base_url), until_ts=until_ts)
