import uuid
import requests as req
import logging as log
from typing import Dict, Any
from scripts.commons.consts import Consts as cnst

from airflow.hooks.base import BaseHook


class RpcManager:
    """ RPC connector for dbt models scheduling """

    @staticmethod
    def __get_service_credentials():
        """ Get url and port for rpc service """
        host = BaseHook.get_connection(cnst.DATA_TRANSFORMATION).host
        port = BaseHook.get_connection(cnst.DATA_TRANSFORMATION).port
        return {
            'url': host + ':' + str(port) + '/jsonrpc'
        }

    @staticmethod
    def run(dbt_command: str) -> str:
        """ Run cli request on rpc server and return request_token"""
        url = RpcManager.__get_service_credentials().get('url')
        payload = {
            "jsonrpc": "2.0",
            "method": "cli_args",
            "id": f'"{uuid.uuid4()}"',  # generate random id for rpc request
            "params": {
                "cli": f'{dbt_command}'
            }
        }
        try:
            log.info(f'Sending dbt command "{dbt_command}" to service on {url}')
            resp_raw = req.post(url=url, json=payload)
            log.info(f'Response returned from service:')
            RpcManager.__parse_response(resp_raw.text)
            resp = resp_raw.json()
            if 'result' in resp.keys():
                req_result = resp.get('result')
                req_token = req_result.get('request_token')
                return req_token
            else:
                raise Exception(f'Got response with status error. See full response in message above')
        except Exception as e:
            raise Exception(f"Error in rpc cli request: {e}")

    @staticmethod
    def poll(req_token: str) -> Dict[str, Any]:
        """ Poll dbt request identified by req_token"""
        url = RpcManager.__get_service_credentials().get('url')
        payload = {
            "jsonrpc": "2.0",
            "method": "poll",
            "id": f'"{uuid.uuid4()}"',  # generate random id for rpc request
            "params": {
                "request_token": f"{req_token}",
                "logs": True,
                "logs_start": 0
            }
        }
        try:
            log.info(f'Polling service {url} with request token {req_token}')
            resp_raw = req.post(url=url, json=payload)
            log.info(f'Response returned from service:')
            RpcManager.__parse_response(resp_raw.text)
            resp = resp_raw.json()
            res = resp.get('result')
            if res and res.get('state') != 'failed':
                return {
                    'status': res.get('state'),
                    'start': res.get('start'),
                    'end': res.get('end'),
                    'elapsed': res.get('elapsed')
                }
            else:
                raise Exception(f"Request {req_token} failed. See full response in message above'")
        except Exception as e:
            raise Exception(f"There was a problem in request {req_token}. See full response in message above'")

    @staticmethod
    def __parse_response(resp: str) -> None:
        """ Parse response from rpc server """
        log_splitted = resp.split("\\n")
        for log_line in log_splitted:
            log.info(f'>> {log_line} ')
