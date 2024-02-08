import logging as log
import time

from scripts.commons.dbt.RpcManager import RpcManager as rpc
from scripts.commons.consts import Consts as cnst


class DbtManager:
    """ dbt models scheduler """

    @staticmethod
    def dbt_run(dbt_command_select: str, is_test: bool = False):
        """ run dbt models """
        if is_test:
            dbt_command = f"test -s {dbt_command_select}"
        else:
            dbt_command = f"run -s {dbt_command_select}"

        try:
            req_token = rpc.run(dbt_command)
            log.info(f"Start running dbt command {dbt_command}. Request token: {req_token}")
            state = 'running'
            while state == 'running':
                time.sleep(cnst.POLLING_INTERVAL)
                result = rpc.poll(req_token)
                log.info(f"Command status: {result['status']}, elapsed time {result['elapsed']}")
                state = result['status']
            log.info(f"Finished running dbt command {dbt_command}")
        except Exception as e:
            raise Exception(f"DBT command failed. Reason: {e}")


