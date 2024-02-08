import logging as log
from airflow.models import Variable
from simple_salesforce import Salesforce, SalesforceLogin
from scripts.commons.consts import Consts


class SalesforceManager:
    """ Salesforce Load Manager """

    @staticmethod
    def get_salesforce_client() -> Salesforce:
        log.info("get config data from Airflow variable")
        salesforce_var = Variable.get("salesforce", deserialize_json=True)
        env_config = Variable.get("env_config", deserialize_json=True)
        if env_config['INFRA'] in Consts.PROD_ENV:
            salesforce_env = 'prod'
        else:
            salesforce_env = 'test'

        log.info(salesforce_var[salesforce_env]['username'])
        session_id, instance = SalesforceLogin(username=salesforce_var[salesforce_env]['username'],
                                               password=salesforce_var[salesforce_env]['password'],
                                               consumer_key=salesforce_var[salesforce_env]['consumer_key'],
                                               privatekey=salesforce_var[salesforce_env]['privatekey'],
                                               security_token=salesforce_var[salesforce_env]['security_token'],
                                               domain=salesforce_var[salesforce_env].get('domain'),
                                               )

        return Salesforce(session_id=session_id, instance=instance)
