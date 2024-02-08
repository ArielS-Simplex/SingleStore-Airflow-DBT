import os
import logging as log
from yaml import safe_load
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from scripts.commons.consts import Consts as cnst


class AwsManager:
    """ AWS Manager """

    @staticmethod
    def __get_config_from_s3(folder, file_name, file_type, bucket_name):
        log.info("loading config from s3")
        s3_conn = S3Hook(aws_conn_id="aws_conn", verify=True)
        config_from_s3 = s3_conn.get_key(key=f'''{folder}{file_name}.{file_type}''', bucket_name=bucket_name)
        file_content = config_from_s3.get()['Body'].read().decode('utf-8')
        content = safe_load(file_content)
        log.info("Finished loading")
        return content

    @staticmethod
    def get_config(folder, file_name, file_type):
        config = {}
        env_config = Variable.get("env_config", deserialize_json=True)
        if env_config['INFRA'] in (cnst.PROD_ENV, cnst.STAGE_ENV):
            bucket_name = env_config.get('configs_s3_bucket')
            config_path = env_config.get('configs_s3_path')
            config = AwsManager.__get_config_from_s3(config_path+folder, file_name, file_type, bucket_name)
        elif env_config['INFRA'] == cnst.LOCAL_ENV:
            with open(f'{cnst.LOCAL_CONFIG_PATH}{folder}{file_name}.{file_type}', "r") as stream:
                config = safe_load(stream)
        return config
