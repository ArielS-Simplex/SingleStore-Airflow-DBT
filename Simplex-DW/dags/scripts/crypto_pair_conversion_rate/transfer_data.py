from scripts.crypto_pair_conversion_rate.CryptoClient import CryptoClient
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas
from scripts.commons.timestamp_converter import convert_to_standard_time, convert_to_unix_time
from scripts.commons.aws.AwsManager import AwsManager
from pandas import DataFrame
from datetime import datetime

table_name_default = 'CRYPTOCURRENCY_PAIRS_CONVERSION_RATE'


def delete_data(**context):
    snowflake_hook = SnowflakeHook(
        snowflake_conn_id='snowflake_conn',
        schema='ETL'
    )
    exec_date = datetime.fromisoformat(str(context['execution_date']))

    next_exec_date = datetime.fromisoformat(str(context['next_execution_date']))
    table_name = context['dag_run'].conf.get("table_name", table_name_default)

    query = f"""
    DELETE FROM {table_name}
    WHERE TS > '{exec_date}' AND TS <= '{next_exec_date}'
    """

    snowflake_hook.run(query)


def convert_pairs_format(response):
    data, pair = response['data'], response['pair']

    return list(map(lambda obj: {'currency_pair': pair,
                                 'price': obj['close'],
                                 'ts': convert_to_standard_time(obj['time'])}, data))


def convert_to_df(elements):
    currency_pair = 'CURRENCY_PAIR'
    price = 'PRICE'
    ts = 'TS'

    data = {currency_pair: [], price: [], ts: []}

    for element in elements:
        data[currency_pair].append(element.get(currency_pair.lower()))

        data[price].append(element.get(price.lower()))

        data[ts].append(element.get(ts.lower()))

    return DataFrame(data=data)


def transfer_data(**context):
    snowflake_hook = SnowflakeHook(
        snowflake_conn_id='snowflake_conn',
        schema='ETL'
    )

    table_name = context['dag_run'].conf.get("table_name", table_name_default)
    timeframe = context['dag_run'].conf.get("timeframe", 'histominute')
    data_limit = context['dag_run'].conf.get("data_limit", 9)

    next_exc_date = context['next_execution_date']

    until_ts = convert_to_unix_time(str(next_exc_date))

    cryptoClient = CryptoClient.connect(until_ts=until_ts)

    s3_currencies_response = AwsManager.get_config(folder="cryptocurrency_pair_conversion_rate/", file_name="currencies"
                                                   , file_type="json")

    currencies = s3_currencies_response.get('currencies')
    currencies_pairs = list(map(lambda pair: pair.split('/'), currencies))

    historical_pairs = map(
        lambda pair: cryptoClient.get_historical_pair(pair[0], pair[1], timeframe=timeframe, data_limit=data_limit),
        currencies_pairs)

    converted_pairs = map(convert_pairs_format, historical_pairs)

    data = [sub for item in converted_pairs for sub in item]

    df = convert_to_df(data)

    write_pandas(snowflake_hook.get_conn(), df, table_name)
