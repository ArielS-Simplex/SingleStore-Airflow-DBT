
class Consts:
    """ Global constants for project """

    # config
    LOCAL_CONFIG_PATH = 'config/'
    PROD_ENV = 'prod'
    STAGE_ENV = 'stage'
    LOCAL_ENV = 'local'

    # connections
    DATA_TRANSFORMATION = 'data-transformation'

    # slack_channels
    SNOWFLAKE_MONITOR_PIPE = 'slack_snowflake_monitor'
    AMPLITUDE_PIPE = 'slack_amplitude_pipe'
    PIPELINE_DEV_CHANNEL = 'slack_pipeline_dev_stage'
    SNOWFLAKE_EVENTS = 'slack_snowflake_events'
    SALESFORCE = 'salesforce_channel'

    # rpc
    POLLING_INTERVAL = 10

    # tags
    EVENTS_TAG = 'events_based_data'
    DBT = 'dbt_dags'

    # message_types
    INFO = 'info'
    WARNING = 'warning'
    ERROR = 'error'

    # bucket
    STAGE_DATA_SYNC_BUCKET = "splx-data-snowflake--stage"

    # sql tables
    LOG_TABLE = 'etl_logs'

    # method
    columns_to_hash = 1
    no_columns_to_hash = 2




