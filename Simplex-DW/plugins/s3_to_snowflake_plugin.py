from airflow.plugins_manager import AirflowPlugin
from operators.s3_to_snowflake_operator import S3ToSnowflakeOperator


class s3_to_snowflake_operator(S3ToSnowflakeOperator):
  pass


class s3_to_snowflake_plugin(AirflowPlugin):
                    
    name = 's3_to_snowflake_plugin'
    operators = [s3_to_snowflake_operator]
