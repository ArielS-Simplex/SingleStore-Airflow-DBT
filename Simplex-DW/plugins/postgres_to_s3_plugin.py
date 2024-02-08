from airflow.plugins_manager import AirflowPlugin
from operators.postgres_to_s3_operator import PostgresToS3Operator


class postgres_to_s3_operator(PostgresToS3Operator):
  pass


class postgres_to_s3_plugin(AirflowPlugin):
                    
    name = 'postgres_to_s3_plugin'
    operators = [postgres_to_s3_operator]
