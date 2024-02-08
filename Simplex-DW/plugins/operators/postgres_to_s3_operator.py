#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import gzip
import io
import json
import os
from tempfile import NamedTemporaryFile
from typing import Optional, Union
import logging
import re
import pandas
import psycopg2.extras

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.decorators import apply_defaults
from pandas import DataFrame

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PostgresToS3Operator(BaseOperator):
    """
    Saves data from an specific Postgres query into a file in S3.

    :param query: the sql query to be executed. If you want to execute a file, place the absolute path of it,
        ending with .sql extension. (templated)
    :type query: str
    :param s3_bucket: bucket where the data will be stored. (templated)
    :type s3_bucket: str
    :param s3_key: desired key for the file. It includes the name of the file. (templated)
    :type s3_key: str
    :param postgres_conn_id: reference to a specific postgres database
    :type postgres_conn_id: str
    :param aws_conn_id: reference to a specific S3 connection
    :type aws_conn_id: str
    :param verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.
        You can provide the following values:

        - ``False``: do not validate SSL certificates. SSL will still be used
                (unless use_ssl is False), but SSL certificates will not be verified.
        - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                You can specify this argument if you want to use a different
                CA cert bundle than the one used by botocore.
    :type verify: bool or str
    :param pd_csv_kwargs: arguments to include in pd.to_csv (header, index, columns...)
    :type pd_csv_kwargs: dict
    :param index: whether to have the index or not in the dataframe
    :type index: str
    :param header: whether to include header or not into the S3 file
    :type header: bool
    """

    template_fields = (
        's3_bucket',
        's3_key',
        'query',
        'columns_to_hash'
    )

    template_ext = ('.sql',)

    @apply_defaults
    def __init__(
        self,
        *,
        query: str,
        s3_bucket: str,
        s3_key: str,
        postgres_conn_id: str = 'postgres_default',
        aws_conn_id: str = 'aws_default',
        verify: Optional[Union[bool, str]] = None,
        pd_csv_kwargs: Optional[dict] = None,
        index: bool = False,
        header: bool = False,
        batch_min_rows: int = 1,
        method: int = 1,
        columns_to_hash: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.query = query
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.postgres_conn_id = postgres_conn_id
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.batch_min_rows = batch_min_rows
        self.method = method
        self.columns_to_hash = columns_to_hash
        self.pd_csv_kwargs = pd_csv_kwargs or {}
        if "path_or_buf" in self.pd_csv_kwargs:
            raise AirflowException('The argument path_or_buf is not allowed, please remove it')
        if "index" not in self.pd_csv_kwargs:
            self.pd_csv_kwargs["index"] = index
        if "header" not in self.pd_csv_kwargs:
            self.pd_csv_kwargs["header"] = header

    def _get_pepper_value(self, sm_secretId_name: str) -> str:
        # set up Secrets Manager
        hook = AwsBaseHook(client_type='secretsmanager')
        client = hook.get_client_type('us-east-1')
        response = client.get_secret_value(SecretId=sm_secretId_name)
        pepper = response["SecretString"]

        return pepper

    def _validate_email(self, email: str):
        regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\b'
        # pass the regular expression
        # and the string into the fullmatch() method
        if email is not None:
            if len(email) == 0:
                return True
            elif re.fullmatch(regex, email):
                return True
            else:
                return False
        else:
            return False

    def _hash(self, df: DataFrame, cols) -> DataFrame:
        data_utils_db_hook = PostgresHook(postgres_conn_id="data-utils-db")
        data_utils_db_conn = data_utils_db_hook.get_conn()

        psycopg2.extras.register_default_jsonb(loads=lambda x: x)

        pepper = self._get_pepper_value("datalake/pepper")

        df['row_index'] = df.index

        for col in cols.split(","):
            temp_df = df[df[col].notnull()]
            for index, row in temp_df.iterrows():
                email_value = None
                col = col.strip()

                if "->" in col:
                    json_paths = col.split("->")
                    col = json_paths[0]
                    col = col.strip()
                    try:
                        email_value = json.loads(row[col])

                        for json_path in json_paths[1:]:
                            json_path = json_path.strip()
                            email_value = email_value[json_path]
                    except Exception as e:
                        print("error when loading json")
                        print(e)
                        print("******")
                        email_value = None
                elif col in row:
                    email_value = row[col]
                else:
                    logging.info(f"column {col} is not in row")

                if "@" in email_value:  # self._validate_email(email_value):
                    original_email_value = email_value
                    email_value = email_value.replace("'", "''")
                    if len(email_value) == 0:
                        df.at[int(row["row_index"]), col] = None
                    else:
                        cursor = data_utils_db_conn.cursor()
                        cursor.execute(f"select sp_get_hash_for_email ('{email_value}', '{pepper}')")
                        data_utils_db_conn.commit()

                        hash_value = cursor.fetchone()
                        cursor.close()

                        df.at[int(row["row_index"]), col] = str(df.at[int(row["row_index"]), col]).replace(original_email_value, str(hash_value[0]))

        df.drop('row_index', axis=1, inplace=True)

        return df

    def execute(self, context) -> None:

        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        s3_conn = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)

        psycopg2.extras.register_default_jsonb(loads=lambda x: x)

        cursor = postgres_hook.get_conn().cursor()

        logging.info(f"Executing query: {self.query}")

        copy_query = f"""COPY ({self.query}) TO STDOUT \
                            WITH (FORMAT csv, DELIMITER ',', QUOTE '"', HEADER FALSE)"""

        # ##################  method 1: using panda dataframe ####################
        if self.method == 1:

            cursor.execute(self.query)
            data_df = DataFrame(cursor.fetchall())

            if cursor.rowcount > 0:
                data_df.columns = [desc[0] for desc in cursor.description]

                logging.info("Data from Postgres obtained")

                if self.columns_to_hash is not None:
                    self._hash(data_df, self.columns_to_hash)

            with NamedTemporaryFile(mode='r+', suffix='.csv') as tmp_csv:
                data_df.to_csv(tmp_csv.name, **self.pd_csv_kwargs)
                s3_conn.load_file(filename=tmp_csv.name, key=self.s3_key, bucket_name=self.s3_bucket)

        # ##################  method 2: using copy expert (buffer - not compressed) ####################
        if self.method == 2:
            bytes_data = io.BytesIO()
            logging.info("cursor copy expert")
            cursor.copy_expert(copy_query, bytes_data, size=1024)

            logging.info(f"loading to s3://{self.s3_bucket}/{self.s3_key}")
            s3_conn.load_bytes(bytes_data=bytes_data.getvalue(), key=self.s3_key, bucket_name=self.s3_bucket)
            logging.info("Finished uploading")

        logging.info(f"checking if s3://{self.s3_bucket}/{self.s3_key} exists")
        if s3_conn.check_for_key(self.s3_key, bucket_name=self.s3_bucket):
            file_location = os.path.join(self.s3_bucket, self.s3_key)
            logging.info("File saved correctly in %s", file_location)

