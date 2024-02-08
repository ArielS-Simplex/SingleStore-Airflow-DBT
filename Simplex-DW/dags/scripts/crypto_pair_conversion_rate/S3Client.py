from botocore.client import BaseClient
from boto3 import client
import json
import re


class S3PathDecorator:

    def __init__(self, path: str):
        self._bucket, self._key = re\
            .match(r's3:\/\/(.+?)\/(.+)', path)\
            .groups()

    @property
    def bucket(self):
        return self._bucket

    @property
    def key(self):
        return self._key

    @property
    def prefix(self):
        pattern = re.match(r'(.+?)\//*', self.key)

        prefix = '' if pattern is None else f'{pattern.group(1)}/'

        return f's3://{self.bucket}/{prefix}'

    @property
    def full_path(self):
        return f's3://{self.bucket}/{self.key}'


class S3Client:
    DEFAULT_HOST = ''

    def __init__(self, s3client: BaseClient):
        self._client = s3client

    def _upload(self, data, path: str):
        decorator = S3PathDecorator(path)

        self._client.put_object(
            Body=data,
            Bucket=decorator.bucket,
            Key=decorator.key
        )

    def get_object(self, path: str, decode='latin1') -> str:
        decorator = S3PathDecorator(path)

        response = self._client.get_object(
            Bucket=decorator.bucket,
            Key=decorator.key
        )

        return response\
            .get('Body')\
            .read()\
            .decode(decode)

    def download(self, path: str):
        decorator = S3PathDecorator(path)

        self._client.download_file(
            Bucket=decorator.bucket,
            Key=decorator.key
        )

    def copy(self):
        pass

    def upload_from_json(self, data, path: str):
        return self._upload(
            json.dumps(data),
            path
        )

    def upload_from_string(self):
        pass

    def upload_from_path(self):
        pass

    def list(self, path: str, recursive=True):
        decorator = S3PathDecorator(path)

        children = [child for child in self._client.list_objects(Bucket=decorator.bucket,
                                                                 Prefix=decorator.key)]

    @classmethod
    def connect(cls, aws_access_key_id=None, aws_secret_key_id=None, host=None):
        host = host if host else cls.DEFAULT_HOST

        return cls(client(service_name='s3',
                          endpoint_url=host,
                          aws_access_key_id=aws_access_key_id,
                          aws_secret_key_id_key_id=aws_secret_key_id))

    @classmethod
    def connect(cls):
        return cls(client(service_name='s3'))






