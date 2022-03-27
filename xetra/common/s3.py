"""Connector and methods s3"""
import os
import boto3
import logging

class S3BucketConnector():
    """
    Interacting with s3
    """

    def __init__(self, access_key,secret_key,endpoint_url,bucket:str):

        self._logger = logging.getLogger(__name__)
        self.endpoint_url = endpoint_url
        self.session = boto3.Session(aws_access_key_id=os.environ[access_key],
        aws_secret_access_key=os.environ[secret_key])

        self._s3 = self.session.resource(service_name='s3',endpoint_url=endpoint_url)
        self._bucket = self._s3.Bucket(bucket)

    def list_files_in_prefix(self,prefix:str):
        """
        listing all files with prefix on the s3
        """
        files = [obj.key for obj in self._bucket.objects.filter(Prefix=prefix)]
        return files

    def read_csv_to_df(self):
        pass

    def write_df_to_s3(self):
        pass
