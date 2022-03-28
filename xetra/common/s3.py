"""Connector and methods s3"""
import os

import boto3
import logging

import pandas as pd
from io import BytesIO, StringIO

from xetra.common.constants import S3FileTypes
from xetra.common.custom_exceptions import WrongFormatException

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



    def read_csv_to_df(self, key,decoding='utf-8',sep=','):
        """
        read csv from s3 bucket and return a dataframe
        """
        self._logger.info("Reading file %s/%s/%s",self.endpoint_url,self._bucket.name,key)
        csv_obj = self._bucket.Object(key = key).get().get('Body').read().decode(decoding)
        data = StringIO(csv_obj)
        df = pd.read_csv(data,delimiter= sep )
        return df

    def write_df_to_s3(self,df:pd.DataFrame,key,file_format):
        """
        writing df to s3
        """

        if df.empty:
            self._logger.info('The dataframe is empty! No file will be written!')
        elif file_format == S3FileTypes.CSV.value:
            out_buffer = StringIO()
            df.to_csv(out_buffer,index=False)
            return self.__put_object(out_buffer,key)
        elif file_format == S3FileTypes.PARQUET.value:
            out_buffer = BytesIO()
            df.to_parquet(out_buffer,index=False)
            return self.__put_object(out_buffer,key)

        else:
            self._logger.info(f'The file format {file_format} is not supported to be written to s3!')
            raise WrongFormatException

    def __put_object(self,out_buffer:StringIO or BytesIO, key:str):
        self._logger.info('Writing file to %s/%s/%s/',self.endpoint_url,self._bucket.name,key)
        self._bucket.put_object(Body = out_buffer.getvalue(),Key=key)
        return True
        

