import os
import fnmatch
import boto3

from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class UploadToS3Operator(BaseOperator):
    ui_color = '#89DA59'
    
    @apply_defaults
    def __init__(self,
                 path = "",
                 exclude = "", # file pattern to exclude
                 bucket = "",
                 aws_credentials_id="",
                 *args, **kwargs):
        super(UploadToS3Operator, self).__init__(*args, **kwargs)
        aws_hook = AwsHook(aws_credentials_id)
        aws_credentials = aws_hook.get_credentials()
        self.path = path
        self.exclude = exclude
        self.bucket = bucket
        self.aws_key = aws_credentials.access_key
        self.aws_secret = aws_credentials.secret_key

    def execute(self, context):
        session = boto3.Session(
            aws_access_key_id = self.aws_key,
            aws_secret_access_key = self.aws_secret,
            region_name = 'us-west-2'
        )
        s3 = session.resource('s3')
        bucket = s3.Bucket(self.bucket)
        
        for subdir, dirs, files in os.walk(self.path):
            for file in files:
                full_path = os.path.join(subdir, file)
                if not fnmatch.fnmatch(full_path,self.exclude):
                    with open(full_path, 'rb') as data:
                        relative_path = full_path[len(self.path.rstrip('/')) + 1:]
                        bucket.put_object(Key = relative_path, Body = data)
                        print(f'{relative_path} uploaded.')
        print('all files uploaded.')