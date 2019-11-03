from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SqlQueries

class LoadToRedshiftOperator(BaseOperator):
    # override attribute
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 region="us-west-2",
                 ignore_header = 0,
                 delimiter = "|",
                 file_format = "csv",
                 overwrite = True,
                 *args, **kwargs):

        super(LoadToRedshiftOperator, self).__init__(*args, **kwargs)
        
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.file_format = file_format
        self.ignore_header = ignore_header
        self.delimiter = delimiter
        self.overwrite = overwrite

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        con = redshift.get_connection(self.redshift_conn_id)
        iam_role = con.extra_dejson['aws_iam_role']

        if self.overwrite:
            self.log.info("Clearing stage table")
            redshift.run(f"Truncate Table {self.table}")

        self.log.info("Copying data from s3 to stage table")
        s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"

        format_parms = {
            'table':self.table,
            's3_path':s3_path,
            'iam_role':iam_role
        }
        
        if self.file_format == 'csv':
            format_parms.update({'ignore_header':self.ignore_header,
                                 'delimiter':self.delimiter,
                                 'region':self.region})
        
        exec_copy_sql = SqlQueries.copy_from_s3[self.file_format].format(**format_parms)
        redshift.run(exec_copy_sql)