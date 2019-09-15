from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    # override attribute
    template_fields = ['s3_key']
    ui_color = '#358140'

    copy_sql = """
        COPY {table}
        FROM '{s3_path}'
        ACCESS_KEY_ID '{aws_key}'
        SECRET_ACCESS_KEY '{aws_secret}'
        JSON '{json_paths}'
        REGION '{region}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_paths="auto",
                 region="us-west-2",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_paths = json_paths
        self.region = region
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        aws_credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing stage table")
        redshift.run(f"Truncate Table {self.table}")

        self.log.info("Copying data from s3 to stage table")
        render_s3_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{render_s3_key}"
        exec_copy_sql = StageToRedshiftOperator.copy_sql.format(
            table=self.table,
            s3_path=s3_path,
            aws_key=aws_credentials.access_key,
            aws_secret=aws_credentials.secret_key,
            json_paths=self.json_paths,
            region=self.region,
        )
        redshift.run(exec_copy_sql)

        
        
        




