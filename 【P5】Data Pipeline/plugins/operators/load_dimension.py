from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 target_table="",
                 mode = "delete-load", # append-only or delete-load
                 sql="",
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.mode = mode
        self.sql=sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.mode == 'delete-load':
            self.log.info(f'Clearing DIM table {self.target_table}')
            redshift.run(f'Truncate Table {self.target_table}')
        self.log.info(f'Inserting DIM table {self.target_table}')
        redshift.run(self.sql)
       

        
