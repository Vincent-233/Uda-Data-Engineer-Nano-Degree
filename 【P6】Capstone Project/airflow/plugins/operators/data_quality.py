from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SqlQueries

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 tables,
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        # total row count should > 0
        for check_table in self.tables:
            sql = SqlQueries.check_sql.format(table=check_table)
            records = redshift_hook.get_records(sql)
            if records[0][0] < 1:
                raise ValueError(f"Data quality check failed. {check_table} contained 0 rows.")
            self.log.info(f"Data quality check passed with {records[0][0]} rows.")