from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'
    
    check_sql_1 = """
        SELECT COUNT(*) FROM {table}
    """
    check_sql_2 = """
        SELECT COUNT(*) FROM {table} WHERE {biz_pk_col} IS NULL
    """
    check_sql_3 = """
        SELECT COUNT(*)
        FROM (SELECT {biz_pk_col},COUNT(*)
              FROM {table}
              GROUP BY {biz_pk_col}
              HAVING COUNT({biz_pk_col}) > 1) AS a
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="", # table name to check
                 biz_pk_col="", # business primary key columns
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.biz_pk_col = biz_pk_col

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        # total row count should > 0
        check_sql = DataQualityOperator.check_sql_1.format(table=self.table)
        records = redshift_hook.get_records(check_sql)
        if records[0][0] < 1:
            raise ValueError(f"Data quality check failed. {self.table} contained 0 rows")

        # business primary key should have no null value
        check_sql = DataQualityOperator.check_sql_2.format(table=self.table,biz_pk_col=self.biz_pk_col)
        records = redshift_hook.get_records(check_sql)
        if records[0][0] > 0:
            raise ValueError(f"Data quality check failed. {self.biz_pk_col} in table {self.table} have null values")

        # business primary key should have no repeat values
        check_sql = DataQualityOperator.check_sql_3.format(table=self.table,biz_pk_col=self.biz_pk_col)
        records = redshift_hook.get_records(check_sql)
        if records[0][0] > 0:
            raise ValueError(f"Data quality check failed. {self.biz_pk_col} in table {self.table} have repeat values")