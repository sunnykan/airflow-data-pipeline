from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self, postgres_conn_id="", table="", table_cols="", sql="", *args, **kwargs
    ):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.table = table
        self.table_cols = table_cols
        self.sql = sql

    def execute(self, context):
        postgres_conn_id = self.postgres_conn_id
        redshift = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        sql = """INSERT INTO {} ({})""".format(self.table, self.table_cols) + self.sql
        redshift.run(sql)
