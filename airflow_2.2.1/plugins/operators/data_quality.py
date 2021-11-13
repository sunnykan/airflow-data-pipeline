from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        tables="",
        cols_check="",
        expected_values="",
        sql="",
        *args,
        **kwargs,
    ):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.cols_check = cols_check
        self.expected_values = expected_values
        self.sql = sql

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        for table in self.tables:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(
                    f"Data quality check failed. {table} returned no results"
                )
            num_records = records[0][0]
            self.log.info(f"Number of records: {num_records}")
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            logging.info(
                f"Data quality on table {table} check passed with {records[0][0]} records"
            )

        for table, col, val in zip(self.tables, self.cols_check, self.expected_values):
            records = redshift_hook.get_records(self.sql.format(table, col))
            if records[0][0] != val:
                raise ValueError(
                    f"Data quality check failed. Column {col} in table {table} does not contain expected number of rows"
                )
            logging.info(
                f"Data quality on table {table} passed. Column {col} in table {table} contains expected number of rows"
            )
