import datetime
import logging

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

import sql_queries

dag = DAG(
    dag_id="populate_dwh_dag",
    start_date=datetime.datetime(2021, 11, 4, 0, 0, 0, 0),
    schedule_interval=None,
    max_active_runs=1,
)

create_tables_task = PostgresOperator(
    task_id="create_tables_task",
    dag=dag,
    postgres_conn_id="redshift",
    sql="sql/create_tables.sql",
)

start_operator = DummyOperator(task_id="Begin_execution", dag=dag)


def load_events_data_to_redshift(*args, **kwargs):
    aws_hook = AwsHook("aws_credentials")
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook("redshift")
    sql = """
        COPY {0}
        FROM '{1}'
        ACCESS_KEY_ID '{2}'
        SECRET_ACCESS_KEY '{3}'
        REGION AS '{4}'
        FORMAT as json '{5}'
    """.format(
        "staging_events",
        "s3://udacity-dend/log_data",
        credentials.access_key,
        credentials.secret_key,
        "us-west-2",
        "s3://udacity-dend/log_json_path.json",
    )
    redshift_hook.run(sql)


copy_events_task = PythonOperator(
    task_id="create_events_table",
    dag=dag,
    python_callable=load_events_data_to_redshift,
)

create_tables_task >> start_operator
start_operator >> copy_events_task
