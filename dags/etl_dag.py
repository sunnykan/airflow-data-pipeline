import datetime
import logging

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook

from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from operators import (
    S3ToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
)

from helpers import SqlQueries

# DAG
default_args = {
    "owner": "sparkify",
    "start_date": datetime.datetime(2018, 11, 1),
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": datetime.timedelta(minutes=5),
    "catchup": False,
    "email_on_retry": False,
}

dag = DAG(
    "sparkify_dag",
    default_args=default_args,
    description="Load data from S3 into Redshift and transform it with Airflow",
    schedule_interval="0 * * * *",
    catchup=True,
    max_active_runs=1,
)

start_operator = DummyOperator(task_id="Begin_execution", dag=dag)
end_operator = DummyOperator(task_id="End_execution", dag=dag)

# Create tables
create_tables_task = PostgresOperator(
    task_id="create_tables_task",
    dag=dag,
    postgres_conn_id="redshift",
    sql="sql/create_tables.sql",
)

# Load staging tables
stage_events = S3ToRedshiftOperator(
    task_id="load_events_from_s3_to_redshift",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws-credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data/{execution_date.year}/{execution_date.month}",
    region="us-west-2",
    filetype_params={
        "filetype": "json",
        "format": "s3://udacity-dend/log_json_path.json",
    },
)

stage_songs = S3ToRedshiftOperator(
    task_id="load_songs_from_s3_to_redshift",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws-credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data/A/A/A",
    region="us-west-2",
    filetype_params={"filetype": "json", "format": "auto"},
)

# Load fact table
load_songplays_fact_table = LoadFactOperator(
    task_id="load_songplays_fact_table",
    dag=dag,
    postgres_conn_id="redshift",
    table="songplays",
    table_cols="playid, start_time, userid, level, songid, artistid, sessionid, location, user_agent",
    sql=SqlQueries.songplays_table_insert,
    truncate=False,
)

# Load dimension tables
load_users_dim_table = LoadDimensionOperator(
    task_id="load_users_dim_table",
    dag=dag,
    postgres_conn_id="redshift",
    table="users",
    table_cols="userid, first_name, last_name, gender, level",
    sql=SqlQueries.users_table_insert,
    truncate=True,
)

load_songs_dim_table = LoadDimensionOperator(
    task_id="load_songs_dim_table",
    dag=dag,
    postgres_conn_id="redshift",
    table="songs",
    table_cols='songid, title, artistid, "year", duration',
    sql=SqlQueries.songs_table_insert,
    truncate=True,
)

load_artists_dim_table = LoadDimensionOperator(
    task_id="load_artists_dim_table",
    dag=dag,
    postgres_conn_id="redshift",
    table="artists",
    table_cols="artistid, name, location, latitude, longitude",
    sql=SqlQueries.artists_table_insert,
    truncate=True,
)

load_time_dim_table = LoadDimensionOperator(
    task_id="load_time_dim_table",
    dag=dag,
    postgres_conn_id="redshift",
    table='"time"',
    table_cols='start_time, "hour", "day", week, "month", "year", weekday',
    sql=SqlQueries.time_table_insert,
    truncate=True,
)

# Data quality check
run_data_quality_check = DataQualityOperator(
    task_id="run_data_quality_check",
    dag=dag,
    redshift_conn_id="redshift",
    tables=["songs", "artists", "users", "time"],
    cols_check=["songid", "artistid", "userid", "start_time"],
    expected_values=[0, 0, 0, 0],
    sql="SELECT COUNT(*) FROM {} WHERE {} IS NULL",
)

# Task ordering
start_operator >> create_tables_task

create_tables_task >> [stage_events, stage_songs]

[stage_events, stage_songs] >> load_songplays_fact_table

load_songplays_fact_table >> [
    load_users_dim_table,
    load_songs_dim_table,
    load_artists_dim_table,
    load_time_dim_table,
]

[
    load_users_dim_table,
    load_songs_dim_table,
    load_artists_dim_table,
    load_time_dim_table,
] >> run_data_quality_check

run_data_quality_check >> end_operator
