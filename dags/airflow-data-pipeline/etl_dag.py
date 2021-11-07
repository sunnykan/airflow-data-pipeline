import datetime
import logging

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook

from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators import (
    HasRowsOperator,
    S3ToRedshiftOperator,
    LoadFactOperator,
)

from operators import LoadDimensionOperator

from helpers import SqlQueries

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

stage_events = S3ToRedshiftOperator(
    task_id="load_events_from_s3_to_redshift",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws-credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    region="us-west-2",
    format="s3://udacity-dend/log_json_path.json",
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
    format="auto",
)

load_songplays_fact_table = LoadFactOperator(
    task_id="load_songplays_fact_table",
    dag=dag,
    postgres_conn_id="redshift",
    table="songplays",
    table_cols="playid, start_time, userid, level, songid, artistid, sessionid, location, user_agent",
    sql=SqlQueries.songplay_table_insert,
)

load_user_dim_table = LoadDimensionOperator(
    task_id="load_user_dim_table",
    dag=dag,
    postgres_conn_id="redshift",
    table="users",
    table_cols="userid, first_name, last_name, gender, level",
    sql=SqlQueries.user_table_insert,
)

load_songs_dim_table = LoadDimensionOperator(
    task_id="load_songs_dim_table",
    dag=dag,
    postgres_conn_id="redshift",
    table="songs",
    table_cols='songid, title, artistid, "year", duration',
    sql=SqlQueries.song_table_insert,
)

load_artists_dim_table = LoadDimensionOperator(
    task_id="load_artists_dim_table",
    dag=dag,
    postgres_conn_id="redshift",
    table="artists",
    table_cols="artistid, name, location, latitude, longitude",
    sql=SqlQueries.artist_table_insert,
)

load_time_dim_table = LoadDimensionOperator(
    task_id="load_time_dim_table",
    dag=dag,
    postgres_conn_id="redshift",
    table='"time"',
    table_cols='start_time, "hour", "day", week, "month", "year", weekday',
    sql=SqlQueries.time_table_insert,
)


create_tables_task >> start_operator
start_operator >> [stage_events, stage_songs]
[stage_events, stage_songs] >> load_songplays_fact_table
load_songplays_fact_table >> [
    load_user_dim_table,
    load_songs_dim_table,
    load_artists_dim_table,
    load_time_dim_table,
]

