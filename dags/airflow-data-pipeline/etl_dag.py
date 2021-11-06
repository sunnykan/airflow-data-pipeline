import datetime
import logging

from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook

from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators import HasRowsOperator, S3ToRedshiftOperator

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

# load_songplays_fact_table = PostgresOperator(
#     task_id="load_songplays_fact_table",
#     dag=dag,
#     postgres_conn_id="redshift",
#     sql="""INSERT INTO songplays (playid, start_time, userid, level, songid, artistid, sessionid, location, user_agent)
#         SELECT
#                 md5(events.sessionid || events.start_time) songplay_id,
#                 events.start_time,
#                 events.userid,
#                 events.level,
#                 songs.song_id,
#                 songs.artist_id,
#                 events.sessionid,
#                 events.location,
#                 events.useragent
#                 FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
#             FROM staging_events
#             WHERE page='NextSong') events
#             LEFT JOIN staging_songs songs
#             ON events.song = songs.title
#                 AND events.artist = songs.artist_name
#                 AND events.length = songs.duration
#     """,
# )

# load_song_dim_table = PostgresOperator(
#     task_id="load_song_dim_table",
#     dag=dag,
#     postgres_conn_id="redshift",
#     sql=sql_queries.song_table_insert,
# )

# load_user_dim_table = PostgresOperator(
#     task_id="load_user_dim_table",
#     dag=dag,
#     postgres_conn_id="redshift",
#     sql="""insert into users (userid, first_name, last_name, gender, level)
#     select distinct userId,
#                     firstName,
#                     lastName,
#                     gender,
#                     level
#     from staging_events
#     where page = 'NextSong'
#     and userId NOT IN (select distinct userid FROM users);
#     """,
# )


# load_artist_dim_table = PostgresOperator(
#     task_id="load_artist_dim_table",
#     dag=dag,
#     postgres_conn_id="redshift",
#     sql=sql_queries.artist_table_insert,
# )

# load_time_dim_table = PostgresOperator(
#     task_id="load_time_dim_table",
#     dag=dag,
#     postgres_conn_id="redshift",
#     sql=sql_queries.time_table_insert,
# )

create_tables_task >> start_operator
start_operator >> [stage_events, stage_songs]
# [stage_events, stage_songs] >> load_songplays_fact_table
# load_songplays_fact_table >> load_user_dim_table
