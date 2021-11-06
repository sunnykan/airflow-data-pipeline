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


stage_events = PythonOperator(
    task_id="create_events_table",
    dag=dag,
    python_callable=load_events_data_to_redshift,
)


def load_songs_data_to_redshift(*args, **kwargs):
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
        "staging_songs",
        "s3://udacity-dend/song_data/A/A/A",
        credentials.access_key,
        credentials.secret_key,
        "us-west-2",
        "auto",
    )
    redshift_hook.run(sql)


stage_songs = PythonOperator(
    task_id="create_songs_table", dag=dag, python_callable=load_songs_data_to_redshift
)

load_songplays_fact_table = PostgresOperator(
    task_id="load_songplays_fact_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql="""INSERT INTO songplays (playid, start_time, userid, level, songid, artistid, sessionid, location, user_agent)
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """,
)

# load_song_dim_table = PostgresOperator(
#     task_id="load_song_dim_table",
#     dag=dag,
#     postgres_conn_id="redshift",
#     sql=sql_queries.song_table_insert,
# )

load_user_dim_table = PostgresOperator(
    task_id="load_user_dim_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql="""insert into users (userid, first_name, last_name, gender, level)
    select distinct userId,
                    firstName,
                    lastName,
                    gender,
                    level
    from staging_events
    where page = 'NextSong'
    and userId NOT IN (select distinct userid FROM users);
    """,
)


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
[stage_events, stage_songs] >> load_songplays_fact_table
load_songplays_fact_table >> load_user_dim_table
