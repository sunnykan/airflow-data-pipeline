from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self, postgres_conn_id="", table="", table_cols="", sql="", *args, **kwargs
    ):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.table = table
        self.table_cols = table_cols
        self.sql = sql

    def execute(self, context):
        postgres_conn_id = self.postgres_conn_id
        redshift = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        sql = """INSERT INTO {} ({})""".format(self.table, self.table_cols) + self.sql
        redshift.run(sql)


# load_songplays_fact_table = PostgresOperator(
#     task_id="load_songplays_fact_table",
#     dag=dag,
#     postgres_conn_id="redshift",
#     sql=


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
