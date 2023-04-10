from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common import final_project_sql_statements

default_args = {
    'owner': 'nagy',
    'start_date': pendulum.now(),
    'depends_on_past': True,
    'retries': 3,
    'retry_delay': timedelta(hours=1),
    'catchup': False
}

S3_BUCKET_NAME = 'udacity-dend'
S3_SONG_KEY = 'song_data'
REGION = 'us-west-2'
AWS_CREDENTIALS_ID = 'aws_nagy'
REDSHIFT_CONN_ID = 'redshift_nagy'


@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():
    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id=REDSHIFT_CONN_ID,
        aws_credentials_id=AWS_CREDENTIALS_ID,
        table="staging_events",
        s3_bucket=S3_BUCKET_NAME,
        s3_key="log_data",
        dformat="JSON 's3://{}/log_json_path.json'".format(S3_BUCKET_NAME),
        region=REGION
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id=REDSHIFT_CONN_ID,
        aws_credentials_id=AWS_CREDENTIALS_ID,
        table="staging_songs",
        s3_bucket=S3_BUCKET_NAME,
        s3_key="song_data",
        dformat="JSON 'auto'",
        region=REGION
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id=REDSHIFT_CONN_ID,
        sql=final_project_sql_statements.SqlQueries.songplay_table_insert,
        table="songplays"
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id=REDSHIFT_CONN_ID,
        sql=final_project_sql_statements.SqlQueries.user_table_insert,
        table="users",
        truncate=True
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id=REDSHIFT_CONN_ID,
        sql=final_project_sql_statements.SqlQueries.song_table_insert,
        table="songs",
        truncate=True
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id=REDSHIFT_CONN_ID,
        sql=final_project_sql_statements.SqlQueries.artist_table_insert,
        table="artists",
        truncate=True
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id=REDSHIFT_CONN_ID,
        sql=final_project_sql_statements.SqlQueries.time_table_insert,
        table="time",
        truncate=True
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id=REDSHIFT_CONN_ID,
        dq_checks=[
            {'check_sql': 'SELECT COUNT(*) FROM "time"', 'invalid_result': 0, 'is_not': True},
            {'check_sql': 'SELECT COUNT(*) FROM "artists"', 'invalid_result': 0, 'is_not': True},
            {'check_sql': 'SELECT COUNT(*) FROM "songs"', 'invalid_result': 0, 'is_not': True},
            {'check_sql': 'SELECT COUNT(*) FROM "users"', 'invalid_result': 0, 'is_not': True},
            {'check_sql': 'SELECT COUNT(*) FROM "songplays"', 'invalid_result': 0, 'is_not': True},
            {'check_sql': 'SELECT COUNT(*) FROM "users" WHERE userid is null', 'expected_result': 0, 'is_not': False},
            {'check_sql': 'SELECT COUNT(*) FROM "songs" WHERE songid is null', 'expected_result': 0, 'is_not': False},
        ]
    )

    end_operator = DummyOperator(task_id='Stop_execution')

    start_operator >> stage_events_to_redshift >> load_songplays_table
    start_operator >> stage_songs_to_redshift >> load_songplays_table
    load_songplays_table >> load_song_dimension_table >> run_quality_checks
    load_songplays_table >> load_user_dimension_table >> run_quality_checks
    load_songplays_table >> load_artist_dimension_table >> run_quality_checks
    load_songplays_table >> load_time_dimension_table >> run_quality_checks
    run_quality_checks >> end_operator


final_project_dag = final_project()
