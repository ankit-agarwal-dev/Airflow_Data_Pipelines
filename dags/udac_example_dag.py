from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
#from airflow.operators import (
    #LoadFactOperator,
    #LoadDimensionOperator,
    #DataQualityOperator,
    #StageToRedshiftOperator)
import airflow.operators
from helpers import SqlQueries

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 8, 24),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default' : False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',
                               dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Stage_events",
    dag=dag,
    table="events_stg",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="s3://udacity-dend/data-pipelines",
    s3_key="s3://udacity-dend/log_data/{execution_date.year}/{execution_date.month}/{ds}-events.json",
    json_format="s3://udacity-dend/log_json_path.json"
    )

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="Stage_songs",
    dag=dag,
    table="songs_stg",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="s3://udacity-dend/data-pipelines",
    s3_key="s3://udacity-dend/song_data/A/A/A",
    json_format="auto"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs_plays_fact",
    fact_sql=SqlQueries.songplay_table_insert,
    append_yn="Y"
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users_dim",
    dimension_sql=SqlQueries.user_table_insert,
    append_yn="N"
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs_dim",
    dimension_sql=SqlQueries.song_table_insert,
    append_yn="N"
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists_dim",
    dimension_sql=SqlQueries.artist_table_insert,
    append_yn="N"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time_dim",
    dimension_sql=SqlQueries.time_table_insert,
    append_yn="N"
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs_plays_fact",
    column_name="user_id",
    no_of_null_val=0
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift 
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator