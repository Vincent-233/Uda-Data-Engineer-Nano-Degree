from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries


default_args = {
    'owner':'airflow',
    'depends_on_past':False,
    'start_date':datetime(2018,11, 1),
    'retries':3,
    'retry_delay':timedelta(minutes=5),
    'catchup':False,
    'email_on_retry':False
}

dag = DAG('uda_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval="@hourly"
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Staging_events',
    redshift_conn_id='redshift-bp',
    aws_credentials_id='aws_credentials',
    table='staging_events',
    json_paths='s3://udacity-dend/log_json_path.json',
    s3_bucket='udacity-dend',
    s3_key='log_data/{execution_date.year}/{execution_date.month}/{execution_date.year}-{execution_date.month}-{execution_date.day}-events.json',
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Staging_songs',
    redshift_conn_id='redshift-bp',
    aws_credentials_id='aws_credentials',
    table='Staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song_data/',
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id='redshift-bp',
    sql=SqlQueries.songplay_table_insert,
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id='redshift-bp',
    target_table='users',
    mode = "delete-load",
    sql=SqlQueries.user_table_insert,
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id='redshift-bp',
    target_table='songs',
    mode = "delete-load",
    sql=SqlQueries.song_table_insert,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id='redshift-bp',
    target_table='artists',
    mode = "delete-load",
    sql=SqlQueries.artist_table_insert,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id='redshift-bp',
    target_table='time',
    mode = "delete-load",
    sql=SqlQueries.time_table_insert,
    dag=dag
)

start_data_check = DummyOperator(task_id='start_data_quality_checks',  dag=dag) 

check_fact_songplay = DataQualityOperator(
    task_id='check_fact_songplay',
    redshift_conn_id='redshift-bp',
    table='songplays',
    biz_pk_col='songplay_id',
    dag=dag
)

check_dim_users = DataQualityOperator(
    task_id='check_dim_users',
    redshift_conn_id='redshift-bp',
    table='users',
    biz_pk_col='user_id',
    dag=dag
)

check_dim_songs = DataQualityOperator(
    task_id='check_dim_songs',
    redshift_conn_id='redshift-bp',
    table='songs',
    biz_pk_col='song_id',
    dag=dag
)

check_dim_artists = DataQualityOperator(
    task_id='check_dim_artists',
    redshift_conn_id='redshift-bp',
    table='artists',
    biz_pk_col='artist_id',
    dag=dag
)

check_dim_time = DataQualityOperator(
    task_id='check_dim_time',
    redshift_conn_id='redshift-bp',
    table='time',
    biz_pk_col='start_time',
    dag=dag
)

end_operator = DummyOperator(task_id='End_execution',  dag=dag)


# task dependency
start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table,
                         load_artist_dimension_table, load_time_dimension_table] >> start_data_check
start_data_check >> [check_fact_songplay,check_dim_users,check_dim_songs,check_dim_artists,check_dim_time] >> end_operator
