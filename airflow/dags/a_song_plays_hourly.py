from datetime import datetime, timedelta

from airflow import DAG
from helpers import SqlQueries

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.subdag_operator import SubDagOperator

from airflow.operators import (DataQualityOperator
                              ,HasRowsOperator
                              ,LoadDimensionOperator
                              ,LoadFactOperator
                              ,StageToRedshiftOperator
                              )

from a_song_plays_dim_subdag import dim_subdag
from a_song_plays_stg_subdag import stg_subdag

import sql_statements

#Ubuntu 18.04 LTS
#export aws_access_key_id=
#export aws_secret_key=
import os
aws_access_key_id = os.environ.get('aws_access_key_id')
aws_secret_key = os.environ.get('aws_secret_key')
#print(aws_access_key_id)
#print(aws_secret_key)

start_date = datetime.utcnow()- timedelta(minutes=5)

# Switches
sw_delete_stages = False
sw_delete_fact = False
sw_delete_dimensions = True
sw_skip_data_quality_checks = False
data_quality_tables = ['staging_events','staging_songs','songplays','artists','songs','time','users']

default_args = {'owner': 'dwhuser'
               ,'start_date': datetime(2018, 11, 21)
               ,'end_date': datetime(2018, 11, 22)
               ,'depends_on_past': False
               ,'retries': 1
               ,'retry_delay': timedelta(minutes=1)
               ,'email_on_retry': False
               #,'email': ['airflow@example.com']
               #,'email_on_failure': False
               #,'queue': 'bash_queue'
               #,'pool': 'backfill'
               #,'priority_weight': 10
               }

dag = DAG('a_song_plays_hourly'
         ,catchup=True
         ,default_args=default_args
         ,description='Project: Data Pipelines with Airflow'
         #,schedule_interval=timedelta(days=1)
         #,schedule_interval='0 * * * *'
         ,schedule_interval='@hourly'
         ,max_active_runs=1
         )

begin_execution = DummyOperator(
    task_id='begin_execution',  
    dag=dag
)

end_execution = DummyOperator(
    task_id='end_execution',  
    dag=dag
)

staging_events_task_id = "staging_events_subdag"
staging_events_subdag_task = SubDagOperator(
    subdag=stg_subdag(
        "a_song_plays_hourly",
        staging_events_task_id,
        "aws_credentials",
        "redshift",
        "staging_events",
        sql_statements.CREATE_TABLE_STAGING_EVENTS,
        s3_prefix="s3:/",
        s3_bucket="udacity-dend",
        s3_key="log_data/{execution_date.year}/{execution_date.month}",
        s3_jsonpath_file="log_json_path.json",
        sw_delete_stages=sw_delete_stages,
        partition_year="{execution_date.year}",
        partition_month="{execution_date.month}",
        start_date=start_date,
    ),
    task_id=staging_events_task_id,
    dag=dag,
)

staging_songs_task_id = "staging_songs_subdag"
staging_songs_subdag_task = SubDagOperator(
    subdag=stg_subdag(
        "a_song_plays_hourly",
        staging_songs_task_id,
        "aws_credentials",
        "redshift",
        "staging_songs",
        sql_statements.CREATE_TABLE_STAGING_SONGS,
        s3_prefix="s3:/",
        s3_bucket="udacity-dend",
        s3_key="song_data",
        s3_jsonpath_file="auto",
        sw_delete_stages=sw_delete_stages,
        partition_year="{execution_date.year}",
        partition_month="{execution_date.month}",
        start_date=start_date,
    ),
    task_id=staging_songs_task_id,
    dag=dag,
)

load_songplays_fact_table = LoadFactOperator(
    task_id='load_songplays_fact_table'
    ,dag=dag
    ,postgres_conn_id="redshift"
    ,table="songplays"
    ,sw_delete_fact=sw_delete_fact
    ,sql=f"""
        {SqlQueries.songplay_table_insert}
        WHERE 1 = 1
        AND to_char(events.start_time,'YYYY-MM-DD"T"HH24:00:00"+"00:00') > '{{{{ prev_execution_date }}}}'
        AND to_char(events.start_time,'YYYY-MM-DD"T"HH24:00:00"+"00:00') < '{{{{ next_execution_date }}}}'
    """
)

dim_time_task_id = "dim_time_subdag"
dim_time_subdag_task = SubDagOperator(
    subdag=dim_subdag(
        "a_song_plays_hourly",
        dim_time_task_id,
        postgres_conn_id="redshift",
        table="time",
        insert_sql_stmt=f"""
            {SqlQueries.time_table_insert}
        """,
        sw_delete_dimensions=sw_delete_dimensions,
        start_date=start_date,
    ),
    task_id=dim_time_task_id,
    dag=dag,
)

dim_artists_task_id = "dim_artists_subdag"
dim_artists_subdag_task = SubDagOperator(
    subdag=dim_subdag(
        "a_song_plays_hourly",
        dim_artists_task_id,
        postgres_conn_id="redshift",
        table="artists",
        insert_sql_stmt=f"""
            {SqlQueries.artists_table_insert}
        """,
        sw_delete_dimensions=sw_delete_dimensions,
        start_date=start_date,
    ),
    task_id=dim_artists_task_id,
    dag=dag,
)

dim_songs_task_id = "dim_songs_subdag"
dim_songs_subdag_task = SubDagOperator(
    subdag=dim_subdag(
        "a_song_plays_hourly",
        dim_songs_task_id,
        postgres_conn_id="redshift",
        table="songs",
        insert_sql_stmt=f"""
            {SqlQueries.songs_table_insert}
        """,
        sw_delete_dimensions=sw_delete_dimensions,
        start_date=start_date,
    ),
    task_id=dim_songs_task_id,
    dag=dag,
)

dim_users_task_id = "dim_users_subdag"
dim_users_subdag_task = SubDagOperator(
    subdag=dim_subdag(
        "a_song_plays_hourly",
        dim_users_task_id,
        postgres_conn_id="redshift",
        table="users",
        insert_sql_stmt=f"""
            {SqlQueries.users_table_insert}
        """,
        sw_delete_dimensions=sw_delete_dimensions,
        start_date=start_date,
    ),
    task_id=dim_users_task_id,
    dag=dag,
)

run_data_quality_checks = DataQualityOperator(
    task_id='run_data_quality_checks'
    ,dag=dag
    ,postgres_conn_id="redshift"
    ,data_quality_tables=data_quality_tables
    ,sw_skip_data_quality_checks=sw_skip_data_quality_checks
)

begin_execution >> staging_events_subdag_task
begin_execution >> staging_songs_subdag_task

staging_events_subdag_task >> load_songplays_fact_table
staging_songs_subdag_task >> load_songplays_fact_table

load_songplays_fact_table >> dim_time_subdag_task
load_songplays_fact_table >> dim_artists_subdag_task
load_songplays_fact_table >> dim_songs_subdag_task
load_songplays_fact_table >> dim_users_subdag_task

dim_time_subdag_task >> run_data_quality_checks
dim_artists_subdag_task >> run_data_quality_checks
dim_songs_subdag_task >> run_data_quality_checks
dim_users_subdag_task >> run_data_quality_checks

run_data_quality_checks >> end_execution
