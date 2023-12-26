from datetime import datetime, timedelta
import pendulum
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from plugins.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
)
from plugins.helpers import SqlQueries

default_args = {
    "owner": "Martin Mwangi",
    "start_date": pendulum.now(),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}


@dag(
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    # schedule_interval="0 * * * *",
    start_date = pendulum.now(),
)

def final_project():
    start_operator = DummyOperator(task_id="Begin_execution", dag=dag)
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id="Stage_events",
        dag=dag,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_events",
        s3_path="s3://udacity-dend/log_data",
        json_path="s3://udacity-dend/log_json_path.json",
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id="Stage_songs",
        dag=dag,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_songs",
        s3_path="s3://udacity-dend/song_data",
        json_path="auto",
    )

    load_songplays_table = LoadFactOperator(
        task_id="Load_songplays_fact_table",
        dag=dag,
        redshift_conn_id="redshift",
        table="songplays",
        sql=SqlQueries.songplay_table_insert,
        append_only=False,
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id="Load_user_dim_table",
        dag=dag,
        redshift_conn_id="redshift",
        table="users",
        sql=SqlQueries.user_table_insert,
        append_only=False,
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id="Load_song_dim_table",
        dag=dag,
        redshift_conn_id="redshift",
        table="songs",
        sql=SqlQueries.song_table_insert,
        append_only=False,
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id="Load_artist_dim_table",
        dag=dag,
        redshift_conn_id="redshift",
        table="artists",
        sql=SqlQueries.artist_table_insert,
        append_only=False,
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id="Load_time_dim_table",
        dag=dag,
        redshift_conn_id="redshift",
        table="time",
        sql=SqlQueries.time_table_insert,
        append_only=False,
    )

    run_quality_checks = DataQualityOperator(
        task_id="Run_data_quality_checks",
        dag=dag,
        redshift_conn_id="redshift",
        tables=["songplays", "songs", "artists", "time", "users"],
    )


final_project_dag = final_project()
