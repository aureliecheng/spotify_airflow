from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

import pandas as pd

default_args = {
  'owner': 'aurelie',
  'depends_on_past': False,
  'email': ['aurelie.cheng@gmail.com'],
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 0,
  'max_active_runs': 1,
  'retry_delay': timedelta(minutes=15)
}

dag_id = 'spotify_api'
dag = DAG(
  dag_id,
  default_args=default_args,
  schedule_interval=timedelta(days=1),
  start_date=datetime(2022, 10, 13, 7)
)

task_get_tracks = BashOperator(
    task_id='get_tracks',
    dag=dag,
    bash_command='python3 /home/simplon/airflow/spotify_playlist/spotify_playlist/get_tracks.py'
)

task_send_to_hdfs_tracks = BashOperator(
task_id='send_to_hdfs_tracks',
dag=dag,
bash_command="""
tail -n +2 /home/simplon/airflow/spotify_playlist/spotify_playlist/tracks_{{ execution_date.strftime('%Y-%m-%d') }}.csv > /home/simplon/airflow/spotify_playlist/spotify_playlist/tracks_{{ execution_date.strftime('%Y-%m-%d') }}_body.csv
hdfs dfs -put /home/simplon/airflow/spotify_playlist/spotify_playlist/tracks_{{ execution_date.strftime('%Y-%m-%d') }}_body.csv /DATA/SPOTIFY/TRACKS/CSV
"""
)

task_get_artists = BashOperator(
    task_id='get_artists',
    dag=dag,
    bash_command='python3 /home/simplon/airflow/spotify_playlist/spotify_playlist/get_artists.py'
)
task_send_to_hdfs_artists= BashOperator(
    task_id='send_to_hdfs_artists',
    dag=dag,
    bash_command="""
    tail -n +2 /home/simplon/airflow/spotify_playlist/spotify_playlist/artists_{{ execution_date.strftime('%Y-%m-%d') }}.csv > /home/simplon/airflow/spotify_playlist/spotify_playlist/artists_{{ execution_date.strftime('%Y-%m-%d') }}_body.csv
    hdfs dfs -put /home/simplon/airflow/spotify_playlist/spotify_playlist/artists_{{ execution_date.strftime('%Y-%m-%d') }}_body.csv /DATA/SPOTIFY/ARTISTS/CSV
    """
)

def in_out_f(**kwargs):
  #read input
  previous_tracks = pd.read_csv(f"/home/simplon/airflow/spotify_playlist/spotify_playlist/tracks_{kwargs['previous_tracks_dt']}.csv")
  current_tracks = pd.read_csv(f"/home/simplon/airflow/spotify_playlist/spotify_playlist/tracks_{kwargs['current_tracks_dt']}.csv")

  previous_tracks = previous_tracks.drop("track_id", axis=1).drop_duplicates()
  current_tracks = current_tracks.drop("track_id", axis=1).drop_duplicates()
  outer = previous_tracks.merge(current_tracks, how='outer', indicator=True)

  removed_artist = outer[(outer._merge=='left_only')].drop('_merge', axis=1)
  removed_artist["status"] = "out"
  new_artist = outer[(outer._merge=='right_only')].drop('_merge', axis=1)
  new_artist["status"] = "in"
  df=pd.concat([removed_artist, new_artist])
  #write
  df.to_csv("/home/simplon/airflow/spotify_playlist/spotify_playlist/in_out.csv")

task_in_out = PythonOperator(
  task_id='in_out',
  dag=dag,
  python_callable=in_out_f,
  provide_context=True,
  op_kwargs={'previous_tracks_dt': '{{ ds }}',
            'current_tracks_dt': '{{ tomorrow_ds }}'
            },
)

task_send_to_hdfs_in_out= BashOperator(
    task_id='send_to_hdfs_in_out',
    dag=dag,
    bash_command="""
    hdfs dfs -put /home/simplon/airflow/spotify_playlist/spotify_playlist/in_out.csv /DATA/SPOTIFY/INT_OUT/CSV
    """
)

task_get_tracks >> task_send_to_hdfs_tracks >> task_get_artists >> task_send_to_hdfs_artists >> task_in_out >> task_send_to_hdfs_in_out
