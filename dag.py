from faulthandler import dump_traceback
from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

from airflow.contrib.sensors.file_sensor import FileSensor

from airflow.models import Variable

from airflow.decorators import task

default_args = {
  'owner': 'aurelie',
  'depends_on_past': False,
  'start_date': (datetime.now() - timedelta(days=1)).replace(hour=2),
  'email': ['aurelie.cheng@gmail.com'],
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 0,
  'max_active_runs': 1,
  'retry_delay': timedelta(minutes=15)
}

dag_id = 'hands_on_airflow'
dag = DAG(
  dag_id,
  default_args=default_args,
  schedule_interval=timedelta(hours=1),
  start_date=datetime(2022, 10, 13, 9)
)

# dummy operator
task_start = DummyOperator(
    task_id='start',
    dag=dag
)

# bash operator
task_list = BashOperator(
    task_id='list',
    dag=dag,
    bash_command='ls /home/simplon'
)

# Ajouter une tâche utilisant le python opérateur la nommer "hello" 
# et lui faire écrire un fichier contenant le mot "world" et contenant la date d'exécution du workflow.
def write_into_file(**context):
    date_exec = context['execution_date']
    text_file = open("/home/simplon/Documents/world.txt", "w")
    text_file.writelines('world ' + date_exec.strftime("%d/%m/%Y, %H:%M:%S"))
    text_file.close()

task_hello = PythonOperator(
  task_id='hello',
  python_callable=write_into_file,
  provide_context=True,
  dag=dag
)

# Créer deux tâches utilisant le bash opérator: 
# la tâche nommée "sleep10" devra faire un sleep de 10 secondes 
# la tache nommée "sleep15" devra faire un sleep de 15 secondes.
# faire s'exécuter ces deux tâches en parallèle après la tâche "hello"

sleep10 = BashOperator(
    task_id='sleep10',
    dag=dag,
    bash_command='sleep 10'
)

sleep15 = BashOperator(
    task_id='sleep15',
    dag=dag,
    bash_command='sleep 15'
)

# Créer une tâche nommée "join" qui utilise le dummy operator et 
# qui attend que les tâches sleep10 et sleep15 soient terminées pour s'exécuter.
task_join = DummyOperator(
    task_id='join',
    dag=dag
)

task_start >> task_list >> task_hello >> [sleep10, sleep15] >> task_join

# ------------------ XCOM : shared data between tasks ------------------
# générer un fichier dont le nom contient la date d'exécution du workflow 
# & transmettre un identifiant entre les différentes tâches

def get_exe_date(**context):
    return context['execution_date'].strftime("%d_%m_%Y")

task_get_exe_date = PythonOperator(
    task_id='get_exe_date',
    python_callable=get_exe_date,
    dag=dag,
    do_xcom_push=True
)

def save_exe_date(ti):
    dt = ti.xcom_pull(task_ids=['get_exe_date'])
    if not dt:
        raise ValueError('No value currently stored in XComs.')
    path = '/home/simplon/Documents/'
    text_file = open(path+dt[0]+'.txt', "w")
    text_file.writelines('date saved')
    text_file.close()

task_save_exe_date = PythonOperator(
    task_id='save_exe_date',
    python_callable=save_exe_date,
    dag=dag
)

# ------------------ SENSOR ------------------
# détecter l'arrivé d'un fichier

# check for every 10s if a file exists
sensor_task = FileSensor(
    task_id='file_sensor',
    poke_interval=10, #time interval at which the condition will be evaluated
    #filepath='/home/simplon/test_sensor/',
    filepath = Variable.get("file_sensor_path"),
    dag=dag
)

# Créer dynamiquement des tâches dans un DAG à partir d'une list de string python


# ------------------ POSTGRES ------------------
# Créer et utiliser des connections airflow pour se connecter à une base de donnée (postgresql)
args = {
    'owner': 'aurelie'
}
dag_psql = DAG(
    dag_id = "postgresoperator_demo",
    default_args=args,
    schedule_interval='@once',
    description='use case of psql operator in airflow',
    start_date=datetime(2022, 10, 13, 9)
)

create_table_sql_query = """ 
CREATE TABLE employee (id INT NOT NULL, name VARCHAR(250) NOT NULL, dept VARCHAR(250) NOT NULL);
"""
insert_data_sql_query = """
insert into employee (id, name, dept) values(1, 'vamshi','bigdata'),(2, 'divya','bigdata'),(3, 'binny','projectmanager'),
(4, 'omair','projectmanager') ;"""

create_table_task = PostgresOperator(
    sql = create_table_sql_query,
    task_id = "create_table",
    postgres_conn_id = "postgres_default",
    dag = dag_psql
)

insert_data_task  = PostgresOperator(
    sql = insert_data_sql_query,
    task_id = "insert_data",
    postgres_conn_id = "postgres_default",
    dag = dag_psql
)

create_table_task >> insert_data_task
