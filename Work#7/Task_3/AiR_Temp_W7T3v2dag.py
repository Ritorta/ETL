from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import timedelta
import pendulum



default_args = {
'owner': 'Ritorta',
'depends_on_past': False,
'start_date': pendulum.datetime(year=2024, month=4, day=25).in_timezone('Europe/Moscow'),
'email': ['meddesu@yandex.ru'],
'email_on_failure': False,
'email_on_retry': False,
'retries': 0,
'retry_delay': timedelta(minutes=5)
}

#DAG1
dag1 = DAG('Work_7_Task_1',
default_args=default_args,
description="Home_Work_7_1",
catchup=False,
schedule_interval='0 6 * * *')

AiR_Home_7_1  = BashOperator(
task_id='Run_Work_7_1',
bash_command='export SPARK_HOME=/home/spark && export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin && python3 /home/ritorta/HomeWork/W7/Task_1/AiR_W7T1.py',
dag=dag1)

AiR_Home_7_1

#DAG2
dag2 = DAG('Work_7_Task_2',
default_args=default_args,
description="Home_Work_7_2",
catchup=False,
schedule_interval='0 6 * * *')

AiR_Home_7_2  = BashOperator(
task_id='Run_Work_7_2',
bash_command='export SPARK_HOME=/home/spark && export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin && python3 /home/ritorta/HomeWork/W7/Task_2/AiR_W7T2.py',
dag=dag2)

AiR_Home_7_2

#DAG3
dag3 = DAG('Work_7_Task_3',
default_args=default_args,
description="Home_Work_7_3",
catchup=False,
schedule_interval='0 6 * * *')


def run_airflow_script(**kwargs):
    exec(open("/home/ritorta/HomeWork/W7/Task_3/AiR_W7T3.py").read())

AiR_Home_7_3  = PythonOperator(
    task_id='Run_Work_7_3',
    python_callable=run_airflow_script,
    provide_context=True,
    dag=dag3
)

AiR_Home_7_3
