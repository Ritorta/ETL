# Файл Task1 и файлы вместе с папками Home_3 и Home_4 надо скинуть в WSL для того, чтобы они работали.

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta
import pendulum

default_args = {
'owner': 'Ritorta',
'depends_on_past': False,
'start_date': pendulum.datetime(year=2024, month=4, day=23).in_timezone('Europe/Moscow'),
'email': ['meddesu@yandex.ru'],
'email_on_failure': False,
'email_on_retry': False,
'retries': 0,
'retry_delay': timedelta(minutes=5)
}

dag1 = DAG('Work_6_Task_1',
default_args=default_args,
description="Home_Work_6",
catchup=False,
schedule_interval='0 6 * * *')

WSL_Home_3 = BashOperator(
task_id='Run_Work_3',
bash_command='export SPARK_HOME=/home/spark && export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin && spark-shell -i /home/ritorta/HomeWork/W6/Home_3/WSL_W3Task_1_v2.scala',
dag=dag1)

WSL_Home_4  = BashOperator(
task_id='Run_Work_4',
bash_command='export SPARK_HOME=/home/spark && export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin && python3 /home/ritorta/HomeWork/W6/Home_4/WSL_W4T1.py',
dag=dag1)

WSL_Home_3 >> WSL_Home_4
