from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
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
description="Home Work 6",
catchup=False,
schedule_interval='0 6 * * *')

task2 = BashOperator(
task_id='spark',
bash_command='export SPARK_HOME=/home/spark && export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin && spark-shell -i /home/t4.scala',
dag=dag1)


dag = DAG( 'hello_world' , description= 'Hello World DAG' , 
          schedule_interval= '0 12 * * *' , 
          start_date=datetime( 2023 , 1 , 1
          ), catchup= False ) 

hello_operator = BashOperator(task_id= 'hello_task' , bash_command='echo Hello from Airflow', dag=dag)
hello_file_operator = BashOperator(task_id= 'hello_file_task' , bash_command='sh /home/t4.sh ', dag=dag) 
skipp_operator = BashOperator(task_id= 'skip_task' , bash_command='exit 99', dag=dag) 

hello_operator >> hello_file_operator >> skipp_operator
