from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import pendulum
import requests
import os
from dotenv import load_dotenv


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
bash_command='export SPARK_HOME=/home/spark && export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin && python3 /home/ritorta/HomeWork/W7/Task_1/W7T1.py',
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
def openwear_get_temp(**kwargs):

    #API
    dotenv_path = '/home/ritorta/HomeWork/API_KEY.env' # Проверить путь к API
    load_dotenv(dotenv_path)
    openweather_api = os.getenv('OPENWEATHER_API')

    ti = kwargs['ti']
    city = "Tyumen"
    api_key = openweather_api
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}"
    payload = {}
    headers = {}
    response = requests.request("GET", url, headers=headers, data=payload)
    return round(float(response.json()['main']['temp'])-273.15, 2)


def openwear_check_temp(ti):
    temp = int(ti.xcom_pull(task_ids='Tyumen_get_temperature'))
    print(f'Temperature now is {temp}')
    if temp >= 15:
        return 'Tyumen_Temp_warm'
    else:
        return 'Tyumen_Temp_cold'

with DAG(
        'Tyumen_check_temperature_warm_or_cold',
        start_date=datetime(2024, 4, 25),
        catchup=False,
        tags=['W7T3'],
) as dag:
    Tyumen_get_temperature = PythonOperator(
        task_id='Tyumen_get_temperature',
        python_callable=openwear_get_temp,
    )

    Tyumen_check_temperature = BranchPythonOperator(
        task_id='Tyumen_check_temperature',
        python_callable=openwear_check_temp,
    )

    Tyumen_Temp_warm = BashOperator(
        task_id='Tyumen_Temp_warm',
        bash_command='echo "It is warm"',
    )

    Tyumen_Temp_cold = BashOperator(
        task_id='Tyumen_Temp_cold',
        bash_command='echo "It is cold"',
    )

Tyumen_get_temperature >> Tyumen_check_temperature >> [Tyumen_Temp_warm, Tyumen_Temp_cold]
