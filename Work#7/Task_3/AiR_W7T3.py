
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import os
from dotenv import load_dotenv


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
