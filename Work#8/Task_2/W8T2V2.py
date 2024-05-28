import datetime
import os
import requests
import pendulum
import os
from dotenv import load_dotenv
from airflow.decorators import dag, task
from airflow.providers.telegram.operators.telegram import TelegramOperator
from sqlalchemy import create_engine
import pandas as pd
from tabulate import tabulate

os.environ["no_proxy"]="*"

@dag(
    dag_id="wether-tlegram-sql-exel-V2",
    schedule="@once",
    start_date=pendulum.datetime(2024, 5, 6, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)


def WetherETL():

    #API
    dotenv_path = '/home/ritorta/HomeWork/API_KEY.env' # Проверить путь к API
    load_dotenv(dotenv_path)
    yandex_api_key = os.getenv('YANDEX_API')
    openweather_api = os.getenv('OPENWEATHER_API')

    first_message_telegram = TelegramOperator(
        task_id='weather_sql_message_telegram',
        telegram_conn_id='telegram_default',
        token='6875707033:AAG2TaDKrrLUwlUmcX9LIVA1uCm6S43pya0',
        chat_id='547504860',
        text='Wether in Tyumen \n\n' + "<pre>{{ ti.xcom_pull(task_ids=['get_sql_save_weather'],key='table_wether')[0]}}</pre>"
    )

    second_message_telegram = TelegramOperator(
        task_id='exel_sql_message_telegram',
        telegram_conn_id='telegram_default',
        token='6875707033:AAG2TaDKrrLUwlUmcX9LIVA1uCm6S43pya0',
        chat_id='547504860',
        text="<pre>{{ ti.xcom_pull(task_ids=['exel_get_sql'],key='table_payments')[0]}}</pre>"
    )


    @task(task_id='yandex_wether')
    def get_yandex_wether(**kwargs):
        ti = kwargs['ti']
        url = "https://api.weather.yandex.ru/v2/informers/?lat=57.152985&lon=65.527168"

        payload={}
        headers = {
        'X-Yandex-API-Key': yandex_api_key
        }
        response = requests.request("GET", url, headers=headers, data=payload)
        print("test")
        temperature = response.json()['fact']['temp']

        a = response.json()['fact']['temp']
        print(a)
    
        ti.xcom_push(key='wether', value=temperature)


    @task(task_id='open_wether')
    def get_open_wether(**kwargs):
        ti = kwargs['ti']
        url = "https://api.openweathermap.org/data/2.5/weather?lat=57.152985&lon=65.527168"

        payload={}
        headers = {
        'x-api-key': openweather_api
        }

        response = requests.request("GET", url, headers=headers, data=payload)
        print("test")
        temperature = round(float(response.json()['main']['temp']) - 273.15, 2)

        a = round(float(response.json()['main']['temp']) - 273.15, 2)
        print(a)
        
        ti.xcom_push(key='open_wether', value=temperature)
    

    @task(task_id='get_sql_save_weather')
    def save_weather_get_sql(**kwargs):
        ti = kwargs['ti']
        yandex_data = str(kwargs['ti'].xcom_pull(task_ids=['yandex_wether'], key='wether')[0])
        open_weather_data = str(kwargs['ti'].xcom_pull(task_ids=['open_wether'],key='open_wether')[0])
        engine = create_engine("mysql://root:1@localhost:33061/spark")
        city='Tyumen'

        df = pd.DataFrame  ({
            'City': [city],
            'Yandex': [yandex_data], 
            'Open Weather': [open_weather_data], 
            'date_time': [pd.Timestamp.now().round(freq='s')]
        })

        df.to_sql(name='test_8_3', con=engine, schema='spark', if_exists='append', index=False)
        ti.xcom_push(key='table_wether', value=tabulate(df, headers='keys', tablefmt='psql'))


    @task(task_id='python_wether')
    def get_wether(**kwargs):
        print("Yandex "+str(kwargs['ti'].xcom_pull(task_ids=['yandex_wether'],key='wether')[0])+" Open "+str(kwargs['ti'].xcom_pull(task_ids=['open_wether'],key='open_wether')[0]))
    
    
    @task(task_id='exel_get_sql')
    def get_sql_exel(**kwargs):
        ti = kwargs['ti']
        engine = create_engine("mysql://root:1@localhost:33061/spark")

        df = pd.read_sql(sql='AiR_W8T2', con=engine) # База данных уже должна быть предварительно загружена в SQL, если этого не сделано будет вылезать ошибка.
        df['Month'] = df['Month'].dt.date
        df.drop('Balance of debt', inplace=True, axis=1)
        ti.xcom_push(key='table_payments', value=tabulate(df.head(20), headers='keys', tablefmt='psql'))


    yandex_wether = get_yandex_wether()
    open_wether = get_open_wether()
    python_wether = get_wether()
    save_weather = save_weather_get_sql()
    sql_exel = get_sql_exel()

    yandex_wether >> open_wether >> python_wether >> save_weather >> first_message_telegram >> sql_exel >> second_message_telegram
    
dag = WetherETL()
