import datetime
import os
import requests
import pendulum
import os
from dotenv import load_dotenv
from airflow.decorators import dag, task
from airflow.providers.telegram.operators.telegram import TelegramOperator
from sqlalchemy import create_engine

os.environ["no_proxy"]="*"

@dag(
    dag_id="wether-tlegram-sql",
    schedule="@once",
    start_date=pendulum.datetime(2024, 4, 30, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)


def WetherETL():

    #API
    dotenv_path = '/home/ritorta/HomeWork/API_KEY.env' # Проверить путь к API
    load_dotenv(dotenv_path)
    yandex_api_key = os.getenv('YANDEX_API')
    openweather_api = os.getenv('OPENWEATHER_API')  

    send_message_telegram_task = TelegramOperator(
        task_id='send_message_telegram',
        telegram_conn_id='telegram_default',
        token='6875707033:AAG2TaDKrrLUwlUmcX9LIVA1uCm6S43pya0',
        chat_id='547504860',
        text='Wether in Tyumen \nYandex: ' + "{{ ti.xcom_pull(task_ids=['yandex_wether'],key='wether')[0]['temperature']}}" + " degrees at " + "{{ ti.xcom_pull(task_ids=['yandex_wether'],key='wether')[0]['datetime']}}" +
    "\nOpen wether: " + "{{ ti.xcom_pull(task_ids=['open_wether'],key='open_wether')[0]['temperature']}}" + " degrees at " + "{{ ti.xcom_pull(task_ids=['open_wether'],key='open_wether')[0]['datetime']}}",
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
        current_datetime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        a = response.json()['fact']['temp']
        print(a)
    
        ti.xcom_push(key='wether', value={'temperature': temperature, 'datetime': current_datetime})


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
        current_datetime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        a = round(float(response.json()['main']['temp']) - 273.15, 2)
        print(a)
        
        ti.xcom_push(key='open_wether', value={'temperature': temperature, 'datetime': current_datetime})


    @task(task_id='save_weather')
    def get_save_weather(**kwargs):
        yandex_data = kwargs['ti'].xcom_pull(task_ids='yandex_wether', key='wether')
        open_weather_data = kwargs['ti'].xcom_pull(task_ids='open_wether', key='open_wether')

        temperature_yandex = yandex_data['temperature']
        datetime_yandex = yandex_data['datetime']
        service_yandex = 'Yandex'

        temperature_open_weather = open_weather_data['temperature']
        datetime_open_weather = open_weather_data['datetime']
        service_open_weather = 'OpenWeather'

        engine = create_engine("mysql://root:1@localhost:33061/spark")

        with engine.connect() as connection:
            connection.execute("""DROP TABLE IF EXISTS spark.`Temperature_Weather`""")
            connection.execute("""CREATE TABLE IF NOT EXISTS spark.`Temperature_Weather` (
                Service VARCHAR(255),
                Date_time TIMESTAMP,
                City VARCHAR(255),
                Temperature FLOAT,
                PRIMARY KEY (Date_time, Service)
            )COLLATE='utf8mb4_general_ci' ENGINE=InnoDB""")
            connection.execute(f"""INSERT INTO spark.`Temperature_Weather` (Date_time, City, Temperature, Service) VALUES ('{datetime_yandex}', 'Tyumen', {temperature_yandex}, '{service_yandex}')""")
            connection.execute(f"""INSERT INTO spark.`Temperature_Weather` (Date_time, City, Temperature, Service) VALUES ('{datetime_open_weather}', 'Tyumen', {temperature_open_weather}, '{service_open_weather}')""")


    @task(task_id='python_wether')
    def get_wether(**kwargs):
        print("Yandex "+str(kwargs['ti'].xcom_pull(task_ids=['yandex_wether'],key='wether')[0])+" Open "+str(kwargs['ti'].xcom_pull(task_ids=['open_wether'],key='open_wether')[0]))


    yandex_wether = get_yandex_wether()
    open_wether = get_open_wether()
    python_wether = get_wether()
    save_weather = get_save_weather()

    yandex_wether >> open_wether >> python_wether >> send_message_telegram_task >> save_weather

dag = WetherETL()
