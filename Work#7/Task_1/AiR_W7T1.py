from sqlalchemy import create_engine
from datetime import datetime
from pandas.io import sql
import requests


api_key = 'bc6d51747326a116e97fbc66146b6deb'
city = 'Tyumen'


def openwear_get_temp(api_key, city):
    url = f'https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}' 
    response = requests.get(url)
    data = response.json()
    temperature = data['main']['temp']
    timestamp = data['dt']
    date_time = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    return round(float(temperature) - 273.15, 2), date_time
  
    
def save_weather(api_key, city):
    temperature, date_time = openwear_get_temp(api_key, city)
    
    con = create_engine("mysql://root:1@localhost:33061/spark")
    sql.execute("""drop table if exists spark.`Temperature_Tyumen`""",con)
    sql.execute("""CREATE TABLE if not exists spark.`Temperature_Tyumen` 
                (`date_time` TIMESTAMP NULL DEFAULT NULL, `temperature` FLOAT NULL DEFAULT NULL)
                COLLATE='utf8mb4_general_ci' ENGINE=InnoDB""",con)
     
    with con.connect() as connection:
        ins = f"INSERT INTO spark.`Temperature_Tyumen` (date_time, temperature) VALUES ('{date_time}', {temperature})"
        connection.execute(ins)


save_weather(api_key, city)
