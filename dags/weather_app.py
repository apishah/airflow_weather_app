from airflow.decorators import dag, task
import datetime
import requests

@dag(
    dag_id = "weather_data_2",
    schedule="@daily",
    start_date=datetime.datetime(2024, 11, 21),
    catchup=True,
    tags=["Weather"],
)
def weather_data():
     
    @task(multiple_outputs=True)
    def todays_weather(date):
        data = requests.get('http://api.weatherstack.com/current?access_key=4755c8ee0ea1c2d773c8a5d43c98094e&query=New York')
        return data.json()
    
    @task()
    def current_weather():
        return datetime.datetime.today()

    date = current_weather()
    todays_weather(date)
weather_data()