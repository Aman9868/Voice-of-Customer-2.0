from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import requests
import json

APP_DATA = [
    {'app_id': 'com.application.zomato', 'app_name': 'Zomato'},
    {'app_id': 'in.swiggy.android', 'app_name': 'Swiggy'},
    {'app_id': 'com.done.faasos', 'app_name': 'EatSure'},
    {'app_id': 'com.Dominos','app_name': 'Dominos'},
    {'app_id': 'com.ubercab.eats','app_name': 'Uber Eats'},
    {'app_id': 'com.global.foodpanda.android','app_name': 'Food Panda'},
    {'app_id':'com.zeptoconsumerapp','app_name': 'Zepto'},
    {'app_id':'com.grofers.customerapp','app_name': 'Blinkit'},
    {'app_id':'com.bigbasket.mobileapp','app_name': 'BigBasket'},
    {'app_id':'com.trycaviar.customer','app_name':'Cavair'},
    
]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1), \
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
with DAG(dag_id='weather_etl_pipeline',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dags:
    @task
    def scrapeplaystore_data():
        """Extract PlayStore data from Various Sources"""
        mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()
        ##---Create Table if Doesn exist
        cursor.execute("""
                       CREATE TABLE IF NOT EXISTS playstore(
                           app_id VARCHAR(255) PRIMARY KEY,
                           app_name VARCHAR(255),
                           userName VARCHAR(255),
                           userImage VARCHAR(255),
                           content TEXT,
                           score FLOAT,
                           thumbsUpCount INT,
                           reviewCreatedVersion VARCHAR(50),
                           at DATETIME,
                           replyContent TEXT,
                           repliedAt DATETIME,
                           appVersion VARCHAR(50)
                                )
                       """)