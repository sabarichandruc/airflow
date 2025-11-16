from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.providers.postgres.hooks.postgres import PostgresHook

import requests
from datetime import datetime

@dag(dag_id='user_processing')
def user_processing():
    create_table = SQLExecuteQueryOperator(task_id ="create_table",
                                           conn_id ="postgres",      
                                           sql="""
                                           CREATE TABLE IF NOT EXISTS users (
                                               id INT PRIMARY KEY,
                                               first_name VARCHAR(100),
                                               last_name VARCHAR(100),
                                               email VARCHAR(100),
                                               created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
                                           )
                                           """
                                           )
    @task.sensor(poke_interval=30,timeout=300)
    def is_api_available()-> PokeReturnValue:
        response = requests.get("https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json")
        print(f"API response status code: {response.status_code}")
        if response.status_code == 200:
            condition = True
            fake_users = response.json()
        else:
            condition = False
            fake_users = None

        return PokeReturnValue(is_done=condition,xcom_value=fake_users)
    @task
    def _extract_user(fake_users): 
        return{
            "id": fake_users['id'],
            "first_name": fake_users['personalInfo']['firstName'],
            "last_name": fake_users['personalInfo']['lastName'],
            "email": fake_users['personalInfo']['email']
        }
    
    @task
    def process_user(user_info):
        import csv
        user_info["created_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open('/tmp/processed_users.csv', mode='w', newline='') as file:
            fieldnames = ['id', 'first_name', 'last_name', 'email', 'created_at']
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerow(user_info)

    @task
    def store_user():
        hook = PostgresHook(postgres_conn_id="postgres")
        hook.copy_expert(
            sql="""COPY users FROM STDIN WITH CSV HEADER""",
            filename='/tmp/processed_users.csv'
        )
    process_user(_extract_user(create_table >> is_api_available())) >> store_user()
    
user_processing()








