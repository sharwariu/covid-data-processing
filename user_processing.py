import json
from datetime import datetime
from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

def insert_data_to_postgres(ti):
    response_text = ti.xcom_pull(task_ids='get_posts')
    ti.log.info(f'Received response from API: {response_text}')

    try:
        response_json = json.loads(response_text)
    except json.JSONDecodeError as e:
        ti.log.error(f'Error parsing JSON data: {e}')
        raise

    postgres_hook = PostgresHook(postgres_conn_id='postgres')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    district_data_list = []

    for state_name, state_data in response_json.items():
        covid_data_list = state_data.get("districtData", {})

        for district_name, district_data in covid_data_list.items():
            delta = district_data["delta"]
            district_data_list.append(
                {
                    "state_name": state_name,
                    "district_name": district_name,
                    "notes": district_data["notes"],
                    "active": district_data["active"],
                    "confirmed": district_data["confirmed"],
                    "migratedother": district_data["migratedother"],
                    "deceased": district_data["deceased"],
                    "recovered": district_data["recovered"],
                    "delta_confirmed": delta["confirmed"],
                    "delta_deceased": delta["deceased"],
                    "delta_recovered": delta["recovered"],
                }
            )

    district_data_list.sort(key=lambda x: x["active"], reverse=True)

    for district_data in district_data_list:
        cursor.execute(
            "INSERT INTO covid_data (state_name, district_name, notes, active, confirmed, migratedother, deceased, recovered, delta_confirmed, delta_deceased, delta_recovered) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                district_data["state_name"],
                district_data["district_name"],
                district_data["notes"],
                district_data["active"],
                district_data["confirmed"],
                district_data["migratedother"],
                district_data["deceased"],
                district_data["recovered"],
                district_data["delta_confirmed"],
                district_data["delta_deceased"],
                district_data["delta_recovered"],
            )
        )

    conn.commit()
    conn.close()

with DAG(
    dag_id='user_processing',
    schedule_interval='@daily',
    start_date=datetime(2023, 9, 14),
    catchup=False
) as dag:

    task_is_api_active = HttpSensor(
        task_id='is_api_active',
        http_conn_id='api_conn',
        endpoint='state_district_wise.json'
    )

    task_get_posts = SimpleHttpOperator(
        task_id='get_posts',
        http_conn_id='api_conn',
        endpoint='state_district_wise.json',
        method='GET',
        response_filter=lambda response: response.text if response and response.text else None,
        log_response=True
    )

    task_create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres',
        sql='''
            CREATE TABLE IF NOT EXISTS covid_data (
                state_name VARCHAR(255),
                district_name VARCHAR(255),
                notes TEXT,
                active INT,
                confirmed INT,
                migratedother INT,
                deceased INT,
                recovered INT,
                delta_confirmed INT,
                delta_deceased INT,
                delta_recovered INT
            );
        '''
    )

    task_save_to_postgres = PythonOperator(
        task_id='save_to_postgres',
        python_callable=insert_data_to_postgres
    )

    task_is_api_active >> task_get_posts >> task_create_table >> task_save_to_postgres
