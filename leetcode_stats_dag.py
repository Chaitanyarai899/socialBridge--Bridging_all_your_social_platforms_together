from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from leetcode_stats import fetch_leetcode_stats, insert_stats_to_db, create_db_and_schema

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 31),
}

with DAG('leetcode_stats_dag',
         default_args=default_args,
         schedule_interval='@daily',
         ) as dag:
    
    def run_leetcode_stats():
        username = "chaitanyarai"
        data = fetch_leetcode_stats(username)
        create_db_and_schema()
        insert_stats_to_db(data, username)

    task1 = PythonOperator(
        task_id='fetch_and_insert_leetcode_stats',
        python_callable=run_leetcode_stats
    )

    task1
