from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from scripts.leetcode_stats import *


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 7),
    'email': ['chaitanyarai899@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('leetcode_stats_pipeline',
          default_args=default_args,
          description='A DAG for the LeetCode Stats Pipeline',
          schedule_interval=timedelta(days=1))

fetch_tag_task = PythonOperator(
    task_id='fetch_tag_problem_counts',
    python_callable=fetch_tag_problem_counts,
    op_kwargs={'username': 'chaitanyarai'},
    dag=dag,
)

fetch_calendar_task = PythonOperator(
    task_id='fetch_user_profile_calendar',
    python_callable=fetch_user_profile_calendar,
    op_kwargs={'username': 'chaitanyarai', 'year': datetime.now().year},
    dag=dag,
)

fetch_submissions_task = PythonOperator(
    task_id='fetch_recent_ac_submissions',
    python_callable=fetch_recent_ac_submissions,
    op_kwargs={'username': 'chaitanyarai', 'limit': 10},
    dag=dag,
)

fetch_solved_task = PythonOperator(
    task_id='fetch_user_problem_solved',
    python_callable=fetch_user_problem_solved,
    op_kwargs={'username': 'chaitanyarai'},
    dag=dag,
)

insert_stats_task = PythonOperator(
    task_id='insert_stats_to_db',
    python_callable=insert_stats_to_db,
    op_kwargs={'username': 'chaitanyarai'},
    dag=dag,
)

# Define dependencies
fetch_tag_task >> insert_stats_task
fetch_calendar_task >> insert_stats_task
fetch_submissions_task >> insert_stats_task
fetch_solved_task >> insert_stats_task
