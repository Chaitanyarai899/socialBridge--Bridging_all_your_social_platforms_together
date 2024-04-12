from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


from scripts.leetcode_stats import (
    fetch_tag_problem_counts,
    fetch_user_profile_calendar,
    fetch_recent_ac_submissions,
    fetch_user_problem_solved,
    leetcode_stats_pipeline_endpoint,
    insert_into_db
)


class LeetCodeStatsPipeline:
    def __init__(self, dag_id, start_date, schedule_interval=timedelta(days=1)):
        self.dag_id = dag_id
        self.start_date = start_date
        self.schedule_interval = schedule_interval
        self.dag = DAG(
            dag_id=dag_id,
            default_args=self.default_args(),
            description='A DAG for the LeetCode Stats Pipeline',
            schedule_interval=schedule_interval,
        )
        self.define_tasks()

    def default_args(self):
        return {
            'owner': 'airflow',
            'depends_on_past': False,
            'start_date': self.start_date,
            'email': ['chaitanyarai899@gmail.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        }

    def define_tasks(self):
        fetch_tag_task = PythonOperator(
            task_id='fetch_tag_problem_counts',
            python_callable=fetch_tag_problem_counts,
            op_kwargs={'username': 'chaitanyarai'},
            dag=self.dag,
        )

        fetch_calendar_task = PythonOperator(
            task_id='fetch_user_profile_calendar',
            python_callable=fetch_user_profile_calendar,
            op_kwargs={'username': 'chaitanyarai', 'year': datetime.now().year},
            dag=self.dag,
        )

        fetch_submissions_task = PythonOperator(
            task_id='fetch_recent_ac_submissions',
            python_callable=fetch_recent_ac_submissions,
            op_kwargs={'username': 'chaitanyarai'},
            dag=self.dag,
        )

        fetch_solved_task = PythonOperator(
            task_id='fetch_user_problem_solved',
            python_callable=fetch_user_problem_solved,
            op_kwargs={'username': 'chaitanyarai'},
            dag=self.dag,
        )

        insert_stats_task = PythonOperator(
            task_id='combine_data',
            python_callable=leetcode_stats_pipeline_endpoint,
            op_kwargs={'username': 'chaitanyarai'}, 
            dag=self.dag,
        )

        insert_into_postgreSQL = PythonOperator(
            task_id='insert_stats_into_db',
            python_callable = insert_into_db,
            op_kwargs={'username':'chaitanyarai'},
            dag = self.dag,
        )

        # Define dependencies
        fetch_tag_task >> insert_into_postgreSQL
        fetch_calendar_task >> insert_into_postgreSQL
        fetch_submissions_task >> insert_into_postgreSQL
        fetch_solved_task >> insert_into_postgreSQL
        insert_stats_task >> insert_into_postgreSQL

    def get_dag(self):
        return self.dag


# Creating a DAG instance
leetcode_stats_dag_instance = LeetCodeStatsPipeline(
    dag_id='leetcode_stats_pipeline',
    start_date=datetime(2024, 4, 7),
).get_dag()