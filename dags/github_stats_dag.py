from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


from scripts.github_stats import (
    fetch_commits_today,
    fetch_user_repos,
    analyze_user,
    save_to_database,
    insert_into_db
)

class GithubStatsPipeline:
    def __init__(self, dag_id, start_date, schedule_interval=timedelta(days=1)):
        self.dag_id = dag_id
        self.start_date = start_date
        self.schedule_interval = schedule_interval
        self.dag = DAG(
            dag_id=dag_id,
            default_args=self.default_args(),
            description='A DAG for the GitHub Stats Pipeline',
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
        fetch_user_repos_task = PythonOperator(
            task_id='fetch_user_repositories',
            python_callable=fetch_user_repos,
            op_kwargs={'username': 'chaitanyarai899'},
            dag=self.dag,
        )

        fetch_user_commits_task = PythonOperator(
            task_id='fetch_user_commits',
            python_callable=fetch_commits_today,
            op_kwargs={'username': 'chaitanyarai899','repo':'saksham-web'},
            dag=self.dag,
        )

        fetch_user_issues_task = PythonOperator(
            task_id='fetch_user_issues',
            python_callable=analyze_user,
            op_kwargs={'username': 'chaitanyarai899'},
            dag=self.dag,
        )


        combine_data = PythonOperator(
            task_id='combine_data',
            python_callable=insert_into_db,
            op_kwargs={'username': 'chaitanyarai899'},
            dag=self.dag,
        )

        insert_stats_into_db_task = PythonOperator(
            task_id='insert_stats_into_db',
            python_callable=insert_into_db,
            op_kwargs={'username': 'chaitanyarai899'},
            dag=self.dag,
        )

        # Define dependencies
        fetch_user_repos_task >> combine_data
        fetch_user_commits_task >> combine_data
        fetch_user_issues_task >> combine_data
        combine_data >> insert_stats_into_db_task

    def get_dag(self):
        return self.dag


github_stats_dag_instance = GithubStatsPipeline(
    dag_id='github_stats_pipeline',
    start_date=datetime(2024, 4, 7),
).get_dag()