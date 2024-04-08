# Extend from the official Airflow image
FROM apache/airflow:2.6.3-python3.10

# Install python-dotenv or any other dependencies
RUN pip install psycopg2-binary python-dotenv
