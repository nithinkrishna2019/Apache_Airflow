
# import sys
# print(sys.executable)


# import pandas
# print(pandas.__version__)  # Should print the installed pandas version

# import airflow
# print(airflow.__version__)  # Should print the installed airflow version


from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime
from airflow.operators.python import PythonOperator
from ETL_DAG import run_twitter_etl


default_arguments={
    'owner':'airflow',
    'depends_on_past':False,
    'Start_date':datetime(2024,11,8),
    'email':['example@gmail.com'],
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay':timedelta(minutes=1)
}



# dag = DAG(
#     'twitter_ETL_DAG',
#     default_args=default_arguments,
#     description='Our first DAG with ETL process!',
#     schedule_interval=timedelta(days=1),
#     start_date=datetime(2024, 2, 1)
# )

dag = DAG(
    'twitter_ETL_DAG',
    default_args=default_arguments,
    description='ETL DAG for Twitter data processing',
    schedule_interval=None,  # This ensures the DAG runs ONLY when triggered manually
    catchup=False,  # Prevents running past missed intervals
)


run_etl = PythonOperator(
    task_id='complete_twitter_etl',
    python_callable=run_twitter_etl,
    dag=dag, 
)

run_etl