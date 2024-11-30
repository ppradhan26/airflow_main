from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta  # Import timedelta

# Sample functions to be executed in each step
def task_1():
    print("Task 1: Starting the process")

def task_2():
    print("Task 2: Processing data")

def task_3():
    print("Task 3: Completing the process")

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # Corrected by importing timedelta
    'start_date': datetime(2024, 11, 17),
}

# Define the DAG
with DAG(
    'sample_dag_three_steps',
    default_args=default_args,
    description='A simple DAG with three tasks',
    schedule_interval=None,  # This DAG will run only manually or triggered by external event
    catchup=False,
) as dag:

    # Define tasks
    step_1 = PythonOperator(
        task_id='task_1',
        python_callable=task_1,
    )

    step_2 = PythonOperator(
        task_id='task_2',
        python_callable=task_2,
    )

    step_3 = PythonOperator(
        task_id='task_3',
        python_callable=task_3,
    )

    # Set task dependencies (task_1 -> task_2 -> task_3)
    step_1 >> step_2 >> step_3
