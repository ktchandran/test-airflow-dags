from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
# from airflow.models.dagwarning import DagWarning, DagWarningType
# from airflow.utils.session import provide_session
from airflow import settings

default_args = {
    'owner': 'Data Analytics',
    'depends_on_past': False,
    'start_date': datetime(2022, 5, 16),
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'in_cluster': True,
}


def set_dags(**kwargs):
    session = settings.Session()
    print("session: ", str(session))
    stmt = "Select * from dag;"
    result = session.execute(stmt)
    print(f"Attributes - {result._metadata.keys}")
    for row in result:
        print(f"Result from task instance - {row}")


def task2():
    print("Running task2")


def task3():
    print("Running task3")


dag = DAG(
    'test_dag',
    description='A simple DAG',
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2022, 1, 1),
)

task1 = PythonOperator(
    task_id='task1',
    python_callable=set_dags,
    dag=dag,
)

task2 = PythonOperator(
    task_id='task2',
    python_callable=task2,
    dag=dag,
)

task3 = PythonOperator(
    task_id='task3',
    python_callable=task3,
    dag=dag,
)

task1 >> task2
task1 >> task3

