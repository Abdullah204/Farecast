from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

import clean_and_transform as ct
import extract_additional_resources as ear

import my_dashboard
import integrate_and_ingest as iai


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(2),
    "retries": 1,
}


dag = DAG(
    "nyc_etl_pipeline",
    default_args=default_args,
    description="nyc etl pipeline",
)

with DAG(
    dag_id="nyc_etl_pipeline",
    schedule_interval="@once",
    default_args=default_args,
    tags=["nyc-pipeline"],
) as dag:
    clean_and_transform = PythonOperator(
        task_id="clean_and_transform",
        python_callable=ct.clean_and_transform,
        op_kwargs={},
    )
    extract_additional_resources = PythonOperator(
        task_id="extract_additional_resources",
        python_callable=ear.extract_additional_resources,
        op_kwargs={},
    )
    integrate_and_ingest = PythonOperator(
        task_id="integrate_and_ingest",
        python_callable=iai.integrate_and_ingest,
        op_kwargs={},
    )
    create_dashboard_task = PythonOperator(
        task_id="create_dashboard_task",
        python_callable=my_dashboard.create_dashboard,
        op_kwargs={},
    )

    (
        clean_and_transform
        >> extract_additional_resources
        >> integrate_and_ingest
        >> create_dashboard_task
    )
