# -*- coding: utf-8 -*-
"""
Gets a dagname from Variable and prints next run datetime
https://stackoverflow.com/questions/63527204/airflow-how-to-get-all-the-future-run-date
"""
import pendulum
from datetime import datetime, timedelta
from airflow import DAG, models
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

LOCAL_TZ = pendulum.timezone('Asia/Tokyo')
default_args = {
    "depends_on_past": False,
    "start_date": datetime(2021, 9, 1, tzinfo=LOCAL_TZ),
}

dag = DAG(
    "next_run_schedule",
    default_args=default_args,
    description="gets a dagname from Variable and prints next run datetime",
    schedule_interval=None,
    catchup=False
)


def print_next_datetime(**kwargs):
    """
    Prints datetime of next run
    """
    dag_id = Variable.get('target_dag')
    print("DAG chosen is :", dag_id)

    dag_bag = models.DagBag()
    target_dag = dag_bag.get_dag(dag_id)

    now = datetime.now()
    until = now + timedelta(days=2)

    runs = target_dag.get_run_dates(start_date=now, end_date=until)
    for run in runs:
        print(run)


print_dagruns_task = PythonOperator(
    task_id="print_dagruns_task",
    python_callable=print_next_datetime,
    provide_context=True,
    dag=dag,
)

print_dagruns_task
