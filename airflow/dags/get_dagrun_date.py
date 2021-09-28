# -*- coding: utf-8 -*-

"""
### DAG to show how to get the execution date instead of just today()
when running DAG for older dates or recalculating older runs.
"""
from datetime import date, datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    "depends_on_past": False,
    "start_date": datetime(2021, 8, 8),
    "provide_context": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "sample_date_dag_1",
    default_args=default_args,
    description="prints date and ds. DS gives the execution date",
    schedule_interval="@daily",
    catchup=True
)


def date_test(**kwargs):
    """
    Runs ../../hql/demo.q on grid and returns the results.
    """
    print(date.today().strftime('%Y%m%d'))
    print(kwargs['ds'])


print_date = PythonOperator(
    task_id="print_date",
    python_callable=date_test,
    provide_context=True,
    dag=dag,
)

print_date
