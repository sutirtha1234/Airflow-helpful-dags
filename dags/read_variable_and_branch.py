# -*- coding: utf-8 -*-

"""
### DAG to show how to get variable data and branch
"""
import pendulum
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.models import Variable

LOCAL_TZ = pendulum.timezone('Asia/Tokyo')
default_args = {
    "depends_on_past": False,
    "start_date": datetime(2021, 9, 1, tzinfo=LOCAL_TZ),
    "provide_context": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "provide_context": True
}

dag = DAG(
    "variable_and_branch_2",
    default_args=default_args,
    description="gets variable and branches",
    schedule_interval="@daily",
    catchup=False
)


def get_data(**kwargs):
    """
    Gets Variable data and passes it on
    """
    choice = Variable.get("selection_input")
    print(choice)
    return {"choice": choice}


get_data = PythonOperator(task_id="get_data", python_callable=get_data, dag=dag)


def choose_path(**kwargs):
    """
    Chooses which task to run next based on a value from the previous task.
    :return: Name of the next task
    """
    input_args = kwargs['task_instance'].xcom_pull(
        task_ids='get_data')
    choice = input_args['choice']

    print('Choice value obtained: ', choice)
    if choice == '1':
        return 'task_branch_1'
    elif choice == '2':
        return 'task_branch_2'
    else:
        return 'task_branch_2'


choose_path = BranchPythonOperator(task_id='choose_path', python_callable=choose_path, dag=dag)


def task_branch_1(**kwargs):
    """
    Gets Variable data and passes it on
    """
    print('Creating Feature_1')


def task_branch_2(**kwargs):
    """
    Gets Variable data and passes it on
    """
    print('Creating Feature_2')


task_branch_1 = PythonOperator(task_id="task_branch_1", python_callable=task_branch_1, dag=dag)
task_branch_2 = PythonOperator(task_id="task_branch_2", python_callable=task_branch_2, dag=dag)

get_data >> choose_path
choose_path >> [task_branch_1, task_branch_2]
