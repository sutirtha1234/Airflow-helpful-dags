# -*- coding: utf-8 -*-
"""
DAG to show how we can use basic libraries to run Transactions on MySQL dbs in Airflow
"""

import logging
from datetime import datetime
from airflow import DAG
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.python_operator import PythonOperator


# Database connection variables
CONNECTION_ID = 'id_stored_in_airflow'

default_args = {
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 1),
    'provide_context': True,
    'retries': 0
}

dag = DAG(
    'mysql_transaction_sample',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)


# Python function 
def run_transaction(**kwargs):
    """
    Run a transaction.
    :return: True
    """
    
    # Connection ID is name of connection in Admin settings
    mysql_execute_hook = MySqlHook(mysql_conn_id=CONNECTION_ID)
    connection = mysql_execute_hook.get_conn()
    
    try:
        cursor = connection.cursor()
        # query1
        query1 = 'UPDATE table1 SET value = 1 WHERE id = 1'
        cursor.execute(query1)

        
        # query2
        query2 = 'UPDATE table2 SET value = 2 WHERE id = 2'
        cursor.execute(query2)

        connection.commit()
    except Exception as ex:
        logging.error('Airflow DB Session error: %s', str(ex))
        connection.rollback()
        raise
    finally:
        if connection:
            connection.close()

    return True


# The task
run_db_updates = PythonOperator(
    task_id='run_db_updates',
    python_callable=run_transaction,
    provide_context=True,
    dag=dag,
)

# Task order
run_db_updates
