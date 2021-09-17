"""
Add additional DAGs folders
https://xnuinside.medium.com/how-to-load-use-several-dag-folders-airflow-dagbags-b93e4ef4663c
"""
import os
from airflow.models import DagBag

parent_directory = os.environ.get('AIRFLOW_HOME')
current_env = os.environ.get('DEPLOY_ENV')

dags_dirs = []
if current_env == 'dev':
    dags_dirs.append("{}/tests_dev".format(parent_directory))
elif current_env == 'prod':
    dags_dirs.append("{}/tests_prod".format(parent_directory))

for dir in dags_dirs:
    dag_bag = DagBag(os.path.expanduser(dir))

    if dag_bag:
        for dag_id, dag in dag_bag.dags.items():
            globals()[dag_id] = dag
