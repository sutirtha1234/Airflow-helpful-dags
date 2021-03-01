"""
The airflow sensors only check equality. That means they sense the task running the exact moment the current dag starts. 

Suppose you want your dag to have 3 tasks
Task1 > triggerExternalDag
Task2 > sense the DAG has finished
Task3 > update DB/print done/etc

So sensers will never work for Manual dag execution as the target DAG will start a few miliseconds off and sensors doo exact time equality check.
Main change is 
DR.execution_date > dttm_filter[0]
= changed to > 
To Sense for the end of the DAG AFTER current time 

"""

import os

from airflow.exceptions import AirflowException
from airflow.models import DagBag, DagModel, DagRun, TaskInstance
from airflow.utils.db import provide_session
from sqlalchemy import func
from airflow.operators.sensors import ExternalTaskSensor


class CustomDagSensor(ExternalTaskSensor):
    @provide_session
    def poke(self, context, session=None):
        if self.execution_delta:
            dttm = context['execution_date'] - self.execution_delta
        elif self.execution_date_fn:
            dttm = self._handle_execution_date_fn(context=context)
        else:
            dttm = context['execution_date']

        dttm_filter = [dttm]
        # dttm_filter = dttm if isinstance(dttm, list) else [dttm]
        # serialized_dttm_filter = ','.join(
        #     [datetime.isoformat() for datetime in dttm_filter])

        self.log.info(
            'Poking for %s.%s on %s ... ',
            self.external_dag_id, self.external_task_id, dttm_filter
        )

        DM = DagModel
        TI = TaskInstance
        DR = DagRun
        if self.check_existence:
            dag_to_wait = session.query(DM).filter(
                DM.dag_id == self.external_dag_id
            ).first()

            if not dag_to_wait:
                raise AirflowException('The external DAG '
                                       '{} does not exist.'.format(self.external_dag_id))
            else:
                if not os.path.exists(dag_to_wait.fileloc):
                    raise AirflowException('The external DAG '
                                           '{} was deleted.'.format(self.external_dag_id))

            if self.external_task_id:
                refreshed_dag_info = DagBag(dag_to_wait.fileloc).get_dag(self.external_dag_id)
                if not refreshed_dag_info.has_task(self.external_task_id):
                    raise AirflowException('The external task'
                                           '{} in DAG {} does not exist.'.format(self.external_task_id,
                                                                                 self.external_dag_id))

        if self.external_task_id:
            # .count() is inefficient
            count = session.query(func.count()).filter(
                TI.dag_id == self.external_dag_id,
                TI.task_id == self.external_task_id,
                TI.state.in_(self.allowed_states),
                TI.execution_date > dttm_filter[0],
            ).scalar()
        else:
            # .count() is inefficient
            count = session.query(func.count()).filter(
                DR.dag_id == self.external_dag_id,
                DR.state.in_(self.allowed_states),
                DR.execution_date > dttm_filter[0],
            ).scalar()

        session.commit()
        return count == len(dttm_filter)
