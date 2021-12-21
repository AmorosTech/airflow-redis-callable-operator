from datetime import datetime, timedelta
from textwrap import dedent
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator
from callback_function import receive_param, dag_python_operator
# from callback_function import success_callback,failure_callback,receive_param
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'trigger_rule': 'all_success'
}
with DAG(
    'sales',
    default_args=default_args,
    description='销售完成后的统计',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['sales'],
) as dag:
    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = dag_python_operator(
        task_id='DailyRetailBatchHandler'
    )

    t2 = dag_python_operator(
        task_id='FaceRetailMatchHandler'
    )

    t3 = dag_python_operator(
        task_id='flowStayDetailHandler',
    )

    t4 = dag_python_operator(
        task_id='storeDailyStatsHandler',
    )

    t5 = dag_python_operator(
        task_id='endStatisticshandler',
    )

    t1 >> t2 >> [t3, t4]
    t5 << t3
    t5 << t4
