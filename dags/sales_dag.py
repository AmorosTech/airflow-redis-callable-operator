from datetime import datetime, timedelta
from textwrap import dedent
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from redis_operator.redis_operator import RedisOperator, DefaultCallableRedisOperator
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
    t1 = RedisOperator(
        task_id='DailyRetailBatchHandler',
        connection_id='airflow_redis',
        task_params='agentIds=levis startDate(date)=2021/12/02 endDate(date)=2021/12/09',  # 参数订阅消息，根据需要可以是json字符串
        python_callable=receive_param  # 回调方法
    )

    t2 = DefaultCallableRedisOperator(
        task_id='FaceRetailMatchHandler',
        task_params='agentIds=levis startDate(date)=2021/12/02 endDate(date)=2021/12/09',
        connection_id='airflow_redis',  # connection连接ID对应数据库表connection的conn_id
    )

    t3 = DefaultCallableRedisOperator(
        task_id='flowStayDetailHandler',
        task_params='agentIds=levis startDate(date)=2021/12/02 endDate(date)=2021/12/09',
        connection_id='airflow_redis',
    )

    t4 = DefaultCallableRedisOperator(
        task_id='storeDailyStatsHandler',
        task_params='agentIds=levis isSeven=false startDate(date)=2021/12/02 endDate(date)=2021/12/09 enableCustomer=false enableInv=false enableStaff=false realtime=false',
        task_timeout=600,  # 任务600秒没完成超时停止
        connection_id='airflow_redis',
    )

    t5 = DefaultCallableRedisOperator(
        task_id='endStatisticshandler',
        task_wait=False,
        connection_id='airflow_redis',
    )

    t1 >> t2 >> [t3, t4] >> t5
