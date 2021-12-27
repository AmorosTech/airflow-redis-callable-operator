from datetime import datetime, timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from redis_operator.RedisPublisherOperator import RedisPublisherOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 1,  # 失败后重试次数
    'retry_delay': timedelta(minutes=5),  # 失败多久后重试
    'trigger_rule': 'all_success'
}
with DAG(
        'storage',
        default_args=default_args,
        description='销售完成后的统计',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['storage'],
) as dag:
    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = RedisPublisherOperator(
        task_id='StorageBatchHandler',
        redis_conn_id='airflow_redis',
        depends_on_past=False,
        task_params='agentIds=levis startDate(date)=2021/12/02 endDate(date)=2021/12/09',  # 参数订阅消息，根据需要可以是json字符串
    )
    t1
