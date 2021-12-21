import redis
from redis import StrictRedis
import json
import time
import os

from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator

broker_url = os.environ.get('AIRFLOW__CELERY__BROKER_URL')
print(" broker_url: {} ".format(broker_url))
client = StrictRedis.from_url(url=broker_url, decode_responses=True)


def receive_param(**kwargs):
    """通过api接口调度job获取接口参数"""
    print("redis.version: {} ".format(redis.VERSION))
    print(" broker_url: {} ".format(broker_url))
    if client is None:
        raise AirflowException('`redis not init`')
    try:
        start_time = time.time()
        task_id = kwargs["task_instance"].task_id
        dag_id = kwargs["task_instance"].dag_id
        execution_date_str = kwargs["task_instance"].execution_date.strftime('%Y%m%dT%H%M%S')
        param = kwargs["params"]
        key = dag_id + "_" + task_id + "_" + execution_date_str
        param["task_instance_key"] = key
        param["task_id"] = task_id
        xxl_job_timeout = param['xxl_job_timeout']
        redis_message = json.dumps(param)
        print(" message: {}".format(redis_message))
        client.publish("airflow-missions", redis_message)
        finish_flag = False
        while not finish_flag:
            now__diff_time = time.time() - start_time
            if now__diff_time > xxl_job_timeout:
                raise AirflowException('missions job: {}, task: {} 超时失败'.format(dag_id, task_id))
                break
            key_value = client.get(key)
            print(" end: {} ".format(key_value))
            if "true" == key_value:
                client.delete(key)
                break
            elif "false" == key_value:
                raise AirflowException('missions job: {}, task: {} 失败'.format(dag_id, task_id))
                break
    except KeyError:
        try:
            param = process_param(kwargs)
        except KeyError:
            param = ""
    print("参数为：{}".format(param))


def process_param(**kwargs):
    # """从Xcom中获取job参数"""
    print("参数: {}".format(kwargs))
    param = ""
    try:
        print("参数: {}".format(kwargs["ti"]))
        param = kwargs["ti"].xcom_pull()
    except KeyError:
        param = ""
    print("参数为：{}".format(param))
    return param


def dag_python_operator(task_id: str, xxl_job_params: str = None, xxl_job_timeout: int = None,
                        provide_context: bool = None, retries: int = 1):
    """返回PythonOperator task任务
      task_id：任务ID
      xxl_job_params：xxl_job参数
      xxl_job_timeout：任务超时时间
      provide_context：dag上下文context传参数，参数是否传递
      retries：重试次数
    """
    if provide_context is None:
        provide_context = True
    if xxl_job_timeout is None or xxl_job_timeout <= 0:
        xxl_job_timeout = 1000 * 60 * 60
    params = {'task_id': task_id, 'xxl_job_timeout': xxl_job_timeout}
    if xxl_job_params is not None:
        params["xxl_job_params"] = xxl_job_params
    return PythonOperator(
        task_id=task_id,
        depends_on_past=False,
        params=params,
        provide_context=provide_context,
        python_callable=receive_param,
        retries=retries
    )
