import json
import time
import os
import logging

from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from redis_hook.redis_hook import RedisHook

hive_server2: RedisHook = RedisHook(conn_id="airflow_redis")
client = hive_server2.get_conn()
# broker_url = os.environ.get('AIRFLOW__CELERY__BROKER_URL')
# print(" broker_url: {} ".format(broker_url))
# client = StrictRedis.from_url(url=broker_url, decode_responses=True)
prefix = os.environ.get('AIRFLOW__CELERY__REDIS_PREFIX') or "airflow"
redis_hash_name = os.environ.get('AIRFLOW__CELERY__REDIS_HAS_NAME') or "airflow"


def receive_param(**kwargs):
    """通过api接口调度job获取接口参数"""
    logging.info(" broker_url: {} ".format(hive_server2.get_uri()))
    if client is None:
        raise AirflowException('`redis not init`')
    try:
        logging.info(" kwargs: %s", kwargs)
        start_time = time.time()
        logging.info(" start_time: {} ".format(start_time))
        task_id = kwargs["task_instance"].task_id
        dag_id = kwargs["task_instance"].dag_id
        execution_date_str = kwargs["task_instance"].execution_date.strftime('%Y%m%dT%H%M%S')
        param = kwargs["params"]
        key = dag_id + "_" + task_id + "_" + execution_date_str
        param["task_instance_key"] = key
        param["task_id"] = task_id
        task_wait = param['task_wait']
        redis_message = json.dumps(param)
        logging.info(" topic: %s, message: %s", prefix + "_" + key, redis_message)
        client.publish(prefix + "_" + key, redis_message)
        finish_flag = False
        if task_wait is True:
            task_timeout = param['task_timeout']
            sleep_time = param['sleep_time']
            while not finish_flag:
                now__diff_time = time.time() - start_time
                if now__diff_time > task_timeout:
                    raise AirflowException('missions job: {}, task: {} 超时失败'.format(dag_id, task_id))
                    break
                time.sleep(sleep_time)
                key_value = client.hget(redis_hash_name, key)
                logging.info(" end: {}, sleep_time: {} ".format(key_value, sleep_time))
                if "true" == key_value:
                    client.hdel(redis_hash_name, key)
                    break
                elif "false" == key_value:
                    raise AirflowException('missions job: {}, task: {} 失败'.format(dag_id, task_id))
                    break
    except KeyError:
        try:
            param = process_param(kwargs)
        except KeyError:
            raise AirflowException(' job: {}, task: {} 失败'.format(dag_id, task_id))
            param = ""
    logging.info("参数为：{}".format(param))


def process_param(**kwargs):
    # """从Xcom中获取job参数"""
    logging.info("参数: {}".format(kwargs))
    param = ""
    try:
        logging.info("参数: {}".format(kwargs["ti"]))
        param = kwargs["ti"].xcom_pull()
    except KeyError:
        param = ""
    logging.info("参数为：{}".format(param))
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
