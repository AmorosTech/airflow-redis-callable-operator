from typing import Dict, Optional
import os
import json
import time

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from redis_hook.redis_hook import RedisHook

boolStr = {'True': True, 'true': True, 't': True, '1': True, 'False': False, 'false': False, 'f': False, '0': False}


def get_bool_value(key):
    try:
        return boolStr[key]
    except KeyError:
        return False


class RedisCallableOperator(BaseOperator):
    """
    Executes a Python callable

    .. see also::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:RedisOperator`
    :param task_id Task unique ID
    :param channel message publish topic, redis channel to which the message is published (templated)
    :param prefix message publish topic prefix
    :param relate_instance message publish topic Associated or not instance
    :param redis_type 0: hash key, 1: key
    :param redis_hash_name redis_type=0 redis_hash_name value is valid
    :param redis_conn_id redis connection to use
    :param message the message to publish (templated)
    :param task_params redis Subscription message information type: str or json str
    :param task_wait task need to wait , value default True, values for examples [True, False]
    :param task_timeout Task timeout, value default one hour, Configurable environment variable key AIRFLOW_TASK_TIMEOUT, when task_wait value is True, task_timeout is valid
    :param sleep_time sleep time, value default 60 seconds, Configurable environment variable keyAIRFLOW_SLEEP_TIME, when task_wait value is True, sleep_time is valid
    :param depends_on_past value default False, values for examples [True, False], When set to true, the task will not be triggered if the previous task plan is not successfully executed
    :param retries Task failed retries, default not retries

    """
    template_fields = (
        'channel', 'prefix', 'redis_conn_id', 'message', 'task_params', 'task_wait', 'task_timeout', 'sleep_time',
        'redis_hash_name',
        'redis_type')

    def __init__(self,
                 *,
                 channel: str = os.environ.get('AIRFLOW__CELERY__REDIS_CHANNEL') or None,
                 prefix: str = os.environ.get('AIRFLOW__CELERY__REDIS_PREFIX') or None,
                 relate_instance: bool = get_bool_value(os.environ.get('AIRFLOW__CELERY__REDIS_RELATE_INSTANCE')),
                 redis_type: int = 0,
                 redis_hash_name: str = os.environ.get('AIRFLOW__CELERY__REDIS_HAS_NAME') or None,
                 redis_conn_id: str = 'redis_default',
                 message: Optional[Dict] = None,
                 task_params: str = None,
                 task_wait: bool = True,
                 task_timeout: int = os.environ.get("AIRFLOW_TASK_TIMEOUT") or 1000 * 60 * 60,
                 sleep_time: int = os.environ.get("AIRFLOW_SLEEP_TIME") or 60,
                 **kwargs):
        super().__init__(**kwargs)
        self.channel = channel
        self.prefix = prefix
        self.relate_instance = relate_instance
        self.redis_type = redis_type
        self.redis_hash_name = redis_hash_name
        if not message:
            message = {}
        self.task_wait = task_wait
        if task_wait is not None:
            message['task_wait'] = task_wait
        if task_wait is True:
            self.task_timeout = task_timeout
            message['task_timeout'] = task_timeout
            self.sleep_time = sleep_time
            message['sleep_time'] = sleep_time
        if task_params is not None:
            message["task_params"] = task_params
        self.redis_conn_id = redis_conn_id
        message["redis_conn_id"] = redis_conn_id
        self.redis_hook = RedisHook(conn_id=self.redis_conn_id)
        self.task_params = task_params
        self.message = message

    def execute(self, context: Dict):
        client = self.redis_hook.get_conn()
        self.log.info('context: %s', context)
        start_time = time.time()
        self.log.info(" start_time: %s ", start_time)
        task_id = context["task_instance"].task_id
        dag_id = context["task_instance"].dag_id
        execution_date_str = context["task_instance"].execution_date.strftime('%Y%m%dT%H%M%S')
        param = context["params"]
        key = dag_id + "_" + task_id + "_" + execution_date_str
        self.message["task_instance_key"] = key
        self.message["task_id"] = task_id
        if not param['task_params']:
            self.message['task_params'] = param['task_params']
        redis_message = json.dumps(self.message)
        topic = ""
        if self.prefix:
            topic = topic + self.prefix + "_"
        if self.channel:
            topic = topic + self.channel + "_"
        if self.relate_instance is True:
            topic = topic + key
        self.log.info(" topic: %s, message: %s ", topic, redis_message)
        client.publish(topic, redis_message)
        if self.task_wait is True:
            finish_flag = False
            while not finish_flag:
                now__diff_time = time.time() - start_time
                if now__diff_time > self.task_timeout:
                    raise AirflowException(' job: %s, task: %s 超时失败', dag_id, task_id)
                    break
                time.sleep(self.sleep_time)
                if self.redis_type == 0:
                    self.log.info(" hash name: %s, key: %s ", self.redis_hash_name, key)
                    key_value = client.hget(self.redis_hash_name, key)
                else:
                    self.log.info(" end: %s, sleep_time: %s ", key_value, self.sleep_time)
                    key_value = client.get(key)
                self.log.info(" end: %s, sleep_time: %s ", key_value, self.sleep_time)
                if "true" == key_value:
                    client.hdel(self.redis_hash_name, key)
                    break
                elif "false" == key_value:
                    raise AirflowException(' job: %s, task: %s 失败', dag_id, task_id)
                    break
        self.log.info('Sending message %s to Redis on channel %s', self.message, self.channel)
