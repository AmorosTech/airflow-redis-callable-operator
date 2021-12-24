from typing import Any
from typing import Callable, Dict, List, Optional
import os
import json
import time
import logging

from airflow.models import BaseOperator
from airflow.utils.operator_helpers import determine_kwargs
from airflow.exceptions import AirflowException
from redis_hook.redis_hook import RedisHook


def task(python_callable: Optional[Callable] = None, multiple_outputs: Optional[bool] = None, **kwargs):
    """
    Deprecated function that calls @task.python and allows users to turn a python function into
    an Airflow task. Please use the following instead:

    from airflow.decorators import task

    @task
    def my_task()

    :param python_callable: A reference to an object that is callable
    :type python_callable: python callable
    :param op_kwargs: a dictionary of keyword arguments that will get unpacked
        in your function (templated)
    :type op_kwargs: dict
    :param op_args: a list of positional arguments that will get unpacked when
        calling your callable (templated)
    :type op_args: list
    :param multiple_outputs: if set, function return value will be
        unrolled to multiple XCom values. Dict will unroll to xcom values with keys as keys.
        Defaults to False.
    :type multiple_outputs: bool
    :return:
    """
    # To maintain backwards compatibility, we import the task object into this file
    # This prevents breakages in dags that use `from airflow.operators.python import task`
    from airflow.decorators.python import python_task
    return python_task(python_callable=python_callable, multiple_outputs=multiple_outputs, **kwargs)


class RedisOperator(BaseOperator):
    """
    Executes a Python callable

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:RedisOperator`

    :param python_callable: A reference to an object that is callable
    :type python_callable: python callable
    :param op_kwargs: a dictionary of keyword arguments that will get unpacked
        in your function
    :type op_kwargs: dict (templated)
    :param op_args: a list of positional arguments that will get unpacked when
        calling your callable
    :type op_args: list (templated)
    :param templates_dict: a dictionary where the values are templates that
        will get templated by the Airflow engine sometime between
        ``__init__`` and ``execute`` takes place and are made available
        in your callable's context after the template has been applied. (templated)
    :type templates_dict: dict[str]
    :param templates_exts: a list of file extensions to resolve while
        processing templated fields, for examples ``['.sql', '.hql']``
    :type templates_exts: list[str]
    :param task_id Task unique ID
    :param task_params redis Subscription message information type: str or json str
    :param task_timeout Task timeout, value default one hour, Configurable environment variable key AIRFLOW_TASK_TIMEOUT
    :param task_wait task need to wait , value default True, values for examples [True, False]
    :param sleep_time sleep time, value default 60 seconds, Configurable environment variable keyAIRFLOW_SLEEP_TIME
    :param depends_on_past value default False, values for examples [True, False], When set to true, the task will not be triggered if the previous task plan is not successfully executed
    :param retries Task failed retries, default not retries
    :param connection_id
    """
    template_fields = ('templates_dict', 'op_args', 'op_kwargs')
    template_fields_renderers = {"templates_dict": "json", "op_args": "py", "op_kwargs": "py"}
    BLUE = '#ffefeb'
    ui_color = BLUE

    # since we won't mutate the arguments, we should just do the shallow copy
    # there are some cases we can't deepcopy the objects(e.g protobuf).
    shallow_copy_attrs = (
        'python_callable',
        'op_kwargs',
    )

    hive_server2_connection_id: str = None
    hive_server2: RedisHook = None

    def __init__(self, task_id: str, connection_id: str, task_params: str = None, task_timeout: int = None,
                 task_wait: bool = True, sleep_time: int = None,
                 depends_on_past: bool = False,
                 retries: int = 0, *, python_callable: Callable,
                 op_args: Optional[List] = None,
                 op_kwargs: Optional[Dict] = None,
                 templates_dict: Optional[Dict] = None,
                 templates_exts: Optional[List[str]] = None,
                 **kwargs):
        params = {'task_id': task_id}
        if task_wait is not None:
            params['task_wait'] = task_wait
        if task_wait is True:
            if task_timeout is None or task_timeout <= 0:
                task_timeout = os.environ.get("AIRFLOW_TASK_TIMEOUT") or 1000 * 60 * 60
                self.task_timeout = task_timeout
            params['task_timeout'] = task_timeout
            if sleep_time is None or sleep_time <= 0:
                sleep_time = os.environ.get("AIRFLOW_SLEEP_TIME") or 60
                self.sleep_time = sleep_time
            params['sleep_time'] = sleep_time
        if task_params is not None:
            params["task_params"] = task_params
        self.hive_server2_connection_id = connection_id
        self.hive_server2 = RedisHook(self.hive_server2_connection_id)
        params["hive_server2_connection_id"] = connection_id
        self.task_params = task_params
        self.params = params
        self.depends_on_past = depends_on_past
        self.retries = retries
        self.task_id = task_id
        super().__init__(task_id=task_id, params=params, retries=retries,
                         depends_on_past=depends_on_past, **kwargs)
        if not callable(python_callable):
            raise AirflowException('`python_callable` param must be callable')
        self.python_callable = python_callable
        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or []
        self.templates_dict = templates_dict
        self.templates_exts = templates_exts

    def execute(self, context: Any):
        context.update(self.op_kwargs)
        context['templates_dict'] = self.templates_dict

        self.op_kwargs = determine_kwargs(self.python_callable, self.op_args, context)

        return_value = self.execute_callable()
        self.log.info("Done. Returned value was: %s", return_value)
        return return_value

    def execute_callable(self):
        """
        Calls the python callable with the given arguments.

        :return: the return value of the call.
        :rtype: any
        """
        return self.python_callable(*self.op_args, **self.op_kwargs)

    def get_redis_hook(self) -> RedisHook:
        return self.hive_server2

    def get_redis_client(self):
        if self.hive_server2 is None:
            raise AirflowException('`redis not init`')
        return self.hive_server2.get_conn()


class DefaultCallableRedisOperator(RedisOperator):
    def __init__(self, **kwargs):
        super().__init__(python_callable=self.receive_param, **kwargs)

    @staticmethod
    def receive_param(**kwargs):
        prefix = os.environ.get('AIRFLOW__CELERY__REDIS_PREFIX') or "airflow"
        redis_hash_name = os.environ.get('AIRFLOW__CELERY__REDIS_HAS_NAME') or "airflow"
        """通过api接口调度job获取接口参数"""
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
            hive_server2_connection_id = param['hive_server2_connection_id']
            if not hive_server2_connection_id:
                raise AirflowException('`hive_server2_connection_id is undefined`')
            hive_server2 = RedisHook(hive_server2_connection_id)
            client = hive_server2.get_conn()
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
                        raise AirflowException(' job: {}, task: {} 超时失败'.format(dag_id, task_id))
                        break
                    key_value = client.hget(redis_hash_name, key)
                    logging.info(" end: {}, sleep_time: {} ".format(key_value, sleep_time))
                    if "true" == key_value:
                        client.hdel(key)
                        break
                    elif "false" == key_value:
                        raise AirflowException(' job: {}, task: {} 失败'.format(dag_id, task_id))
                        break
                    else:
                        time.sleep(sleep_time)
        except KeyError:
            raise AirflowException(' job: {}, task: {} 失败'.format(dag_id, task_id))
            pass
        logging.info("参数为：{}".format(param))

    @staticmethod
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
