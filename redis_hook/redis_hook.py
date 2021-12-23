from typing import Any
from redis import StrictRedis
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException


class RedisHook(BaseHook):
    # Override to provide the connection name.
    conn_id = None  # type: str
    # Override to have a default connection id for a particular dbHook
    # Override if this db supports autocommit.
    supports_autocommit = False
    # Override with the object that exposes the connect method
    connector = None  # type: StrictRedis

    def __init__(self, conn_id):
        super().__init__()
        self.connector = StrictRedis
        if conn_id:
            self.conn_id = conn_id
        else:
            raise AirflowException("conn_id is not defined")

    def get_conn(self) -> Any:
        return self.connector.from_url(self.get_uri(), decode_responses=True)

    def get_uri(self) -> str:
        """
        Extract the URI from the connection.

        :return: the extracted uri.
        """
        conn = self.get_connection(self.conn_id)
        login = ''
        if conn.login:
            login = f'{conn.login}:{conn.password}@'
        if not conn.login and conn.password:
            login = f':{conn.password}@'
        host = conn.host
        if conn.port is not None:
            host += f':{conn.port}'
        uri = f'{conn.conn_type}://{login}{host}/'
        if conn.schema:
            uri += conn.schema
        return uri
