from airflow.hooks.base import BaseHook
from airflow.models import Connection


class Connections(object):

    @staticmethod
    def get_connection_params(conn_id):
        conn: Connection = BaseHook.get_connection(conn_id)
        extra_params = conn.extra_dejson
        params = {
            'dbname': conn.schema,
            'user': conn.login,
            'password': conn.get_password(),
            'host': conn.host,
            'port': conn.port,
        }
        params = {**params, **extra_params}
        return params

    @staticmethod
    def get_aws_connection_params(conn_id):
        conn: Connection = BaseHook.get_connection(conn_id)
        extra_params = conn.extra_dejson
        params = {
            'dbname': extra_params["bucket"],
            'user': conn.login,
            'password': conn.get_password(),
            'host': extra_params["host"],
        }
        params = {**params, **extra_params}
        return params