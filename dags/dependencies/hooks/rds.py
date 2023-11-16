from airflow.hooks.base import BaseHook
from airflow.models.variable import Variable
from psycopg2 import connect
from sqlalchemy import create_engine
import pandas as pd
import boto3
import json
from psycopg2.extras import execute_values



class RdsHook(BaseHook):

    def __init__(self,
                 secrets_manager_id: str or None = None,
                 db_name: str or None = None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        if secrets_manager_id is None:
            self.secrets_manager_id = Variable.get('default_secrets_manager_id')
        else:
            self.secrets_manager_id = secrets_manager_id
        self.db_name = db_name
        self.conn = self.get_conn()
        self.conn.autocommit = True


    def get_conn(self):
        user, password, host, port, dbname = self.get_secret_key()
        if self.db_name is not None:
            dbname = self.db_name
        self.conn = connect(host=host, port=port, database=dbname, user=user, password=password, connect_timeout=60)
        return self.conn

    def get_secret_key(self):
        secret_manager = boto3.client('secretsmanager')
        secret = json.loads(secret_manager.get_secret_value(SecretId=self.secrets_manager_id)['SecretString'])
        if 'dbname' not in secret.keys():
            secret['dbname'] = 'ptcbtrtdb'
        return secret['username'], secret['password'], secret['host'], secret['port'], secret['dbname']

    def get_engine(self):
        user, password, host, port, dbname = self.get_secret_key()

        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')
        return engine

    def get_pandas_df(self, sql:str, parameter: list or tuple or dict or None = None) -> pd.DataFrame:
        return pd.read_sql_query(sql, self.get_engine(), params=parameter)

    def get_records(self, sql) -> list:
        cursor = self.conn.cursor()
        cursor.execute(sql)
        fetch_data = cursor.fetchall()
        cursor.close()
        return fetch_data

    def execute(self, sql: str, params: tuple or None = None):
        cur = self.conn.cursor()
        cur.execute(sql, params)
        self.conn.commit()
        cur.close()

    def execute_many(self, sql, data):
        cur = self.conn.cursor()
        cur.executemany(sql, data)
        self.conn.commit()
        cur.close()

    def execute_values(self, sql, data):
        cur = self.conn.cursor()
        execute_values(cur, sql, data)
        self.conn.commit()
        cur.close()