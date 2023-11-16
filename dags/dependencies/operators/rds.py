from typing import Sequence
from airflow.models import BaseOperator
from airflow.models.xcom_arg import PlainXComArg
from airflow.utils.context import Context
from dependencies.hooks.rds import RdsHook
import json
import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator

class DateTimeEncoder(json.JSONEncoder):
    # Override the default method
    def default(self, obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()

class RdsOperator(BaseOperator):
    template_fields: Sequence[str] = ("sql",)
    ui_color = '#5eccf7'

    def __init__(self,
                 sql: str,
                 secret: str or None = None,
                 hook: RdsHook or None = None,
                 parameter: tuple or None = None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.secret = secret
        self.hook = hook
        self.parameter = parameter


    def execute(self, context):
        if self.hook is None:
            self.hook = RdsHook(secrets_manager_id=self.secret)
        if self.sql.split(' ')[0].lower() == 'select':
            records = self.hook.get_pandas_df(self.sql)
            return json.dumps(records.to_dict(orient='records'), cls=DateTimeEncoder)
        else:
            records = self.hook.execute(self.sql, self.parameter)
            return str(records)

class ProcedureOperator(BaseOperator):
    template_fields: Sequence[str] = ("schema", "procedure_name",)
    #ui_color = '#ff8cba'
    ui_color = '#ffffe0'

    def __init__(self,
                 schema: str,
                 procedure_name: str,
                 secret: str or None = None,
                 parameter: any = None,
                 hook: RdsHook or None = None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.schema = schema
        self.procedure_name = procedure_name
        self.parameter = parameter
        self.secret = secret
        self.hook = hook

    def execute(self, context):
        if self.hook is None:
            self.hook = RdsHook(secrets_manager_id=self.secret)
        tmp_tuple: None or tuple = None
        sql = ''
        if self.parameter is None:
            sql = f"CALL {self.schema}.{self.procedure_name}()"
        elif isinstance(self.parameter, tuple):
            tmp_tuple = self.parameter
            sql = f"CALL {self.schema}.{self.procedure_name}({','.join(['%s' for _ in range(len(tmp_tuple))])})"
        elif isinstance(self.parameter, int):
            tmp_tuple = (self.parameter,)
            sql = f"CALL {self.schema}.{self.procedure_name}(%s)"
        elif isinstance(self.parameter, str):
            tmp_tuple = tuple(self.parameter.split(','))
            sql = f"CALL {self.schema}.{self.procedure_name}({','.join(['%s' for _ in range(len(tmp_tuple))])})"
        elif isinstance(self.parameter, list):
            tmp_tuple = tuple(self.parameter)
            sql = f"CALL {self.schema}.{self.procedure_name}({','.join(['%s' for _ in range(len(tmp_tuple))])})"
        elif isinstance(self.parameter, dict):
            tmp_tuple = tuple(self.parameter.values())
            sql = f"CALL {self.schema}.{self.procedure_name}({','.join(['%s' for _ in range(len(tmp_tuple))])})"
        else:
            raise TypeError('parameter type must be tuple, int, str, list, dict')
        result = self.hook.execute(sql, tmp_tuple)
        return result

class RdsInsertOperator(BaseOperator):
    template_fields: Sequence[str] = ("schema", "table", "data")
    ui_color = '#64f55f'

    def __init__(self,
                 schema: str,
                 table: str,
                 columns: list or None = None,
                 secret: str or None = None,
                 data: any = None,
                 subquery: str or None = None,
                 hook: RdsHook or None = None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.schema = schema
        self.table = table
        self.columns = columns
        self.data = data
        self.subquery = subquery
        self.secret = secret
        self.hook = hook

    def execute(self, context: Context) -> None:
        if self.hook is None:
            self.hook = RdsHook(secrets_manager_id=self.secret)

        # for i in range(len(self.data)):
        #     if isinstance(self.data[i], list):
        #         self.data[i] = tuple(self.data[i])

        sql = f"""
            INSERT INTO {self.schema}.{self.table} {f" ({','.join(self.columns)}) " if self.columns is not None else ''} VALUES %s;
        """

        # result = self.hook.execute_values(sql, self.data, self.subquery)
        # return result
        print(self.data)

class PandasSqlOperator(BaseOperator):
    template_fields: Sequence[str] = ("sql",)
    ui_color = '#64f55f'

    def __init__(self,
                 sql: str,
                 secret: str or None = None,
                 hook: RdsHook or None = None,
                 parameter: tuple or None = None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.secret = secret
        self.hook = hook
        self.parameter = parameter

    def execute(self, context):
        if self.hook is None:
            self.hook = RdsHook(secrets_manager_id=self.secret)
        if self.sql.split(' ')[0].lower() == 'select':
            records = self.hook.get_pandas_df(self.sql, self.parameter)
            return json.dumps(records.to_dict(orient='records'), cls=DateTimeEncoder)
        else:
            records = self.hook.execute(self.sql, self.parameter)
            return str(records)