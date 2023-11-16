import psycopg2
from airflow.sensors.base import BaseSensorOperator

from dependencies.hooks.rds import RdsHook


class RdsSensor(BaseSensorOperator):
    ui_color = '#c40038'

    def __init__(self,
                 sql: str,
                 hook: RdsHook or None = None,
                 *args,
                 **kwargs):
        self.sql = sql,
        self.hook = hook

        if self.hook is None:
            self.hook = RdsHook()

        self.cursor = self.hook.conn.cursor()

        super().__init__(*args, **kwargs)

    def poke(self, context):
        self.log.info(f"RDS Sensor Start")
        self.log.info(f"SQL: {self.sql}")

        self.cursor.execute(self.sql[0])
        result = self.cursor.fetchone()

        if result is None:
            return False
        else:
            return True


    def __del__(self):
        if self.cursor.closed is False:
            self.cursor.close()
        self.log.info(f"RDS Sensor End")


