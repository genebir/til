from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from pendulum import timezone
from dependencies.utils.data_catalog_update import data_catalog_update
from dependencies.utils.rds import RdsConnection
import pandas as pd
from airflow.models.variable import Variable

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1, tzinfo=timezone('Asia/Seoul')),
}

secManageId = Variable.get(key='opt_secret')
opt_db = Variable.get(key='opt_db')

def update_data_catalog(schema_list: list=[], **kwargs) -> None:
    if len(schema_list) == 0:
        raise Exception('>>>>>>>>>>>>>>>>>>>>> schema_list is empty <<<<<<<<<<<<<<<<<<<<<<')

    #운영계
    conn = RdsConnection(secret=secManageId,
                         db_name=opt_db
                         # local=True,
                         )

    query = f"""
        SELECT
            'cb-prd-opt-db' AS GLUE_DB_NAME,                    -- GLUE DATA CATALOG DB명
            current_database() AS RDS_DB_NAME,                  -- RDS DB명
            PS.SCHEMANAME AS SCHEMA_NAME,                       -- RDS 내 SCHEMA 명
            PS.RELNAME AS TABLE_NAME,                           -- RDS 내 TABLE 명
            CASE
                WHEN obj_description(PD.OBJOID) IS NOT NULL THEN obj_description(PD.OBJOID)
                ELSE '' END AS TABLE_COMMENT,                   -- RDS 내 TABLE 설명
            PA.ATTNAME AS COLUMN_NAME,                          -- RDS 내 COLUMN 명
            CASE
                WHEN PD.DESCRIPTION IS NOT NULL THEN PD.DESCRIPTION
                ELSE '' END AS COLUMN_COMMENT                    -- RDS 내 COLUMN 설명
        FROM
            (SELECT * FROM PG_STAT_ALL_TABLES WHERE SCHEMANAME IN ({",".join(["'" + schema + "'" for schema in schema_list])})) PS
            LEFT OUTER JOIN (SELECT * FROM PG_DESCRIPTION WHERE OBJSUBID <> 0) PD
            ON PS.RELID=PD.OBJOID
            LEFT OUTER JOIN PG_ATTRIBUTE PA
            ON PD.OBJOID=PA.ATTRELID
            AND PD.OBJSUBID=PA.ATTNUM
    """

    df = pd.read_sql_query(query, conn.engine())
    df.columns = [x.upper() for x in df.columns]

    df_ = {
        'DB_NAME': [],
        'TABLE_NAME': [],
        'TABLE_COMMENT': [],
        'COLUMN_NAME': [],
        'COLUMN_COMMENT': []
    }

    for i in range(len(df)):
        df_['DB_NAME'].append(df['GLUE_DB_NAME'][i])                    # GLUE DATA CATALOG DB명
        df_['TABLE_NAME'].append('_'.join([df['RDS_DB_NAME'][i], df['SCHEMA_NAME'][i], df['TABLE_NAME'][i]])) # RDS DB명 + SCHEMA 명 + TABLE 명 GLUE DATA CATALOG TABLE 명
        df_['TABLE_COMMENT'].append(df['TABLE_COMMENT'][i])             # GLUE DATA CATALOG TABLE 설명
        df_['COLUMN_NAME'].append(df['COLUMN_NAME'][i])                 # GLUE DATA CATALOG COLUMN 명
        df_['COLUMN_COMMENT'].append(df['COLUMN_COMMENT'][i])           # GLUE DATA CATALOG COLUMN 설명

    df = pd.DataFrame(df_)

    data_catalog_update(df)

with DAG(
    default_args=default_args,
    dag_id='AFEC_TCB_CO_OPTS_META_CRAWLER_D01_DAG',
    description='AWS(RDS)_운영계DB메타정보크롤링스케줄',
    schedule='0 16 * * *',
    tags=['glue', 'crawler', 'daily'],
    catchup=False,
) as dag:
    start = EmptyOperator(task_id='START')
    end = EmptyOperator(task_id='END')
    crawler = GlueCrawlerOperator(
        task_id='T.GCEC_OPTS_CTLOG_crawler',
        config={
            "Name": "GCEC_OPTS_CTLOG_crawler",
        })

    update_data_catalog_operator = PythonOperator(
        task_id='T.UPDATE_RDS_DATA_CATALOG',
        python_callable=update_data_catalog,
        op_kwargs={'schema_list': ['tcb_cs_db']},
    )

    start >> crawler >> update_data_catalog_operator >> end