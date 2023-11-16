from airflow.decorators import dag, task
from datetime import datetime, timedelta
from pendulum import timezone
import boto3 as bt
from dependencies.utils.rds import RdsConnection
from dependencies.operators.rds import PandasSqlOperator
import pandas as pd
import json

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1, tzinfo=timezone('Asia/Seoul')),
}


@dag(dag_id='AFEC_TCB_CO_TRTS_META_UPLOAD_D01_DAG',
     schedule_interval='0 16 * * *',
     catchup=False,
     default_args=default_args,
     tags=['glue', 'crawler', 'daily'])
def AFEC_TCB_CO_TRTS_META_UPLOAD_D01_DAG():
    SCHEMA_LIST = ['tcb_co_db', 'tcb_di_db']
    glue = bt.client('glue')

    @task(task_id='START')
    def start():
        """
        시작
        """
        pass

    @task(task_id='T.GET_GLUE_TABLE_LIST')
    def get_glue_table_list(**context):
        """
        GLUE TABLE LIST 조회

        :param context:
        :return: GLUE TABLE LIST (List)
        """
        table_list = [glue.get_tables(DatabaseName=f'postgres_{schema}')['TableList'] for schema in SCHEMA_LIST]
        print(table_list)
        return table_list

    @task(task_id='T.DELETE_GLUE_TABLE_LIST')
    def delete_glue_tables(**context):
        """
        GLUE TABLE LIST 삭제

        :param context:
        """
        for table_list in context['ti'].xcom_pull(task_ids='T.GET_GLUE_TABLE_LIST'):
            for table in table_list:
                glue.delete_table(DatabaseName=table['DatabaseName'], Name=table['Name'])

    @task(task_id='T.GET_RDS_TABLE_LIST')
    def get_rds_table_list(**context):
        """
        RDS TABLE LIST 조회

        :param context:
        :return: RDS TABLE LIST (List)
        """
        # RDS TABLE, COLUMN LIST 조회 쿼리
        sql_query = f"""
                    select tables.schema_name                                                                  as schema_name, -- 스키마 이름
                           tables.table_name                                                                   as table_name,  -- 테이블 이름
                           (select description from pg_description where objsubid = 0 and objoid = tables.oid) as table_desc,  -- 테이블 설명
                           pa.attname                                                                          as column_name, -- 컬럼 이름
                           pd.description                                                                      as column_desc, -- 컬럼 설명
                           isc.data_type                                                                       as data_type,    -- 컬럼 데이터 타입
                            case when isc.data_type = 'numeric' then concat(isc.numeric_precision, ',', isc.numeric_scale)
                                 when isc.data_type = 'character varying' then isc.character_maximum_length::varchar
                                 else '_' end                                                                  as data_length  -- 컬럼 데이터 길이
                    from (select relid      as oid,
                                 schemaname as schema_name,
                                 relname    as table_name
                          from pg_stat_all_tables
                          where schemaname in ({','.join([f"'{schema}'" for schema in SCHEMA_LIST])})
                            and relname not like '%%_p_m%%') tables,
                         pg_description pd,
                         pg_attribute pa,
                         information_schema.columns isc
                    where tables.oid = pd.objoid
                      and tables.oid = pa.attrelid
                      and pd.objsubid = pa.attnum
                      and isc.table_schema = tables.schema_name
                      and isc.table_name = tables.table_name
                      and isc.column_name = pa.attname;
                    """
        # RDS 컬럼 데이터 타입 매핑
        data_type_dict = {
            'numeric': 'decimal',
            'character varying': 'string',
            'timestamp without time zone': 'timestamp',
            'timestamp with time zone': 'timestamp',
            'character': 'string',
            'integer': 'int',
            'interval': 'string',
            'bigint': 'bigint',
            'text': 'string',
            'date': 'string'
        }
        # Connection객체 생성
        rds_conn = RdsConnection(secret='dev/proc/dtcbtrtdb/tcbmwaaid', db_name='dtcbtrtdb')
        # Sqlalchemy Engine 생성
        engine = rds_conn.engine()
        # RDS TABLE, COLUMN LIST 조회 후 데이터프레임으로 변환
        df = pd.read_sql_query(sql=sql_query, con=engine)
        # Schema, Table, Table 설명 중복 제거 후 Dict로 변환
        temp_table_info = df[['schema_name', 'table_name', 'table_desc']].drop_duplicates().to_dict('records')
        # table_info >> 테이블 정보들을 담을 리스트
        table_info = []
        # 스키마, 테이블 정보에 맞춰 컬럼 정보를 묶어 table_info에 담음
        for _ in temp_table_info:
            _df = df.loc[(df.schema_name == _['schema_name']) & (df.table_name == _['table_name']), ['column_name',
                                                                                                     'column_desc',
                                                                                                     'data_type',
                                                                                                     'data_length']]
            _df['data_type'] = _df.apply(lambda x: data_type_dict[x['data_type']] if x[
                                                                                         'data_type'] != 'numeric' else f"{data_type_dict[x['data_type']]}({x['data_length']})",
                                         axis=1)
            _df.columns = ['Name', 'Comment', 'Type', 'Length']
            _df.drop(columns=['Length'], inplace=True)
            _['columns'] = _df.to_dict('records')
            table_info.append(_)
        rds_conn.connection.close()
        engine.dispose()
        return table_info

    @task(task_id='T.CREATE_GLUE_DATA_CATALOG_TABLE')
    def create_glue_data_catalog_table(**context):
        for _ in context['ti'].xcom_pull(task_ids='T.GET_RDS_TABLE_LIST'):
            glue.create_table(
                DatabaseName=f'postgres_{_["schema_name"]}',
                TableInput={
                    'Name': f'dtcbtrtdb_{_["schema_name"]}_{_["table_name"]}',
                    'Description': f'{_["table_desc"]}',
                    'StorageDescriptor': {
                        'Columns': _['columns'],
                        'Location': f'dtcbtrtdb.{_["schema_name"]}.{_["table_name"]}',
                    },
                    'Parameters': {
                        'classification': 'postgresql',
                        'connectionName': 'dtcbtrtdb',
                        'typeOfData': 'table',
                        'compressionType': 'none',
                    }
                }
            )

        glue.close()
        pass

    @task(task_id='END')
    def end():
        pass

    start() >> get_glue_table_list() >> delete_glue_tables() >> get_rds_table_list() >> create_glue_data_catalog_table() >> end()


AFEC_TCB_CO_TRTS_META_UPLOAD_D01_DAG()