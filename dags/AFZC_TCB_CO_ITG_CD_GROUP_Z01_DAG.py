from airflow import DAG
from airflow.operators.empty import EmptyOperator
from pendulum import timezone
from datetime import datetime
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.models.variable import Variable
from dependencies.operators.rds import ProcedureOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1, tzinfo=timezone('Asia/Seoul')),
}

s3_glue_bucket    = Variable.get(key='s3_meta_bucket')  # cb-dev-glue-s3, cb-prd-glue-s3
base_date         = datetime.now().astimezone(timezone("Asia/Seoul")).strftime('%Y%m%d')
colec_path_itg_cd = '10004'  # 정기배치: 10001, 온라인 : 10002, 온디맨드 : 10003, 비정기 : 10004

with DAG(
    default_args=default_args,
    dag_id='AFZC_TCB_CO_ITG_CD_GROUP_Z01_DAG',
    description='처리계(공통)_표준코드매핑정보적재스케줄링',
    schedule=None,
    tags=['procedure', 'glue', 'code' ],
    catchup=False,
) as dag:
    start = EmptyOperator(task_id='START')
    end = EmptyOperator(task_id='END')

    co_itg_cd_group_src_rel_sensor = S3KeySensor(
        task_id='S.CO_ITG_CD_GROUP_SRC_REL',
        bucket_key=f's3://{s3_glue_bucket}/code-meta/co_itg_cd_group_src_rel/CO_ITG_CD_GROUP_SRC_REL.csv',
        wildcard_match=True,
    )

    co_itg_cd_src_rel_sensor = S3KeySensor(
        task_id='S.CO_ITG_CD_SRC_REL',
        bucket_key=f's3://{s3_glue_bucket}/code-meta/co_itg_cd_src_rel/CO_ITG_CD_SRC_REL.csv',
        wildcard_match=True ,
    )

    gjzc_dl_itg_cd_group_src_rel_1 = GlueJobOperator(
        task_id='T.GJZC_DL_ITG_CD_GROUP_SRC_REL_1',
        job_name='GJZC_DL_ITG_CD_GROUP_SRC_REL_1',
        script_args={
            '--BASE_DATE': base_date,
            '--COLEC_PATH_ITG_CD': colec_path_itg_cd,
            '--S3_BUCKET_NM' : s3_glue_bucket
        },
        dag=dag
    )

    gjzc_dl_itg_cd_src_rel_1 = GlueJobOperator(
        task_id='T.GJZC_DL_ITG_CD_SRC_REL_1',
        job_name='GJZC_DL_ITG_CD_SRC_REL_1',
        script_args={
            '--BASE_DATE': base_date,
            '--COLEC_PATH_ITG_CD': colec_path_itg_cd,
            '--S3_BUCKET_NM': s3_glue_bucket
        },
        verbose=False,  # Glue 로그 출력 여부
        dag=dag
    )

    glue_crawler = GlueCrawlerOperator(
        task_id='T.GCZC_CODE_META_CRAWLER',
        config={
            'Name': 'GCZC_CODE_META_CRAWLER'
        },
    )

    pzc_di_itg_cd_group_bas_1 = ProcedureOperator(
        task_id='T.PZC_DI_ITG_CD_GROUP_BAS_1',
        schema='tcb_dm_db',
        procedure_name='pzc_di_itg_cd_group_bas_1',
        parameter=(base_date, colec_path_itg_cd)
    )

    pzc_di_itg_cd_bas_1 = ProcedureOperator(
        task_id='T.PZC_DI_ITG_CD_BAS_1',
        schema='tcb_dm_db',
        procedure_name='pzc_di_itg_cd_bas_1',
        parameter=(base_date, colec_path_itg_cd)
    )

    start >> [co_itg_cd_group_src_rel_sensor, co_itg_cd_src_rel_sensor] \
        >> EmptyOperator(task_id='JOIN') \
        >> [gjzc_dl_itg_cd_group_src_rel_1, gjzc_dl_itg_cd_src_rel_1] \
        >> glue_crawler \
        >> [pzc_di_itg_cd_group_bas_1, pzc_di_itg_cd_bas_1] \
        >> end