import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
import pandas as pd
import pendulum
from datetime import datetime
from dependencies.utils.data_catalog_update import data_catalog_update
from dependencies.utils.rds import RdsConnection
from dateutil.relativedelta import *
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models.variable import Variable

default_args = {
    'owner': 'airflow',
    # UTC 기준 시간으로 DAG가 Schedule되기 때문에 한국 시간으로 변경
    'start_date': datetime(2023, 1, 1, tzinfo=pendulum.timezone('Asia/Seoul'))
}

# 작업 기준년월 정보
base_ym        = '202106'
colecPathItgCd = '10003'

# Airflow에서 버킷명 전달위해 추가
bucketNm    = Variable.get(key='s3_bucket_nm')   # 마트데이터를 저장할 버킷
metaBucket  = Variable.get(key='s3_meta_bucket') # 메타파일(csv)이 저장되어 있는 버킷
secManageId = Variable.get(key='sec_manager_id') # DB연결

########################################################################################################################
# S3에서 테이블 정보를 가져오는 함수
# bucket: s3 버킷 이름
# key: s3 버킷 내 파일 경로
# return: s3 버킷 내 파일 정보
def get_table_info(**kwargs):
    s3 = boto3.client('s3')
    response = s3.get_object(
        Bucket=kwargs['bucket'],
        Key=kwargs['key']
    )
    return response['Body']

def update_data_catalog(**kwargs) -> None:
    """
    S3에 저장된 테이블 정보를 통해 데이터 카탈로그를 업데이트하는 함수
    :param company: task_id에 들어갈 이름 ex) kt, skt, lg
    :return: None
    """
    s3_client = boto3.client('s3',region_name='ap-northeast-2')

    meta_info_file = s3_client.get_object(
        Bucket=metaBucket,
        Key=f'jobs/META-INF/meta_dt_data_catalog_info_list.csv'
    )

    df = pd.read_csv(meta_info_file['Body'])
    df = df.fillna('')
    print(df)

    data_catalog_update(df)

# DM DAG에서 채번한 CB마트 Job Id를 Select
def selectJobId(batchStepCd : str):
    findQuery = f"select im_wrk_id " \
                f"from (" \
                f"select row_number() over(partition by comm_bizr_itg_cd order by reg_dt desc) as rn, im_wrk_id " \
                f"from tcb_co_db.co_im_wrk_id_bas " \
                f"where colec_path_itg_cd = '{colecPathItgCd}' " \
                f"and batch_trt_step_itg_cd = '{batchStepCd}'" \
                f"and batch_trt_yn = 'N' " \
                f") t101 " \
                f"where rn = 1"

    print("★★★★★★★★★★★★★★★★★★★★")
    print(findQuery)
    print("★★★★★★★★★★★★★★★★★★★★")

    JobId = ''
    with RdsConnection(secrets_manager_id=secManageId,
                       ) as rds_conn:
        cursor = rds_conn.connection.cursor()
        cursor.execute(findQuery)
        # JobIdTemp = cursor.fetchall()[0][0]
        # JobId = str(JobIdTemp)
        JobIdTemp = cursor.fetchall()
        for i in JobIdTemp:
            JobId = i[0]

        cursor.close()
        rds_conn.connection.close()
        print("★★★★★★★★★★★★★★★★★★★★")
        print('JobId = ', JobId)
        print("★★★★★★★★★★★★★★★★★★★★")
    return JobId

# DM Job 작업종료 적재
def jobEnd(ciJobId : str, trtStepCd :str, trtResltCd :str, trtStepNm :str, trtResltNm :str):
    insertQuery = f"insert into tcb_co_db.co_im_wrk_id_bas " \
                  f"select '{ciJobId}', '_', '{trtStepCd}', '{trtResltCd}', '{base_ym}', current_timestamp, " \
                  f"to_char(current_date, 'YYYYMMDD'),  '_', '{colecPathItgCd}', {ciJobId[16:]}, '{trtStepNm}', '{trtResltNm}', 'N' "

    print("★★★★★★★★★★★★★★★★★★★★")
    print(insertQuery)
    print("★★★★★★★★★★★★★★★★★★★★")

    with RdsConnection(secrets_manager_id=secManageId,
                       ) as rds_conn:
        cursor = rds_conn.connection.cursor()
        cursor.execute(insertQuery)
        rds_conn.connection.commit()

        cursor.close()
        rds_conn.connection.close()


# opt DB 전송단계 추가 및 정보제공(S3저장) 처리여부 업데이트
def jobIdNext(ciJobId : str, trtStepCd :str, trtResltCd :str, trtStepNm :str, trtResltNm :str):
    print("★★★★★★★★★★★★★★★★★★★★")
    print(ciJobId)
    print("★★★★★★★★★★★★★★★★★★★★")

    insertQuery = f"insert into tcb_co_db.co_im_wrk_id_bas " \
                  f"select '{ciJobId}', '_', '{trtStepCd}', '{trtResltCd}', '{base_ym}', current_timestamp, " \
                  f"to_char(current_date, 'YYYYMMDD'),  '_', '{colecPathItgCd}', {ciJobId[16:]}, '{trtStepNm}', '{trtResltNm}', 'N' "

    print("★★★★★★★★★★★★★★★★★★★★")
    print(insertQuery)
    print("★★★★★★★★★★★★★★★★★★★★")

    updateQuery1 = f"update tcb_co_db.co_im_wrk_id_bas " \
                   f"set batch_trt_yn = 'Y' " \
                   f"where im_wrk_id = '{ciJobId}' " \
                   f"and batch_trt_step_itg_cd = '20009'"

    print("★★★★★★★★★★★★★★★★★★★★")
    print(updateQuery1)
    print("★★★★★★★★★★★★★★★★★★★★")

    with RdsConnection(secrets_manager_id=secManageId,
                       ) as rds_conn:
        cursor = rds_conn.connection.cursor()
        cursor.execute(insertQuery)
        rds_conn.connection.commit()

        cursor.execute(updateQuery1)
        rds_conn.connection.commit()

        cursor.close()
        rds_conn.connection.close()
########################################################################################################################
# csv파일에서 읽어온 테이블, 컬럼, 스키마, 프로그램명 정보
df = pd.read_csv(get_table_info(bucket=metaBucket,key=f'jobs/META-INF/meta_dt_trt_dm_table_column_list.csv'))

with DAG(
        dag_id=f'AFOD_007_TCB_DT_CBMART_M01_DAG',
        description='운영계DB_CB마트데이터제공(S3저장)스케줄링(온디맨드)',
        default_args=default_args,
        tags=['glue', 's3', 'rds', 'ondemand'],
        schedule='@once',
        # schedule='00 10 * * *',
        # schedule_interval='20 10 * * *',
        # schedule_interval='@once',
        catchup=False,
        max_active_tasks=6,
        concurrency=24
) as dag:

    jobId = selectJobId('20009')

    # S3에 저장된 테이블 정보를 가져옴
    crawler = GlueCrawlerOperator(task_id=f"T.GCME_MART_DM_CRAWLER",
                                  config={"Name": f"GCME_MART_DM_CRAWLER"}
                                  )

    # 스코어 마트데이터 적재 완료 후 본 작업 수행
    sensor = ExternalTaskSensor(
        task_id = 'W.AFOD_TCB_DM_SCOREMART_M01_DAG',
        external_dag_id = 'AFOD_TCB_DM_SCOREMART_M01_DAG',
        external_task_id = 'END',
        start_date= datetime(2023, 1, 1),
        execution_date_fn=lambda x: x,
        mode='reschedule',
        timeout=3600
    )

    with TaskGroup(f'CBMART_TBL_1', dag=dag) as glue_job_1:
        # 테이블 정보를 통해 GlueJobOperator를 호출
        # for i in range(len(df[0:5])):
        for i in df.index[0:12]:
            # GlueJobOperator를 통해 Glue job을 호출
            # Glue job의 script_args에는 Glue job의 인자값을 넣어줌
            # job_name = f'{job}'
            # job_name = f'Table_{df["TABLE_NAME"][i]}'
            job_name = f"{df['JOB'][i]}"

            GlueJobOperator(task_id=f'T.{job_name}',
                            job_name=f'{job_name}',
                            script_args={
                                '--DB_NAME': df['DB_NAME'][i],
                                '--SCHEMA_NAME': df['SCHEMA_NAME'][i],
                                '--TABLE_NAME': df['TABLE_NAME'][i],
                                '--BASE_YM': base_ym,
                                '--JOB': df['JOB'][i],
                                '--COLEC_PATH': colecPathItgCd,
                                '--S3_BUCKET_NM': bucketNm,
                                '--WRK_ID':jobId
                            },
                            verbose=False,  # Glue 로그 출력 여부
                            )

    with TaskGroup(f'CBMART_TBL_2', dag=dag) as glue_job_2:
        # 테이블 정보를 통해 GlueJobOperator를 호출
        # for i in range(len(df[5:])):
        for i in df.index[12:]:
            # GlueJobOperator를 통해 Glue job을 호출
            # Glue job의 script_args에는 Glue job의 인자값을 넣어줌
            # job_name = f'{job}'
            # job_name = f'Table_{df["TABLE_NAME"][i]}'
            job_name = f"{df['JOB'][i]}"

            GlueJobOperator(task_id=f'T.{job_name}',
                            job_name=f'{job_name}',
                            script_args={
                                '--DB_NAME': df['DB_NAME'][i],
                                '--SCHEMA_NAME': df['SCHEMA_NAME'][i],
                                '--TABLE_NAME': df['TABLE_NAME'][i],
                                '--BASE_YM': base_ym,
                                '--JOB': df['JOB'][i],
                                '--COLEC_PATH': colecPathItgCd,
                                '--S3_BUCKET_NM': bucketNm,
                                '--WRK_ID':jobId
                            },
                            verbose=False,  # Glue 로그 출력 여부
                            )

    update_data_catalog_operator = PythonOperator(
        task_id='T.UPDATE_RDS_DATA_CATALOG',
        python_callable=update_data_catalog,
        op_kwargs={'schema_list': ['tcb_dm_db']},
    )

    jobId_End = PythonOperator(
        task_id=f'T.Job_End',
        python_callable=jobEnd,
        op_kwargs={'ciJobId': jobId, 'trtStepCd': '20009', 'trtResltCd': '20002', 'trtStepNm': '정보제공(S3저장)', 'trtResltNm': '작업종료'},
    )

    jobId_Next = PythonOperator(
        task_id=f'T.optInsertJob_Start',
        python_callable=jobIdNext,
        op_kwargs={'ciJobId': jobId, 'trtStepCd': '20010', 'trtResltCd': '20001', 'trtStepNm': '운영계I/F적재', 'trtResltNm': '작업시작'},
        dag=dag
    )

    start_dag = EmptyOperator(task_id='START', dag=dag)
    end_dag = EmptyOperator(task_id='END', dag=dag)

    start_dag >> sensor >> glue_job_1 >> glue_job_2 >> crawler >> update_data_catalog_operator >> jobId_End >> jobId_Next >> end_dag