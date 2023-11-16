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
from airflow.operators.python import BranchPythonOperator

default_args = {
    'owner': 'airflow',
    # UTC 기준 시간으로 DAG가 Schedule되기 때문에 한국 시간으로 변경
    'start_date': datetime(2023, 1, 1, tzinfo=pendulum.timezone('Asia/Seoul'))
}

now   = datetime.now() + relativedelta(hours=8)
year  = str(now.year)
month = '0'+str(now.month) if now.month in (1, 2, 3, 4, 5, 6, 7, 8, 9) else str(now.month)
day   = '0'+str(now.day) if len(str(now.day)) == 1 else str(now.day)
hour  = '0'+str(now.hour) if now.hour in (0, 1, 2, 3, 4, 5, 6, 7, 8, 9) else str(now.hour)

# reg_dt_bef = year+month+day+hour+"00"
# reg_dt_aft = year+month+day+hour+"59"

reg_dt_bef = '202310030100'
reg_dt_aft = '202310030159'

# 작업 기준년월 정보
base_ym           = '202106'
colec_path_itg_cd = '10002' # 온라인

# Airflow에서 버킷명 전달위해 추가
bucketNm    = Variable.get(key='s3_bucket_nm')   # 마트데이터를 저장할 버킷
metaBucket  = Variable.get(key='s3_meta_bucket') # 메타파일(csv)이 저장되어 있는 버킷
secManageId = Variable.get(key='sec_manager_id') # DB연결

########################################################################################################################
# 온라인데이터 적재 유무 확인
def selectOnline():
    count = 0
    print('#####################################')
    print('reg_dt_bef : ', reg_dt_bef)
    print('reg_dt_aft : ', reg_dt_aft)
    print('#####################################')
    selectQuery = f"select count(*) as cnt from tcb_dc_db.dc_ol_input_txn " \
                  f"where to_char(reg_dt, 'YYYYMMDDHH24MI') between '{reg_dt_bef}' and '{reg_dt_aft}'" \
                  f"and svc_rqt_id = 'fe5b2f9d-5720-4aea-9791-af7ae8c13337'"

    with RdsConnection(secrets_manager_id=secManageId,
                       ) as rds_conn:
        cursor = rds_conn.connection.cursor()
        cursor.execute(selectQuery)
        rds_conn.connection.commit()
        countTmp = cursor.fetchall()

        for i in countTmp[0]:
            count = i

        cursor.close()
        rds_conn.connection.close()

    if count > 0:
        return 'data'
    else:
        return 'no_data'

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
                f"where colec_path_itg_cd = '{colec_path_itg_cd}' " \
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
                  f"to_char(current_date, 'YYYYMMDD'),  '_', '{colec_path_itg_cd}', {ciJobId[16:]}, '{trtStepNm}', '{trtResltNm}', 'N' "

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
                  f"to_char(current_date, 'YYYYMMDD'),  '_', '{colec_path_itg_cd}', {ciJobId[16:]}, '{trtStepNm}', '{trtResltNm}', 'N' "

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
dfTemp = pd.read_csv(get_table_info(bucket=metaBucket,key=f'jobs/META-INF/meta_dt_trt_dm_table_column_list(online).csv'))
df = dfTemp[['DB_NAME', 'SCHEMA_NAME', 'TABLE_NAME', 'JOB']].drop_duplicates().to_dict('records')

with DAG(
        dag_id=f'AFRT_007_TCB_DT_CBMART_R01_DAG',
        description='운영계DB_CB마트데이터제공(S3저장)스케줄링(실시간)',
        default_args=default_args,
        tags=['glue', 's3', 'rds', 'online'],
        schedule='@once',
        # schedule='00 10 * * *',
        # schedule_interval='20 10 * * *',
        # schedule_interval='@once',
        catchup=False,
        max_active_tasks=6,
        concurrency=24
) as dag:

    jobId = selectJobId('20009')

    start   = EmptyOperator(task_id='START')
    end     = EmptyOperator(task_id='END', trigger_rule='none_failed', dag=dag)
    # data    = EmptyOperator(task_id='data',dag=dag,)
    # no_data = EmptyOperator(task_id='no_data', dag=dag, )
    #
    # onLine_Chk = BranchPythonOperator(
    #     task_id='onLine_Chk',
    #     python_callable=selectOnline,
    #     dag=dag,
    # )

    # 스코어 마트데이터 적재 완료 후 본 작업 수행
    # sensor = ExternalTaskSensor(
    #     task_id = 'W.AFRT_TCB_DM_SCOREMART_R01_DAG',
    #     external_dag_id = 'AFRT_TCB_DM_SCOREMART_R01_DAG',
    #     external_task_id = 'END',
    #     start_date= datetime(2023, 1, 1),
    #     execution_date_fn=lambda x: x,
    #     mode='reschedule',
    #     timeout=3600
    # )

    # S3에 저장된 테이블 정보를 가져옴
    crawler = GlueCrawlerOperator(task_id=f"T.GCME_MART_DM_CRAWLER",
                                  config={"Name": f"GCME_MART_DM_CRAWLER"}
                                  )

    with TaskGroup(f'CBMART_TBL_1', dag=dag) as glue_job_1:
        # 테이블 정보를 통해 GlueJobOperator를 호출
        # for i in range(len(df[0:5])):
        for i in df[0:12]:
            # GlueJobOperator를 통해 Glue job을 호출
            # Glue job의 script_args에는 Glue job의 인자값을 넣어줌
            # job_name = f'{job}'
            # job_name = f'Table_{df["TABLE_NAME"][i]}'
            job_name = f"{i['JOB']}"
            columns = ','.join(dfTemp[(dfTemp['DB_NAME'] == i['DB_NAME']) &
                                      (dfTemp['SCHEMA_NAME'] == i['SCHEMA_NAME']) &
                                      (dfTemp['TABLE_NAME'] == i['TABLE_NAME'])]['COLUMNS'].tolist()).lower()

            GlueJobOperator(task_id=f'T.{job_name}',
                            job_name=f'{job_name}',
                            script_args={
                                '--DB_NAME': i['DB_NAME'],
                                '--SCHEMA_NAME': i['SCHEMA_NAME'],
                                '--TABLE_NAME': i['TABLE_NAME'],
                                '--COLUMNS' : columns,
                                '--BASE_YM': base_ym,
                                '--JOB': job_name,
                                '--COLEC_PATH': colec_path_itg_cd,
                                '--S3_BUCKET_NM': bucketNm,
                                '--WRK_ID': jobId
                            },
                            verbose=False,  # Glue 로그 출력 여부
                            )

    with TaskGroup(f'CBMART_TBL_2', dag=dag) as glue_job_2:
        # 테이블 정보를 통해 GlueJobOperator를 호출
        # for i in range(len(df[5:])):
        for i in df[12:]:
            # GlueJobOperator를 통해 Glue job을 호출
            # Glue job의 script_args에는 Glue job의 인자값을 넣어줌
            # job_name = f'{job}'
            # job_name = f'Table_{df["TABLE_NAME"][i]}'
            job_name = f"{i['JOB']}"
            columns = ','.join(dfTemp[(dfTemp['DB_NAME'] == i['DB_NAME']) &
                                      (dfTemp['SCHEMA_NAME'] == i['SCHEMA_NAME']) &
                                      (dfTemp['TABLE_NAME'] == i['TABLE_NAME'])]['COLUMNS'].tolist()).lower()
            GlueJobOperator(task_id=f'T.{job_name}',
                            job_name=f'{job_name}',
                            script_args={
                                '--DB_NAME': i['DB_NAME'],
                                '--SCHEMA_NAME': i['SCHEMA_NAME'],
                                '--TABLE_NAME': i['TABLE_NAME'],
                                '--COLUMNS' : columns,
                                '--BASE_YM': base_ym,
                                '--JOB': job_name,
                                '--COLEC_PATH': colec_path_itg_cd,
                                '--S3_BUCKET_NM': bucketNm,
                                '--WRK_ID': jobId
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

    # start >> sensor >> onLine_Chk >> data >> glue_job_1 >> crawler >> update_data_catalog_operator >> jobId_End >> end
    # start >> sensor >> onLine_Chk >> data >> glue_job_2 >> crawler >> update_data_catalog_operator >> jobId_End >> end
    # start >> sensor >> onLine_Chk >> no_data >> end
    start >> glue_job_1 >> glue_job_2 >> crawler >> update_data_catalog_operator >> jobId_End >> jobId_Next >> end