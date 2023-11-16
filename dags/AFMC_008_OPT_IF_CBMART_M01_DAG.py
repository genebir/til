import boto3
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from dependencies.operators.rds import ProcedureOperator
from dependencies.hooks.rds import RdsHook
import pandas as pd
import pendulum
from airflow.models.variable import Variable
from dependencies.utils.rds import RdsConnection
from airflow.operators.python import PythonOperator
from dateutil.relativedelta import *
# from dependencies.utils.data_catalog_update import data_catalog_update
# from dependencies.utils.rds import RdsConnection

default_args = {
    'owner': 'airflow',
    # UTC 기준 시간으로 DAG가 Schedule되기 때문에 한국 시간으로 변경
    'start_date': datetime(2023, 1, 1, tzinfo=pendulum.timezone('Asia/Seoul')),
}

# Airflow에서 버킷명 전달위해 추가
bucketNm       = Variable.get(key='s3_bucket_nm')
metaBucket     = Variable.get(key='s3_meta_bucket')
optDbNm        = Variable.get(key='opt_db')
secManageId    = Variable.get(key='sec_manager_id')     # trt DB연결
optSecManageId = Variable.get(key='opt_sec_manager_id') # opt DB연결

colecPathItgCd = '10001'
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

    updateQuery1 = f"update tcb_co_db.co_im_wrk_id_bas " \
                   f"set batch_trt_yn = 'Y' " \
                   f"where im_wrk_id = '{ciJobId}' " \
                   f"and batch_trt_step_itg_cd = '{trtStepCd}'"

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

def jobIdNext(ciJobId : str):
    insertQuery = f"insert into tcb_co_db.co_if_wrk_id_bas " \
                  f"select '{ciJobId}', current_timestamp, '{base_ym}', '운영계 I/F테이블 적재', 'N' "

    print("★★★★★★★★★★★★★★★★★★★★")
    print(insertQuery)
    print("★★★★★★★★★★★★★★★★★★★★")

    with RdsConnection(secrets_manager_id=optSecManageId,
                       db_name=optDbNm
                       ) as rds_conn:
        cursor = rds_conn.connection.cursor()
        cursor.execute(insertQuery)
        rds_conn.connection.commit()

        cursor.close()
        rds_conn.connection.close()

########################################################################################################################
# csv파일에서 읽어온 테이블, 컬럼, 스키마, 프로그램명 정보
df = pd.read_csv(get_table_info(bucket=metaBucket,key=f'jobs/META-INF/meta_dt_opt_dm_table_column_list.csv'))
# CSV 파일에서 특정 항목의 값이 공백으로 들어갈 경우 오류 발생하므로 '_'나 다른 값으로 대처하여 작성 필요
# CSV 파일이 없을 경우, Airflow UI의 오류로 인해 DAGs 목록에 뜨지 않음

# 작업 기준년월 정보
# AWS 운영계 반영 시 주석 해제
rds_hook = RdsHook(secrets_manager_id=optSecManageId,db_name=optDbNm)
# AWS 개발계 반영 시 주석 해제
# rds_hook = RdsHook(db_name=optDbNm)
base_ym = '202106'
# now = datetime.now()+relativedelta(months=-1)
# base_ym = now.strftime('%Y%m')

with DAG(
        dag_id=f'AFMC_008_OPT_IF_CBMART_M01_DAG',
        description='운영계DB_배치데이터적재_스케줄링',
        default_args=default_args,
        tags=['glue', 's3', 'rds', 'monthly', 'batch'],
        # schedule='@once',
        schedule_interval='@once',
        # schedule='50 16 * * *',
        # schedule_interval='25 10 * * *',
        catchup=False,
        max_active_tasks=6,
        concurrency=24
) as dag:

    sensor = ExternalTaskSensor(
        task_id = 'W.AFMC_007_TCB_DT_CBMART_M01_DAG',
        external_dag_id = 'AFMC_TCB_DT_CBMART_M01_DAG',
        external_task_id = 'END',
        start_date= datetime(2023, 1, 1),
        execution_date_fn=lambda x: x - timedelta(minutes=5),
        mode='reschedule',
        timeout=3600
    )

    job_id = selectJobId('20010')

    with TaskGroup(f'Opt_Table_1', dag=dag) as glue_job_1:
        # 테이블 정보를 통해 GlueJobOperator를 호출
        for i in df.index[0:12]:
            # GlueJobOperator를 통해 Glue job을 호출
            # Glue job의 script_args에는 Glue job의 인자값을 넣어줌
            job_name = f"{df['JOB'][i]}"

            GlueJobOperator(task_id=f'T.{job_name}',
                            job_name=f'{job_name}',
                            script_args={
                                '--DB_NAME': df['DB_NAME'][i],
                                '--SCHEMA_NAME': df['SCHEMA_NAME'][i],
                                '--TABLE_NAME': df['TABLE_NAME'][i],
                                '--BASE_YM': base_ym,
                                '--JOB': df['JOB'][i],
                                '--COLEC_PATH': '10001',
                                '--S3_BUCKET_NM': bucketNm,
                                '--JOBID' : job_id,
                                '--SEC_MANAGER_ID' : optSecManageId
                            },
                            verbose=False,  # Glue 로그 출력 여부
                            )

    with TaskGroup(f'Opt_Table_2', dag=dag) as glue_job_2:
        # 테이블 정보를 통해 GlueJobOperator를 호출
        for i in df.index[12:]:
            # GlueJobOperator를 통해 Glue job을 호출
            # Glue job의 script_args에는 Glue job의 인자값을 넣어줌
            job_name = f"{df['JOB'][i]}"

            GlueJobOperator(task_id=f'T.{job_name}',
                            job_name=f'{job_name}',
                            script_args={
                                '--DB_NAME': df['DB_NAME'][i],
                                '--SCHEMA_NAME': df['SCHEMA_NAME'][i],
                                '--TABLE_NAME': df['TABLE_NAME'][i],
                                '--BASE_YM': base_ym,
                                '--JOB': df['JOB'][i],
                                '--COLEC_PATH': '10001',
                                '--S3_BUCKET_NM': bucketNm,
                                '--JOBID' : job_id,
                                '--SEC_MANAGER_ID' : optSecManageId
                            },
                            verbose=False,  # Glue 로그 출력 여부
                            )
    procedure_custIdfy = ProcedureOperator(
                            hook=rds_hook,
                            task_id=f'T.PEC_CS_DM_CUST_IDFY_BAS_1',
                            schema='tcb_cs_db',
                            procedure_name='pec_cs_dm_cust_idfy_bas_1',
                            parameter=base_ym,
                    )

    with TaskGroup(group_id='PROCEDURE_GROUP1') as procedure_cbgroup1:

        procedures = [
            {   #운영계_고객기본적재
                'schema':  'tcb_cs_db',
                'procedure_name': 'pec_cs_dm_cust_bas_1',
                'parameter': base_ym,
            },
            {    #운영계_고객이력적재
                'schema': 'tcb_cs_db',
                'procedure_name': 'pec_cs_dm_cust_hst_1',
                'parameter': base_ym,
            },
            {   #운영계_연체이력
                'schema':  'tcb_cs_db',
                'procedure_name': 'pec_cs_dm_dlinq_hst_1',
                'parameter': base_ym,
            },
            {   #운영계_연체년월이력
                'schema':  'tcb_cs_db',
                'procedure_name': 'pec_cs_dm_dlinq_ym_hst_1',
                'parameter': base_ym,
            },
            {   #운영계_할부계약이력
                'schema':  'tcb_cs_db',
                'procedure_name': 'pec_cs_dm_insl_cont_hst_1',
                'parameter': base_ym,
            },
            {   #운영계_할부계약내역
                'schema':  'tcb_cs_db',
                'procedure_name': 'pec_cs_dm_insl_cont_txn_1',
                'parameter': base_ym,
            },
            {  # 운영계(통합)_소액결제이력
                'schema': 'tcb_cs_db',
                'procedure_name': 'pec_cs_dm_smstl_hst_1',
                'parameter': base_ym,
            },
            {  # 운영계(통합)_서비스계약이력
                'schema': 'tcb_cs_db',
                'procedure_name': 'pec_cs_dm_svc_cont_hst_1',
                'parameter': base_ym,
            },
            {  # 운영계(통합)_서비스계약내역
                'schema': 'tcb_cs_db',
                'procedure_name': 'pec_cs_dm_svc_cont_txn_1',
                'parameter': base_ym,
            },
        ]
        for procedure in procedures:
            op = ProcedureOperator(
                hook=rds_hook,
                task_id=f'T.{procedure["procedure_name"].upper()}',
                schema=procedure['schema'],
                procedure_name=procedure['procedure_name'],
                parameter=procedure['parameter'],
            )

    procedure_feature = ProcedureOperator(
                            hook=rds_hook,
                            task_id=f'T.PEC_CS_FT_TPS_SVC_TGT_ITEM_TXN_1',
                            schema='tcb_cs_db',
                            procedure_name='pec_cs_ft_tps_svc_tgt_item_txn_1',
                            parameter=base_ym,
                    )

    with TaskGroup(group_id='PROCEDURE_GROUP2') as procedure_cbgroup2:

        procedures = [
            {   #운영계_고객기본적재
                'schema':  'tcb_cs_db',
                'procedure_name': 'pec_cs_sc_score_cmp_mstr_txn_1',
                'parameter': base_ym,
            },
            {    #운영계_고객이력적재
                'schema': 'tcb_cs_db',
                'procedure_name': 'pec_cs_sc_score_cmp_ptcl_item_txn_1',
                'parameter': base_ym,
            },
        ]
        for procedure in procedures:
            op = ProcedureOperator(
                hook=rds_hook,
                task_id=f'T.{procedure["procedure_name"].upper()}',
                schema=procedure['schema'],
                procedure_name=procedure['procedure_name'],
                parameter=procedure['parameter'],
            )

    # procedure_score = ProcedureOperator(
    #                         hook=rds_hook,
    #                         task_id=f'T.PEC_CS_SC_CRDT_EVL_TXN_1',
    #                         schema='tcb_cs_db',
    #                         procedure_name='pec_cs_sc_crdt_evl_txn_1',
    #                         parameter=base_ym,
    #                 )

    jobId_End = PythonOperator(
        task_id=f'T.Job_End',
        python_callable=jobEnd,
        op_kwargs={'ciJobId': job_id, 'trtStepCd': '20010', 'trtResltCd': '20002', 'trtStepNm': '운영계I/F적재', 'trtResltNm': '작업종료'},
    )

    jobId_Opt = PythonOperator(
        task_id=f'T.optJob_Start',
        python_callable=jobIdNext,
        op_kwargs={'ciJobId': job_id},
    )

    start_dag = EmptyOperator(task_id='START', dag=dag)
    end_dag = EmptyOperator(task_id='END', dag=dag)

    start_dag >> sensor >> glue_job_1 >> glue_job_2 >> jobId_Opt >> jobId_End >> procedure_custIdfy >> procedure_cbgroup1 >> procedure_feature >> procedure_cbgroup2 >> end_dag
    # start_dag >> sensor >> glue_job_2 >> jobId_End >> jobId_Opt >> procedure_custIdfy >> procedure_cbgroup1 >> procedure_feature >> procedure_cbgroup2 >> end_dag

