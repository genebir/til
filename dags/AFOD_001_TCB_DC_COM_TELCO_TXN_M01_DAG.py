"""
Airflow에서 Nifi실행 및 중단
S3에서 데이터 수집 확인
S3의 파일을 통한 데이터 카탈로그 생성 및 RDS에 데이터 적재

1. Nifi 실행
2. S3에 데이터 수집
3. S3의 데이터를 크롤링하여 데이터 카탈로그 생성
4. RDS에 데이터 적재
"""
import boto3
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from airflow.models.variable import Variable
import requests
import json
import pandas as pd
import pendulum
# from dateutil.relativedelta import relativedelta
from pendulum import timezone
from dependencies.utils.date_util import get_base_date, get_base_ym
from dependencies.utils.data_catalog_update import data_catalog_update
from dependencies.hooks.rds import RdsHook
from dependencies.utils.rds import RdsConnection
from airflow.operators.python import BranchPythonOperator

default_args = {
    'owner': 'airflow',
    # UTC 기준 시간으로 DAG가 Schedule되기 때문에 한국 시간으로 변경
    'start_date': datetime(2023, 1, 1,
                           tzinfo=pendulum.timezone('Asia/Seoul')),
}

# 수집기관별 DAG 정보
# schedule: DAG가 실행되는 시간
# description: DAG에 대한 설명
COMPANIES = {
    'KT': {
        'schedule': '20 20 * * *',
        'description': '처리계DB_KT신용정보수집스케줄링',
        'comm_bizr_cd' : '10001',
        'contextID' : '9be3faab-f26d-306f-4e23-b15dee5399a3'
    },
    'SK': {
        'schedule': '20 20 * * *',
        'description': '처리계DB_SKT신용정보수집스케줄링',
        'comm_bizr_cd' : '10002',
        'contextID' : '9baf3268-8fad-1fef-b4de-13bba7fec9a1'
    },
    'LG': {
        'schedule': '20 20 * * *',
        'description': '처리계DB_LGU+신용정보수집스케줄링',
        'comm_bizr_cd' : '10003',
        'contextID' : '9baf3269-8fad-1fef-dd1b-c5fda2007cde'
    }
}

# Airflow의 전역변수 값 가져오기
NIFI_API_URL = Variable.get(key='nifi_api_key')
# nifi_url     = Variable.get(key='nifi_url')       # Nifi url
awsKeyId     = Variable.get(key='aws_api_key')
awsAccessKey = Variable.get(key='aws_password')
s3BucketNm   = Variable.get(key='s3_bucket_nm')   # 수집파일 저장한 버킷명
secManageId  = Variable.get(key='sec_manager_id') # DB 접속 Secret Manager
metaBucket   = Variable.get(key='s3_meta_bucket') # Glue 관련 파일 저장한 버킷명
secEncId     = Variable.get(key='sec_enc_id')     # 암호화 Secret Manager

base_date      = get_base_date()
# base_ym        = get_base_ym()
base_ym        = '202106'
today          = datetime.now().astimezone(timezone("Asia/Seoul")).strftime('%Y%m%d')
colecPathItgCd = '10003'
rds_hook       = RdsHook()

########################################################################################################################
# jobId 채번 함수
# trtStepCd  : 배치처리스텝통합코드
# trtResltCd : 배치처리결과통합코드
# trtStepNm  : 배치처리스텝명
# trtResltNm : 배치처리결과명
# commBizrCd : 통신사업자코드
def jobIdStart(trtStepCd :str, trtResltCd :str, trtStepNm :str, trtResltNm :str, commBizrCd : str):
    insertQuery = f"insert into tcb_co_db.co_ci_wrk_id_bas " \
            f"select 'CI_'||to_char(current_date, 'YYYYMMDD')||'{colecPathItgCd}'||ci_seq, '_', '{trtStepCd}', '{trtResltCd}', " \
            f"'{base_ym}', current_timestamp, to_char(current_date, 'YYYYMMDD'),  '{commBizrCd}', '{colecPathItgCd}', ci_seq, '{trtStepNm}', '{trtResltNm}', 'N' " \
            f"from (select nextval('tcb_co_db.co_ci_wrk_id_seq') as ci_seq) t101"

    with RdsConnection(secrets_manager_id=secManageId,
                       ) as rds_conn:
        cursor = rds_conn.connection.cursor()
        cursor.execute(insertQuery)
        rds_conn.connection.commit()

        cursor.close()
        rds_conn.connection.close()

def selectJobId(commBizrCd : str, batchStepCd : str):
    findQuery = f"select ci_wrk_id " \
                f"from (select row_number() over(partition by comm_bizr_itg_cd order by reg_dt desc) as rn, ci_wrk_id " \
                f"      from tcb_co_db.co_ci_wrk_id_bas " \
                f"      where comm_bizr_itg_cd = '{commBizrCd}' " \
                f"      and colec_path_itg_cd = '{colecPathItgCd}' " \
                f"      and batch_trt_step_itg_cd = '{batchStepCd}' " \
                f"      and batch_trt_yn = 'N'" \
                f"     ) t101 " \
                f"where rn = 1"

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
    return JobId

def jobStart(ciJobId : str, trtStepCd :str, trtResltCd :str, trtStepNm :str, trtResltNm :str, commBizrCd : str, trtStepCdBef :str):
    insertQuery = f"insert into tcb_co_db.co_ci_wrk_id_bas " \
                  f"select '{ciJobId}', '_', '{trtStepCd}', '{trtResltCd}', '{base_ym}', current_timestamp, " \
                  f"to_char(current_date, 'YYYYMMDD'),  '{commBizrCd}', '{colecPathItgCd}', {ciJobId[16:]}, '{trtStepNm}', '{trtResltNm}', 'N' "

    updateQuery = f"update tcb_co_db.co_ci_wrk_id_bas " \
                  f"set batch_trt_yn = 'Y' " \
                  f"where ci_wrk_id = '{ciJobId}' " \
                  f"and batch_trt_step_itg_cd = '{trtStepCdBef}'"

    with RdsConnection(secrets_manager_id=secManageId,
                       ) as rds_conn:
        cursor = rds_conn.connection.cursor()
        cursor.execute(insertQuery)
        rds_conn.connection.commit()

        cursor.execute(updateQuery)
        rds_conn.connection.commit()

        cursor.close()
        rds_conn.connection.close()

def jobEnd(ciJobId : str, trtStepCd :str, trtResltCd :str, trtStepNm :str, trtResltNm :str, commBizrCd : str):
    insertQuery = f"insert into tcb_co_db.co_ci_wrk_id_bas " \
                  f"select '{ciJobId}', '_', '{trtStepCd}', '{trtResltCd}', '{base_ym}', current_timestamp, " \
                  f"to_char(current_date, 'YYYYMMDD'),  '{commBizrCd}', '{colecPathItgCd}', {ciJobId[16:]}, '{trtStepNm}', '{trtResltNm}', 'N' "

    with RdsConnection(secrets_manager_id=secManageId,
                       ) as rds_conn:
        cursor = rds_conn.connection.cursor()
        cursor.execute(insertQuery)
        rds_conn.connection.commit()

        cursor.close()
        rds_conn.connection.close()

# 통신사 Interface ID를 조회
def selectIfId(comm_bizr_itg_cd : str):
    selectQuery = f"select t101.if_wrk_id " \
                  f"from (" \
                  f"     select if_wrk_id, row_number() over(partition by base_ym order by amd_dt desc) as rn " \
                  f"     from   tcb_co_db.co_if_wrk_id_bas " \
                  f"     where  colec_path_itg_cd = '{colecPathItgCd}' " \
                  f"     and    comm_bizr_itg_cd = '{comm_bizr_itg_cd}'" \
                  f"     and    wrk_sttus_val = 'COMPLETED' " \
                  f"     and    colec_trt_yn = 'N' " \
                  f"     ) t101 " \
                  f"where t101.rn = 1"

    ifId = ''
    with RdsConnection(secrets_manager_id=secManageId,
                       ) as rds_conn:
        cursor = rds_conn.connection.cursor()
        cursor.execute(selectQuery)
        ifIdTmp = cursor.fetchall()
        for i in ifIdTmp:
            ifId = i[0]
        rds_conn.connection.commit()

        cursor.close()
        rds_conn.connection.close()
    return ifId

# 통신사 Interface ID 적재유무를 조회
def checkIfIdYn(comm_bizr_itg_cd : str):
    # count = 0
    count = len(selectIfId(comm_bizr_itg_cd))
    # selectQuery = f"select count(*) as cnt " \
    #               f"from (" \
    #               f"     select if_wrk_id, row_number() over(partition by base_ym order by amd_dt desc) as rn " \
    #               f"     from   tcb_co_db.co_if_wrk_id_bas " \
    #               f"     where  colec_path_itg_cd = '{colecPathItgCd}' " \
    #               f"     and    wrk_sttus_val = 'COMPLETED' " \
    #               f"     ) t101 " \
    #               f"where t101.rn = 1"
    #
    # with RdsConnection(secrets_manager_id=secManageId,
    #                    ) as rds_conn:
    #     cursor = rds_conn.connection.cursor()
    #     cursor.execute(selectQuery)
    #     ifIdTmp = cursor.fetchall()
    #     for i in ifIdTmp[0]:
    #         count = i
    #     rds_conn.connection.commit()
    #
    #     cursor.close()
    #     rds_conn.connection.close()
    # print("##############################")
    # print("selectQuery : ", selectQuery)
    # print("count : ", count)
    # print("##############################")
    if count > 0:
        return 'data'
    else:
        return 'no_data'

# 통신사 인터페이스ID 처리유무 업데이트
def updateIfIdYn(ifWrkId : str):
    updateQuery = f"update tcb_co_db.co_if_wrk_id_bas " \
                  f"set colec_trt_yn = 'Y'" \
                  f"where if_wrk_id = '{ifWrkId}'"

    print("########################")
    print(updateQuery)
    print("########################")

    with RdsConnection(secrets_manager_id=secManageId,
                       ) as rds_conn:
        cursor = rds_conn.connection.cursor()
        cursor.execute(updateQuery)
        rds_conn.connection.commit()

        cursor.close()
        rds_conn.connection.close()

def get_nifi_pc_version(contextID : str):
    """
    NiFi의 Parameter Context version 정보를 가져온는 함수
    :param pcId: parameter_context ID
    :return: version 정보
    """
    # KT Parameter-Context ID : 9be3faab-f26d-306f-4e23-b15dee5399a3
    response = requests.get(
        f'{NIFI_API_URL}/parameter-contexts/{contextID}',
        headers={"Content-Type": "application/json"},
    )
    data = response.json()
    print("★★★★★★★★★★★★★★★★★★★★")
    print(data["revision"]["version"])
    return data["revision"]["version"]

def get_secret_info():
    """
    AWS Secret Manager의 Secrert Value를 가져온는 함수
    :param secretId : Secret Manager > Secret의 SecretName 값
    :return: ipinci를 암호화할때 쓰는 암호화키
    """
    client = boto3.client('secretsmanager',
                          region_name='ap-northeast-2',
                          aws_access_key_id=awsKeyId,
                          aws_secret_access_key=awsAccessKey
                          )
    response = client.get_secret_value(
        SecretId = secEncId
    )
    ipinci_secret = json.loads(response['SecretString'])
    print("★★★★★★★★★★★★★★★★★★★★")
    print(ipinci_secret['ENC_KEY'])
    return ipinci_secret['ENC_KEY']

def set_nifi_pc_value(ciJobId : str, ifJobId : str, contextID : str):
    """
    NiFi의 Parameter Context 변수를 셋팅하는 함수
    :param version : Parameter-Context의 최신 버전
    :param pcId : Update할 대상 Parameter-Context ID
    :param colecPathItgCd : 수집경로통합코드 (1001:배치, 1002:온디맨드, 1003:온라인)
    :param baseYam : 작업기준년월
    :param ciJobId : 수집작업아이디
    :param secretKey : 암호화키
    :return: none
    """
    version = get_nifi_pc_version(contextID)
    secretKey = get_secret_info()
    print("★★★★★★★★★★★★★★★★★★★★")
    print(version)
    print(secretKey)

    requests.put(
        f'{NIFI_API_URL}/parameter-contexts/{contextID}',
        headers={"Content-Type": "application/json"},
        data=json.dumps(
            {
                "revision": {
                    "version": version
                },
                "id": contextID,
                "uri": "{NIFI_API_URL}/parameter-contexts/{contextID}",
                "component": {
                    "parameters": [
                        {
                            "parameter": {
                                "name": "colecPathItgCd",
                                "value": colecPathItgCd,
                                "parameterContext": {
                                    "id": contextID
                                }
                            }
                        },
                        {
                            "parameter": {
                                "name": "baseYm",
                                "value": base_ym,
                                "parameterContext": {
                                    "id": contextID
                                }
                            }
                        },
                        {
                            "parameter": {
                                "name": "bucketName",
                                "value": s3BucketNm,
                                "parameterContext": {
                                    "id": contextID
                                }
                            }
                        },
                        {
                            "parameter": {
                                "name": "ciJobId",
                                "value": ciJobId,
                                "parameterContext": {
                                    "id": contextID
                                }
                            }
                        },
                        {
                            "parameter": {
                                "name": "encryption.key",
                                "value": secretKey,
                                "parameterContext": {
                                    "id": contextID
                                }
                            }
                        },
                        {
                            "parameter": {
                                "name": "ifJobId",
                                "value": ifJobId,
                                "parameterContext": {
                                    "id": contextID
                                }
                            }
                        }
                    ],
                    "id": contextID
                }
            }
        )
    )

def nifi_trigger_group(company: str,
                       state: str,
                       dag: DAG) -> None:
    """
    Nifi의 processor group을 실행 및 중단하는 함수

    :param company: task_id에 들어갈 이름 ex) kt, skt, lg
    :param state: nifi의 상태값. ex) RUNNING, STOPPED 등
    :param dag: DAG
    :return: None

    -> 2023.08.30 Nifi 값을 전달할 수 있는 방법 확인 필요
    """
    process_group_info = Variable.get(key='nifi_process_group_info', deserialize_json=True)
    requests.put(
        f'{NIFI_API_URL}/flow/process-groups/{process_group_info[company][state]}',
        headers={"Content-Type": "application/json"},
        data=json.dumps({
                "id": process_group_info[company][state],
                "state": state
        })
    )


# S3에서 테이블 정보를 가져오는 함수
# bucket: s3 버킷 이름
# key: s3 버킷 내 파일 경로
# return: s3 버킷 내 파일 정보
def get_table_info(bucket,
                   key,
                   **kwargs):
    """
    S3에서 테이블 정보를 가져오는 함수

    :param kwargs: bucket: s3 버킷 이름, key: s3 버킷 내 파일 경로
    :return: s3 버킷 내 파일 정보

    """
    s3 = boto3.client('s3')
    response = s3.get_object(
        Bucket=bucket,
        Key=key
    )
    return response['Body']


# nifi에서 s3로 데이터가 수집되었는 지 확인하는 sensors
# bucket: s3 버킷 이름
# id: task_id에 들어가며 bucket_key 또한 지정 ex) kt, skt, lg
def nifi_s3_sensor(table_name: str,
                   company: str,
                   dag: DAG,
                   **kwargs) -> S3KeySensor:
    """
    nifi에서 s3로 데이터가 수집되었는 지 확인하는 sensors

    :param table_name:
    :param company: task_id에 들어갈 이름 ex) kt, skt, lg
    :param dag: DAG

    """
    segment_key = '_'.join(table_name.split('_')[2:])

    segment_ids = {
        'cust_txn' : 'A0',
        'svc_cont_txn' : 'B0',
        'svc_cont_hst' : 'B1',
        'hndset_insl_cont_hst' : 'C1',
        'losdmg_insur_hst' : 'E1',
        'hndset_info_txn' : 'F0',
        # 'insl_cont_txn' : 'C0',
        # 'insl_cont_hst' : 'C1',
        'smstl_hst' : 'D1',
        'cust_hst' : 'A1',
    }

    return S3KeySensor(
        task_id=f'S.{table_name.upper()}_NIFI_S3_SENSOR',
        # bucket_key=f's3://{s3BucketNm}/{company.lower()}/monthly/{table_name}/yearmonth={base_ym}/IF_{segment_ids[segment_key]}_{table_name.upper()}_{base_date}_{today}.parquet',
        bucket_key=f's3://{s3BucketNm}/{company.lower()}/monthly/{table_name}/yearmonth={base_ym}/{company.upper()}BATCH_{segment_ids[segment_key]}_{table_name[6:].upper()}_{base_date}_{today}.parquet',
        dag=dag
    )

def update_data_catalog(company: str,
                        **kwargs) -> None:
    """
    S3에 저장된 테이블 정보를 통해 데이터 카탈로그를 업데이트하는 함수

    :param company: task_id에 들어갈 이름 ex) kt, skt, lg
    :return: None

    """
    s3_client = boto3.client('s3',
                             region_name='ap-northeast-2')

    meta_info_file = s3_client.get_object(
        Bucket=metaBucket,
        Key=f'jobs/META-INF/meta_dc_{company.lower()}_data_catalog_info_list.csv'
    )

    df = pd.read_csv(meta_info_file['Body'])
    df = df.fillna('')

    data_catalog_update(df)

########################################################################################################################
for company in COMPANIES.keys():
    # DAG 생성
    with DAG(
        dag_id=f'AFOD_001_TCB_DC_{company}_TELCO_TXN_M01_DAG',
        description=COMPANIES[company]['description'],
        default_args=default_args,
        tags=['nifi', 'glue', 's3', 'rds', 'monthly', 'batch'],
        # schedule = COMPANIES[company]['schedule'],
        schedule='@once',
        catchup=False,
    ) as dag:

        start_dag = EmptyOperator(task_id='START', dag=dag)
        end_dag   = EmptyOperator(task_id='END', trigger_rule='none_failed', dag=dag)
        data      = EmptyOperator(task_id='data', dag=dag)
        no_data   = EmptyOperator(task_id='no_data', dag=dag)

        # S3에 저장된 테이블 정보를 가져옴
        df = pd.read_csv(get_table_info(bucket=metaBucket, key=f'jobs/META-INF/meta_{company.lower()}_dc_table_column_list.csv'))

        crawler = GlueCrawlerOperator(task_id=f"T.GCOD_{company}_DC_CRAWLER",
                                      config={"Name" : f"GCOD_{company}_DC_CRAWLER"}
                                      )
        df_keys = df[['DB_NAME', 'SCHEMA_NAME', 'TABLE_NAME']].drop_duplicates().to_dict('records')

        with TaskGroup(f'GLUE_JOB_GROUP',
                       dag=dag) as glue_job:
            # 테이블 정보를 통해 GlueJobOperator를 호출
            for key in df_keys:
                # GlueJobOperator를 통해 Glue job을 호출
                # Glue job의 script_args에는 Glue job의 인자값을 넣어줌
                columns = ','.join(df[(df['DB_NAME'] == key['DB_NAME']) &
                                      (df['SCHEMA_NAME'] == key['SCHEMA_NAME']) &
                                      (df['TABLE_NAME'] == key['TABLE_NAME'])]['COLUMNS'].tolist()).lower()

                job_name = f'GJME_DL_{"_".join(key["TABLE_NAME"].split("_")[1:]).upper()}_1'
                GlueJobOperator(task_id=f'T.{job_name}',
                                job_name=f'{job_name}',
                                script_args={
                                    '--JOB_NAME': job_name,
                                    '--DB_NAME': key['DB_NAME'],
                                    '--SCHEMA_NAME' : key['SCHEMA_NAME'],
                                    '--COLUMNS' : columns,
                                    '--TABLE_NAME': key['TABLE_NAME'],
                                    '--BASE_YM': base_ym,
                                    '--COLEC_PATH_ITG_CD': colecPathItgCd, # batch
                                    '--S3_BUCKET_NM' : s3BucketNm
                                },
                                verbose=False, # Glue 로그 출력 여부
                                )

        with TaskGroup('SENSOR_GROUP',
                       dag=dag) as nifi_s3_sensor_group:
            for key in df_keys:
                nifi_s3_sensor(table_name=key['TABLE_NAME'],
                               company=company,
                               dag=dag)

        jobId_Start = PythonOperator(
            task_id=f'T.Job_Start',
            python_callable=jobIdStart,
            op_kwargs={'trtStepCd' :'20001', 'trtResltCd' : '20001', 'trtStepNm' : '정보수집(S3저장)', 'trtResltNm' : '작업시작', 'commBizrCd' : COMPANIES[company]['comm_bizr_cd']},
            dag=dag
        )

        ciJobId = selectJobId(COMPANIES[company]['comm_bizr_cd'], '20001')
        ifJobId = selectIfId(COMPANIES[company]['comm_bizr_cd'])
        # ifJobId = 'BATCH'

        ifJobid_chk = BranchPythonOperator(
            task_id=f'T.ifJobid_chk',
            python_callable=checkIfIdYn,
            op_kwargs={'comm_bizr_itg_cd': COMPANIES[company]['comm_bizr_cd']},
            dag=dag
        )

        nifi_parameterThrow = PythonOperator(
            task_id=f'T.nifi_parameterThrow',
            python_callable=set_nifi_pc_value,
            op_kwargs={'ciJobId' : ciJobId, 'ifJobId' : ifJobId, 'contextID' : COMPANIES[company]['contextID'] },
            dag=dag
        )

        nifi_running = PythonOperator(
            task_id=f'T.{company}_NIFI_RUNNING',
            python_callable=nifi_trigger_group,
            op_kwargs={'company': company, 'state': 'RUNNING'},
            dag=dag
        )
        nifi_stopped = PythonOperator(
            task_id=f'T.{company}_NIFI_STOPPED',
            python_callable=nifi_trigger_group,
            op_kwargs={'company': company, 'state': 'STOPPED'},
            dag=dag
        )

        S3job_End = PythonOperator(
            task_id=f'T.S3Job_End',
            python_callable=jobEnd,
            op_kwargs={'ciJobId' : ciJobId, 'trtStepCd' :'20001', 'trtResltCd' : '20002', 'trtStepNm' : '정보수집(S3저장)', 'trtResltNm' : '작업종료', 'commBizrCd' : COMPANIES[company]['comm_bizr_cd']},
            dag=dag
        )

        glue_table_update = PythonOperator(
            task_id=f'T.{company}_UPDATE_DATA_CATALOG',
            python_callable=update_data_catalog,
            op_kwargs={'company': company},
            dag=dag
        )

        ciJobId = selectJobId(COMPANIES[company]['comm_bizr_cd'], '20001')

        dqJob_Start = PythonOperator(
            task_id=f'T.DQJob_Start',
            python_callable=jobStart,
            op_kwargs={'ciJobId' : ciJobId, 'trtStepCd' :'20002', 'trtResltCd' : '20001', 'trtStepNm' : '데이터품질검증', 'trtResltNm' : '작업시작', 'commBizrCd' : COMPANIES[company]['comm_bizr_cd'], 'trtStepCdBef':'20001'},
            dag=dag
        )

        trigger_dq = TriggerDagRunOperator(
            task_id=f'T.TRIGGER_DQ',
            trigger_dag_id=f'AFOD_TCB_CO_{company}_DQ_CHK_RESLT_M01_DAG',
            execution_date=None,
            failed_states=['failed', 'upstream_failed'],
            wait_for_completion=True,
            dag=dag
        )

        ciJobId = selectJobId(COMPANIES[company]['comm_bizr_cd'], '20002')

        dqJob_End = PythonOperator(
            task_id=f'T.DQJob_End',
            python_callable=jobEnd,
            op_kwargs={'ciJobId' : ciJobId, 'trtStepCd' :'20002', 'trtResltCd' : '20002', 'trtStepNm' : '데이터품질검증', 'trtResltNm' : '작업종료', 'commBizrCd' : COMPANIES[company]['comm_bizr_cd']},
            dag=dag
        )

        Glue_Start = PythonOperator(
            task_id=f'T.DCGlue_Start',
            python_callable=jobStart,
            op_kwargs={'ciJobId' : ciJobId, 'trtStepCd' :'20003', 'trtResltCd' : '20001', 'trtStepNm' : '수집', 'trtResltNm' : '작업시작', 'commBizrCd' : COMPANIES[company]['comm_bizr_cd'], 'trtStepCdBef':'20002'},
            dag=dag
        )

        ciJobId = selectJobId(COMPANIES[company]['comm_bizr_cd'], '20003')

        Glue_End = PythonOperator(
            task_id=f'T.DCGlue_End',
            python_callable=jobEnd,
            op_kwargs={'ciJobId' : ciJobId, 'trtStepCd' :'20003', 'trtResltCd' : '20002', 'trtStepNm' : '수집', 'trtResltNm' : '작업종료', 'commBizrCd' : COMPANIES[company]['comm_bizr_cd']},
            dag=dag
        )

        CI_Start = PythonOperator(
            task_id=f'T.CiJob_Start',
            python_callable=jobStart,
            op_kwargs={'ciJobId' : ciJobId, 'trtStepCd' :'20004', 'trtResltCd' : '20001', 'trtStepNm' : '수집통합', 'trtResltNm' : '작업시작', 'commBizrCd' : COMPANIES[company]['comm_bizr_cd'], 'trtStepCdBef':'20003'},
            dag=dag
        )

        IFid_update = PythonOperator(
            task_id=f'T.IFid_update',
            python_callable=updateIfIdYn,
            op_kwargs={'ifWrkId' : ifJobId},
            dag=dag
        )

        start_dag \
        >> ifJobid_chk\
        >> data\
        >> jobId_Start\
        >> nifi_parameterThrow\
        >> nifi_running\
        >> nifi_s3_sensor_group\
        >> [crawler, nifi_stopped]\
        >> S3job_End\
        >> glue_table_update\
        >> dqJob_Start\
        >> trigger_dq\
        >> dqJob_End\
        >> Glue_Start\
        >> glue_job\
        >> Glue_End\
        >> [CI_Start, IFid_update]\
        >> end_dag

        start_dag \
        >> ifJobid_chk\
        >> no_data \
        >> end_dag