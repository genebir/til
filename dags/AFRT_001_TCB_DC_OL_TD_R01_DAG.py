import boto3
from airflow import DAG
from datetime import datetime, timedelta,timezone
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.empty import EmptyOperator
import pendulum
from dependencies.utils.rds import RdsConnection
from dateutil.relativedelta import relativedelta
from airflow.models.variable import Variable
from airflow.operators.python import BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    'owner': 'airflow',
    # UTC 기준 시간으로 DAG가 Schedule되기 때문에 한국 시간으로 변경
    'start_date': datetime(2023, 8, 15, tzinfo=pendulum.timezone('Asia/Seoul')),
}

now     = datetime.now() + relativedelta(hours=8)
now_str = now.strftime('%Y%m%d')
year    = str(now.year)
month   = '0'+str(now.month) if now.month in (1, 2, 3, 4, 5, 6, 7, 8, 9) else str(now.month)
day     = '0'+str(now.day) if len(str(now.day)) == 1 else str(now.day)
hour    = '0'+str(now.hour) if now.hour in (0, 1, 2, 3, 4, 5, 6, 7, 8, 9) else str(now.hour)

# reg_dt_bef = year+month+day+hour+"00"
# reg_dt_aft = year+month+day+hour+"59"

reg_dt_bef = '202310292100'
reg_dt_aft = '202310292159'

base_ym        = '202309'
colecPathItgCd = '10002' # Online
# base_ym = now.strftime('%Y%m%d')

secManageId = Variable.get(key='sec_manager_id')
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

# 온라인데이터 적재 유무 확인
def selectOnline():
    count = 0
    print('#####################################')
    print('reg_dt_bef : ', reg_dt_bef)
    print('reg_dt_aft : ', reg_dt_aft)
    print('#####################################')
    selectQuery = f"select count(*) as cnt from tcb_dc_db.dc_ol_input_txn " \
                  f"where to_char(reg_dt, 'YYYYMMDDHH24MI') between '{reg_dt_bef}' and '{reg_dt_aft}'" \
                  f"and online_trt_yn = 'N'"
    # selectQuery = f"select count(*) as cnt from tcb_dc_db.dc_ol_input_txn " \
    #               f"where base_date = '20231029' " \
    #               f"and online_trt_yn = 'N'"
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

# 온라인 입력내역의 온라인처리여부 Y 업데이트
def updateOnline():
    print('#####################################')
    print('reg_dt_bef : ', reg_dt_bef)
    print('reg_dt_aft : ', reg_dt_aft)
    print('#####################################')
    updateQuery = f"update tcb_dc_db.dc_ol_input_txn " \
                  f"set online_trt_yn = 'Y' " \
                  f"where to_char(reg_dt, 'YYYYMMDDHH24MI') between '{reg_dt_bef}' and '{reg_dt_aft}'" \
                  f"and online_trt_yn = 'N'"

    with RdsConnection(secrets_manager_id=secManageId,
                       ) as rds_conn:
        cursor = rds_conn.connection.cursor()
        cursor.execute(updateQuery)
        rds_conn.connection.commit()

        cursor.close()
        rds_conn.connection.close()


# Online 작업ID 최초 채번 및 적재
# Glue 작업 수행 전이기 떄문에 online_svc_rqt_id 컬럼 적재 불가
def jobIdStart(trtStepCd :str, trtResltCd :str, trtStepNm :str, trtResltNm :str):
    insertQuery = f"insert into tcb_co_db.co_ci_wrk_id_bas " \
                  f"select 'CI_'||to_char(current_date, 'YYYYMMDD')||'{colecPathItgCd}'||ci_seq, '_', '{trtStepCd}', '{trtResltCd}', " \
                  f"'{base_ym}', current_timestamp, to_char(current_date, 'YYYYMMDD'),  '_', '{colecPathItgCd}', ci_seq, '{trtStepNm}', '{trtResltNm}', 'N' " \
                  f"from (select nextval('tcb_co_db.co_ci_wrk_id_seq') as ci_seq) t101"

    with RdsConnection(secrets_manager_id=secManageId,
                       ) as rds_conn:
        cursor = rds_conn.connection.cursor()
        cursor.execute(insertQuery)
        rds_conn.connection.commit()

        cursor.close()
        rds_conn.connection.close()

def selectJobId(batchStepCd : str):
    findQuery = f"select ci_wrk_id " \
                f"from (select row_number() over(partition by comm_bizr_itg_cd order by reg_dt desc) as rn, ci_wrk_id " \
                f"      from tcb_co_db.co_ci_wrk_id_bas " \
                f"      where colec_path_itg_cd = '{colecPathItgCd}' " \
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


def jobEnd(ciJobId : str, trtStepCd :str, trtResltCd :str, trtStepNm :str, trtResltNm :str):
    insertQuery = f"insert into tcb_co_db.co_ci_wrk_id_bas " \
                  f"select '{ciJobId}', '_', '{trtStepCd}', '{trtResltCd}', '{base_ym}', current_timestamp, " \
                  f"to_char(current_date, 'YYYYMMDD'),  '_', '{colecPathItgCd}', {ciJobId[16:]}, '{trtStepNm}', '{trtResltNm}', 'N' "

    with RdsConnection(secrets_manager_id=secManageId,
                       ) as rds_conn:
        cursor = rds_conn.connection.cursor()
        cursor.execute(insertQuery)
        rds_conn.connection.commit()

        cursor.close()
        rds_conn.connection.close()

def jobStart(ciJobId : str, trtStepCd :str, trtResltCd :str, trtStepNm :str, trtResltNm :str, trtStepCdBef :str):
    insertQuery = f"insert into tcb_co_db.co_ci_wrk_id_bas " \
                  f"select '{ciJobId}', '_', '{trtStepCd}', '{trtResltCd}', '{base_ym}', current_timestamp, " \
                  f"to_char(current_date, 'YYYYMMDD'),  '_', '{colecPathItgCd}', {ciJobId[16:]}, '{trtStepNm}', '{trtResltNm}', 'N' "

    selectQuery = f"select distinct svc_rqt_id from   tcb_dc_db.dc_ol_cust_txn where  ci_wrk_id = '{ciJobId}'"

    with RdsConnection(secrets_manager_id=secManageId,
                       ) as rds_conn:
        cursor = rds_conn.connection.cursor()
        cursor.execute(insertQuery)
        rds_conn.connection.commit()

        cursor.execute(selectQuery)
        svcRqtIdTemp = cursor.fetchall()

        for i in svcRqtIdTemp[0]:
            svcRqtId = i

        # 처리한 작업ID의 처리여부를 Y로 업데이트
        updateQuery1 = f"update tcb_co_db.co_ci_wrk_id_bas " \
                      f"set batch_trt_yn = 'Y'" \
                      f"where ci_wrk_id = '{ciJobId}' " \
                      f"and batch_trt_step_itg_cd = '{trtStepCdBef}'"

        cursor.execute(updateQuery1)
        rds_conn.connection.commit()

        # 온라인 작업ID의 서비스트랜잭션ID 업데이트
        updateQuery2 = f"update tcb_co_db.co_ci_wrk_id_bas " \
                      f"set online_svc_rqt_id = '{svcRqtId}'" \
                      f"where ci_wrk_id = '{ciJobId}' " \

        cursor.execute(updateQuery2)
        rds_conn.connection.commit()

        cursor.close()
        rds_conn.connection.close()

########################################################################################################################
# csv파일에서 읽어온 테이블, 컬럼, 스키마, 프로그램명 정보
with DAG(
        dag_id=f'AFRT_001_TCB_DC_OL_TD_R01_DAG',
        description='처리계DB_온라인실시간데이터수집스케줄링(실시간)',
        default_args=default_args,
        tags=['glue', 'rds', 'daily', 'online'],
        schedule='@once',
        # schedule_interval='@hourly',
        # schedule='50 16 * * *',
        # schedule_interval='25 10 * * *',
        catchup=False,
        max_active_tasks=6,
        concurrency=24
) as dag:

    onLine_Chk = BranchPythonOperator(
        task_id='onLine_Chk',
        python_callable=selectOnline,
        dag=dag,
    )

    jobId_Start = PythonOperator(
        task_id=f'T.Job_Start',
        python_callable=jobIdStart,
        op_kwargs={'trtStepCd': '20003', 'trtResltCd': '20001', 'trtStepNm': '수집', 'trtResltNm': '작업시작'},
        dag=dag
    )

    ciJobId = selectJobId('20003')

    glue_job = GlueJobOperator(task_id=f'T.GJRT_DC_TELCO_TRT_TXN_1',
                               job_name=f'GJRT_DC_TELCO_TRT_TXN_1',
                               script_args={
                                    '--CI_WRK_ID': ciJobId
                               },
                               verbose=False,  # Glue 로그 출력 여부
                               )

    Glue_End = PythonOperator(
        task_id=f'T.DCGlue_End',
        python_callable=jobEnd,
        op_kwargs={'ciJobId': ciJobId, 'trtStepCd': '20003', 'trtResltCd': '20002', 'trtStepNm': '수집', 'trtResltNm': '작업종료'},
        dag=dag
    )

    Online_Update = PythonOperator(
        task_id=f'T.Online_Update',
        python_callable=updateOnline,
        op_kwargs={},
        dag=dag
    )

    CI_Start = PythonOperator(
        task_id=f'T.CiJob_Start',
        python_callable=jobStart,
        op_kwargs={'ciJobId': ciJobId, 'trtStepCd': '20004', 'trtResltCd': '20001', 'trtStepNm': '수집통합', 'trtResltNm': '작업시작', 'trtStepCdBef': '20003'},
        dag=dag
    )

    online_ci = TriggerDagRunOperator(
        task_id=f'T.ci_procedure',
        trigger_dag_id=f'AFRT_002_TCB_CI_TDDC_TDCI_R01_DAG',
        execution_date=None,
        # execution_date=datetime.now().replace(tzinfo=timezone.utc),
        failed_states=['failed', 'upstream_failed'],
        wait_for_completion=True,
        dag=dag
    )

    online_di = TriggerDagRunOperator(
        task_id=f'T.di_procedure',
        trigger_dag_id=f'AFRT_003_TCB_DI_TDCI_TDDI_R01_DAG',
        execution_date=None,
        failed_states=['failed', 'upstream_failed'],
        wait_for_completion=True,
        dag=dag
    )

    online_cbmart = TriggerDagRunOperator(
        task_id=f'T.cbmart_procedure',
        trigger_dag_id=f'AFRT_004_TCB_DM_CBMART_R01_DAG',
        execution_date=None,
        failed_states=['failed', 'upstream_failed'],
        wait_for_completion=True,
        dag=dag
    )

    online_feature = TriggerDagRunOperator(
        task_id=f'T.feature_procedure',
        trigger_dag_id=f'AFRT_005_TCB_DM_FEATUREMART_R01_DAG',
        execution_date=None,
        failed_states=['failed', 'upstream_failed'],
        wait_for_completion=True,
        dag=dag
    )

    online_score = TriggerDagRunOperator(
        task_id=f'T.score_procedure',
        trigger_dag_id=f'AFRT_006_TCB_DM_SCOREMART_R01_DAG',
        execution_date=None,
        failed_states=['failed', 'upstream_failed'],
        wait_for_completion=True,
        dag=dag
    )

    online_dt = TriggerDagRunOperator(
        task_id=f'T.online_dt',
        trigger_dag_id=f'AFRT_007_TCB_DT_CBMART_R01_DAG',
        execution_date=None,
        failed_states=['failed', 'upstream_failed'],
        wait_for_completion=True,
        dag=dag
    )

    opt_insert = TriggerDagRunOperator(
        task_id=f'T.opt_insert_online',
        trigger_dag_id=f'AFRT_008_OPT_IF_CBMART_R01_DAG',
        execution_date=None,
        failed_states=['failed', 'upstream_failed'],
        wait_for_completion=True,
        dag=dag
    )

    start_dag = EmptyOperator(task_id='START', dag=dag)
    end_dag   = EmptyOperator(task_id='END', trigger_rule='none_failed', dag=dag)
    data      = EmptyOperator(task_id='data',dag=dag,)
    no_data   = EmptyOperator(task_id='no_data', dag=dag, )

    start_dag >> onLine_Chk >> data >> jobId_Start >> glue_job >> [Glue_End, Online_Update] >> CI_Start >> online_ci >> online_di >> \
    online_cbmart >> online_feature >> online_score >> online_dt >> opt_insert >> end_dag
    start_dag >> onLine_Chk >> no_data >> end_dag

