from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pendulum import timezone
from dependencies.operators.rds import ProcedureOperator
from dependencies.hooks.rds import RdsHook
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from dependencies.utils.rds import RdsConnection
from airflow.models.variable import Variable
from airflow.operators.python import PythonOperator
from dependencies.utils.date_util import get_base_date, get_base_ym

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1, tzinfo=timezone('Asia/Seoul')),
}

# Airflow의 전역변수 값 가져오기
secManageId  = Variable.get(key='sec_manager_id')

base_ym        = '202106'
# base_ym           = get_base_ym()
colecPathItgCd = '10003'  # 온디멘드
########################################################################################################################
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

# IM Job ID(SCORE 단계) 작업시작 적재 및 FEATURE단계 처리여부 Y 업데이트
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
                   f"and batch_trt_step_itg_cd = '20007'"

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
with DAG(
    default_args=default_args,
    dag_id='AFOD_005_TCB_DM_FEATUREMART_M01_DAG',
    description='처리계DB_FEATURE마트데이터적재스케줄링(온디멘드)',
    # schedule='30 04 20 * *',
    schedule='@once',
    tags=['procedure', 'feature', 'ondemand'],
    catchup=False,
) as dag:
    start = EmptyOperator(task_id='START')
    end = EmptyOperator(task_id='END')

    sensor = ExternalTaskSensor(
        # CB마트 작업 대기
        task_id='S.AFOD_TCB_DM_CBMART_M01_DAG',
        external_dag_id='AFOD_TCB_DM_CBMART_M01_DAG',
        external_task_id='END',
        mode='reschedule',
        execution_date_fn=lambda dt: dt,
    )

    rds_hook = RdsHook()

    with TaskGroup(group_id='PROCEDURE_FT_GROUP') as procedure_ft_group:

        # 1.PEC_FT_TPS_CONT_TXN_1           처리계(FEATURE)_TPS계약내역적재
        # 2.PEC_FT_TPS_CHAGE_TXN_1          처리계(FEATURE)_TPS요금내역적재
        # 3.PEC_FT_TPS_COMM_SVC_USE_TXN_1   처리계(FEATURE)_TPS통신서비스사용내역적재(통화/데이터/문자)
        # 4.PEC_FT_TPS_INSUR_TXN_1	        처리계(FEATURE)_TPS보험내역적재
        # 5.PEC_FT_TPS_PAY_TXN_1	        처리계(FEATURE)_TPS납부내역적재
        # 6.PEC_FT_TPS_DLINQ_TXN_1          처리계(FEATURE)_TPS연체내역적재
        # 7.PEC_FT_TPS_SMSTL_TXN_1	        처리계(FEATURE)_TPS소액결제내역적재
        # 8.PEC_FT_TPS_HNDSET_TXN_1	        처리계(FEATURE)_TPS단말기내역적재
        # 9.PEC_FT_TPS_ETC_TXN_1	        처리계(FEATURE)_TPS기타내역적재
        # 10.PEC_FT_TPS_PERF_TXN_1	        처리계(FEATURE)_TPS성능내역적재

        procedures = [
            {
                'schema':  'TCB_DM_DB',
                'procedure_name': 'PEC_FT_TPS_CONT_TXN_1',
                'parameter': (base_ym, colecPathItgCd),
            },
            {
                'schema':  'TCB_DM_DB',
                'procedure_name': 'PEC_FT_TPS_CHAGE_TXN_1',
                'parameter': (base_ym, colecPathItgCd),
            },
            {
                'schema': 'TCB_DM_DB',
                'procedure_name': 'PEC_FT_TPS_COMM_SVC_USE_TXN_1',
                'parameter': (base_ym, colecPathItgCd),
            },
            {
                'schema':  'TCB_DM_DB',
                'procedure_name': 'PEC_FT_TPS_INSUR_TXN_1',
                'parameter': (base_ym, colecPathItgCd),
            },
            {
                'schema':  'TCB_DM_DB',
                'procedure_name': 'PEC_FT_TPS_PAY_TXN_1',
                'parameter': (base_ym, colecPathItgCd),
            },
            {
                'schema':  'TCB_DM_DB',
                'procedure_name': 'PEC_FT_TPS_DLINQ_TXN_1',
                'parameter': (base_ym, colecPathItgCd),
            },
            {
                'schema': 'TCB_DM_DB',
                'procedure_name': 'PEC_FT_TPS_SMSTL_TXN_1',
                'parameter': (base_ym, colecPathItgCd),
            },
            {
                'schema': 'TCB_DM_DB',
                'procedure_name': 'PEC_FT_TPS_HNDSET_TXN_1',
                'parameter': (base_ym, colecPathItgCd),
            },
            {
                'schema': 'TCB_DM_DB',
                'procedure_name': 'PEC_FT_TPS_ETC_TXN_1',
                'parameter': (base_ym, colecPathItgCd),
            },
            {
                'schema': 'TCB_DM_DB',
                'procedure_name': 'PEC_FT_TPS_PERF_TXN_1',
                'parameter': (base_ym, colecPathItgCd),
            },
        ]

        for procedure in procedures:
            ProcedureOperator(
                hook=rds_hook,
                task_id=f'T.{procedure["procedure_name"]}',
                schema=procedure['schema'],
                procedure_name=procedure['procedure_name'],
                parameter=(procedure['parameter']),
            )

    key_proc = ProcedureOperator(
                            hook=rds_hook,
                            task_id=f'T.PEC_FT_TPS_KEY_TXN_1',
                            schema='TCB_DM_DB',
                            procedure_name='PEC_FT_TPS_KEY_TXN_1',
                            parameter=(base_ym, colecPathItgCd),
                        )

    jobId = selectJobId('20007')

    jobId_End = PythonOperator(
        task_id=f'T.Job_End',
        python_callable=jobEnd,
        op_kwargs={'ciJobId': jobId, 'trtStepCd': '20007', 'trtResltCd': '20002', 'trtStepNm': '피쳐마트', 'trtResltNm': '작업종료'},
        dag=dag
    )

    jobId_Next = PythonOperator(
        task_id=f'T.featureJob_Start',
        python_callable=jobIdNext,
        op_kwargs={'ciJobId': jobId, 'trtStepCd': '20008', 'trtResltCd': '20001', 'trtStepNm': '스코어마트', 'trtResltNm': '작업시작'},
        dag=dag
    )

    start >> sensor >> key_proc >> procedure_ft_group >> jobId_End >> jobId_Next >> end
