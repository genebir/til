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
colecPathItgCd = '10001'  # 정기배치
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
                   f"and batch_trt_step_itg_cd = '20008'"

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
    dag_id='AFMC_006_TCB_DM_SCOREMART_M01_DAG',
    description='처리계DB_SCORE마트데이터적재스케줄링',
    # schedule='30 22 * * *',
    schedule='@once',
    tags=['procedure', 'feature', 'monthly', 'batch'],
    catchup=False,
) as dag:
    start = EmptyOperator(task_id='START')
    end = EmptyOperator(task_id='END')

    sensor = ExternalTaskSensor(
        # CB마트 작업 대기
        task_id='S.AFMC_005_TCB_DM_FEATUREMART_M01_DAG',
        external_dag_id='AFMC_005_TCB_DM_FEATUREMART_M01_DAG',
        external_task_id='END',
        mode='reschedule',
        execution_date_fn=lambda dt: dt - timedelta(minutes=30),
    )

    rds_hook = RdsHook()

    with TaskGroup(group_id='PROCEDURE_SC_GROUP') as procedure_ft_group:
        procedures = [
            {
                'schema':  'TCB_DM_DB',
                'procedure_name': 'PEC_SC_SCORE_CMP_MSTR_TXN_1',
                'parameter': (base_ym,colecPathItgCd),
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

    jobId = selectJobId('20008')

    jobId_End = PythonOperator(
        task_id=f'T.Job_End',
        python_callable=jobEnd,
        op_kwargs={'ciJobId': jobId, 'trtStepCd': '20008', 'trtResltCd': '20002', 'trtStepNm': '스코어마트', 'trtResltNm': '작업종료'},
        dag=dag
    )

    jobId_Next = PythonOperator(
        task_id=f'T.featureJob_Start',
        python_callable=jobIdNext,
        op_kwargs={'ciJobId': jobId, 'trtStepCd': '20009', 'trtResltCd': '20001', 'trtStepNm': '정보제공(S3저장)', 'trtResltNm': '작업시작'},
        dag=dag
    )

    start >> sensor >> procedure_ft_group >> jobId_End >> jobId_Next >> end
