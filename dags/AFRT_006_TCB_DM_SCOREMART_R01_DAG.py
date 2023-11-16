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
from airflow.operators.python import BranchPythonOperator
from dependencies.utils.date_util import get_base_date, get_base_ym

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1, tzinfo=timezone('Asia/Seoul')),
}

# Airflow의 전역변수 값 가져오기
secManageId  = Variable.get(key='sec_manager_id')

now   = datetime.now() + relativedelta(hours=8)
year  = str(now.year)
month = '0'+str(now.month) if now.month in (1, 2, 3, 4, 5, 6, 7, 8, 9) else str(now.month)
day   = '0'+str(now.day) if len(str(now.day)) == 1 else str(now.day)
hour  = '0'+str(now.hour) if now.hour in (0, 1, 2, 3, 4, 5, 6, 7, 8, 9) else str(now.hour)

# reg_dt_bef = year+month+day+hour+"00"
# reg_dt_aft = year+month+day+hour+"59"

reg_dt_bef = '202310030100'
reg_dt_aft = '202310030159'

base_ym        = '202309'
# base_ym           = get_base_ym()
colec_path_itg_cd = '10002'  # 온라인
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

# IM Job ID(SCORE 단계) 작업시작 적재 및 FEATURE단계 처리여부 Y 업데이트
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
    dag_id='AFRT_006_TCB_DM_SCOREMART_R01_DAG',
    description='처리계DB_SCORE데이터적재스케줄링(실시간)',
    # schedule='30 04 20 * *',
    schedule='@once',
    tags=['procedure', 'feature', 'online'],
    catchup=False,
) as dag:

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
    #
    # sensor = ExternalTaskSensor(
    #     # CB마트 작업 대기
    #     task_id='S.AFRT_TCB_DM_FEATUREMART_R01_DAG',
    #     external_dag_id='AFRT_TCB_DM_FEATUREMART_R01_DAG',
    #     external_task_id='END',
    #     mode='reschedule',
    #     execution_date_fn=lambda dt: dt,
    # )

    rds_hook = RdsHook()

    with TaskGroup(group_id='PROCEDURE_FT_GROUP') as procedure_ft_group:
        procedures = [
            {
                'schema':  'TCB_DM_DB',
                'procedure_name': 'PEC_SC_SCORE_CMP_MSTR_TXN_1',
                'parameter': (base_ym,colec_path_itg_cd),
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

    # start >> sensor >> onLine_Chk >> data >> procedure_ft_group >> jobId_End >> jobId_Next >> end
    # start >> sensor >> onLine_Chk >> no_data >> end
    start >> procedure_ft_group >> jobId_End >> jobId_Next >> end
