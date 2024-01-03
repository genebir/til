from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pendulum import timezone
from dependencies.hooks.rds import RdsHook
from dependencies.operators.rds import ProcedureOperator
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from dependencies.utils.rds import RdsConnection
from airflow.operators.python import PythonOperator
from airflow.models.variable import Variable
from airflow.operators.python import BranchPythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1, tzinfo=timezone('Asia/Seoul')),
}

secManageId = Variable.get(key='sec_manager_id')

now   = datetime.now() + relativedelta(hours=8)
year  = str(now.year)
month = '0'+str(now.month) if now.month in (1, 2, 3, 4, 5, 6, 7, 8, 9) else str(now.month)
day   = '0'+str(now.day) if len(str(now.day)) == 1 else str(now.day)
hour  = '0'+str(now.hour) if now.hour in (0, 1, 2, 3, 4, 5, 6, 7, 8, 9) else str(now.hour)

# reg_dt_bef = year+month+day+hour+"00"
# reg_dt_aft = year+month+day+hour+"59"

reg_dt_bef = '202310030100'
reg_dt_aft = '202310030159'
colec_path_itg_cd = '10002' # 온라인
base_ym           = '202309'
# base_ym = (datetime.now(tz=timezone('Asia/Seoul')) - relativedelta(months=1)).strftime('%Y%m')

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

# IM Job Id 채번 및 통합단계 적재 및 ci 수집통합 job 배치처리여부 Y 업데이트
def jobIdStart(trtStepCd :str, trtResltCd :str, trtStepNm :str, trtResltNm :str):
    insertQuery = f"insert into tcb_co_db.co_im_wrk_id_bas " \
                  f"select 'IM_'||to_char(current_date, 'YYYYMMDD')||'{colec_path_itg_cd}'||ci_seq, '_', '{trtStepCd}', '{trtResltCd}', " \
                  f"'{base_ym}', current_timestamp, to_char(current_date, 'YYYYMMDD'),  '_', '{colec_path_itg_cd}', ci_seq, '{trtStepNm}', '{trtResltNm}', 'N' " \
                  f"from (select nextval('tcb_co_db.co_im_wrk_id_seq') as ci_seq) t101"
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

# 채번한 통합단계 IM Job ID Select
def selectJobId(batchStepCd : str):
    findQuery = f"select im_wrk_id " \
                f"from (" \
                f"select row_number() over(partition by comm_bizr_itg_cd order by reg_dt desc) as rn, im_wrk_id " \
                f"from tcb_co_db.co_im_wrk_id_bas " \
                f"where colec_path_itg_cd = '{colec_path_itg_cd}' " \
                f"and batch_trt_step_itg_cd = '{batchStepCd}'" \
                f"and batch_trt_yn = 'N'" \
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

# IM Job ID(통합단계) 작업종료 적재
def jobEnd(ciJobId : str, trtStepCd :str, trtResltCd :str, trtStepNm :str, trtResltNm :str):
    insertQuery = f"insert into tcb_co_db.co_im_wrk_id_bas " \
                  f"select '{ciJobId}', '_', '{trtStepCd}', '{trtResltCd}', '{base_ym}', current_timestamp," \
                  f"to_char(current_date, 'YYYYMMDD'),  '_', '{colec_path_itg_cd}', {ciJobId[16:]}, '{trtStepNm}', '{trtResltNm}', 'N' "

    print("★★★★★★★★★★★★★★★★★★★★")
    print(insertQuery)
    print("★★★★★★★★★★★★★★★★★★★★")

    # co_ci_wrk_id_bas 테이블에 수집통합 배치처리여부를 Y로 업데이트
    updateQuery1 = f"update tcb_co_db.co_ci_wrk_id_bas " \
                   f"set batch_trt_yn = 'Y' " \
                   f"where  batch_trt_step_itg_cd = '20005' " \
                   f"and    colec_path_itg_cd     = '{colec_path_itg_cd}' " \
                   f"and    ci_wrk_id in (select ci_wrk_id" \
                   f"                     from (select ci_wrk_id, row_number() over(partition by comm_bizr_itg_cd order by reg_dt desc) as rn" \
                   f"                           from   tcb_co_db.co_ci_wrk_id_bas" \
                   f"                           where  batch_trt_step_itg_cd = '20005'" \
                   f"                           and    colec_path_itg_cd     = '{colec_path_itg_cd}'" \
                   f"                           and    batch_trt_yn          = 'N'" \
                   f"                          ) t201" \
                   f"                     where rn = 1)"
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

# IM Job ID(CB마트 단계) 작업시작 적재 및 통합단계 배치처리여부 업데이트
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

    # co_im_wrk_id_bas 테이블의 통합 배치처리여부를 Y로 업데이트
    updateQuery1 = f"update tcb_co_db.co_im_wrk_id_bas " \
                   f"set batch_trt_yn = 'Y' " \
                   f"where im_wrk_id = '{ciJobId}' " \
                   f"and batch_trt_step_itg_cd = '20005'"

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
    dag_id='AFRT_003_TCB_DI_TDCI_TDDI_R01_DAG',
    description='처리계DB_통합영역데이터적재스케줄링',
    # schedule='30 04 20 * *',
    schedule='@once',
    tags=['procedure', 'online'],
    catchup=False,
) as dag:

    start   = EmptyOperator(task_id='START')
    end     = EmptyOperator(task_id='END', trigger_rule='none_failed', dag=dag)
    # data    = EmptyOperator(task_id='data',dag=dag,)
    # no_data = EmptyOperator(task_id='no_data', dag=dag, )

    # onLine_Chk = BranchPythonOperator(
    #     task_id='onLine_Chk',
    #     python_callable=selectOnline,
    #     dag=dag,
    # )

    rds_hook = RdsHook()

    jobId_Start = PythonOperator(
        task_id=f'T.Job_Start',
        python_callable=jobIdStart,
        op_kwargs={'trtStepCd': '20005', 'trtResltCd': '20001', 'trtStepNm': '통합', 'trtResltNm': '작업시작'},
        dag=dag
    )

    # sensor = ExternalTaskSensor(
    #             task_id=f'S.AFRT_TCB_CI_TDDC_TDCI_R01_DAG',
    #             external_dag_id=f'AFRT_TCB_CI_TDDC_TDCI_R01_DAG',
    #             external_task_id='END',
    #             mode='reschedule',
    #             execution_date_fn=lambda dt: dt - timedelta(minutes=30),
    #     )

        # 1작업
    mk_cust_id = ProcedureOperator(
        # 처리계(공통)_통합고객아이디생성및적재
        task_id='T.PEC_CO_SBT_ID_SRC_PTY_REL_1',
        schema='TCB_CO_DB',
        procedure_name='PEC_CO_SBT_ID_SRC_PTY_REL_1',
        parameter=(base_ym, colec_path_itg_cd)
    )

    # 2작업
    with TaskGroup(group_id='DI_PROCEDURE_GROUP') as di_procedure_group:

        procedures = [
            {  # 처리계(통합)_고객내역적재
                'schema': 'TCB_DM_DB',
                'procedure_name': 'PEC_DI_CUST_TXN_1',
                'parameter': (base_ym, colec_path_itg_cd)
            },
            {  # 처리계(통합)_고객이력적재
                'schema': 'TCB_DM_DB',
                'procedure_name': 'PEC_DI_CUST_HST_1',
                'parameter': (base_ym, colec_path_itg_cd)
            },
            {  # 처리계(통합)_서비스계약내역적재
                'schema': 'TCB_DM_DB',
                'procedure_name': 'PEC_DI_SVC_CONT_TXN_1',
                'parameter': (base_ym, colec_path_itg_cd)
            },
            {  # 처리계(통합)_서비스계약이력적재
                'schema': 'TCB_DM_DB',
                'procedure_name': 'PEC_DI_SVC_CONT_HST_1',
                'parameter': (base_ym, colec_path_itg_cd)
            },
            # {  # 처리계(통합)_할부계약내역적재
            #     'schema': 'TCB_DM_DB',
            #     'procedure_name': 'PEC_DI_INSL_CONT_TXN_1',
            #     'parameter': (base_ym, colec_path_itg_cd)
            # },
            # {  # 처리계(통합)_할부계약이력적재
            #     'schema': 'TCB_DM_DB',
            #     'procedure_name': 'PEC_DI_INSL_CONT_HST_1',
            #     'parameter': (base_ym, colec_path_itg_cd)
            # },
            {  # 처리계(통합)_소액결제이력적재
                'schema': 'TCB_DM_DB',
                'procedure_name': 'PEC_DI_SMSTL_HST_1',
                'parameter': (base_ym, colec_path_itg_cd)
            },
            {  # 처리계(통합)_
                'schema': 'TCB_DM_DB',
                'procedure_name': 'PEC_DI_HNDSET_INFO_TXN_1',
                'parameter': (base_ym, colec_path_itg_cd)
            },
            {  # 처리계(통합)_
                'schema': 'TCB_DM_DB',
                'procedure_name': 'PEC_DI_HNDSET_INSL_CONT_HST_1',
                'parameter': (base_ym, colec_path_itg_cd)
            },
            {  # 처리계(통합)_
                'schema': 'TCB_DM_DB',
                'procedure_name': 'PEC_DI_LOSDMG_INSUR_HST_1',
                'parameter': (base_ym, colec_path_itg_cd)
            },
        ]
        for procedure in procedures:
            op = ProcedureOperator(
                hook=rds_hook,
                task_id=f'T.{procedure["procedure_name"]}',
                schema=procedure['schema'],
                procedure_name=procedure['procedure_name'],
                parameter=(procedure['parameter']),
            )

    ciJobId = selectJobId('20005')

    jobId_End = PythonOperator(
        task_id=f'T.Job_End',
        python_callable=jobEnd,
        op_kwargs={'ciJobId': ciJobId, 'trtStepCd': '20005', 'trtResltCd': '20002', 'trtStepNm': '통합', 'trtResltNm': '작업종료'},
        dag=dag
    )

    jobId_Next = PythonOperator(
        task_id=f'T.dmJob_Start',
        python_callable=jobIdNext,
        op_kwargs={'ciJobId': ciJobId, 'trtStepCd': '20006', 'trtResltCd': '20001', 'trtStepNm': 'CB마트', 'trtResltNm': '작업시작'},
        dag=dag
    )

    # start >> sensor >> onLine_Chk >> data >> jobId_Start >>  mk_cust_id >> di_procedure_group >> jobId_End >> jobId_Next >> end
    # start >> sensor >> onLine_Chk >> no_data >> end
    start >> jobId_Start >> mk_cust_id >> di_procedure_group >> jobId_End >> jobId_Next >> end