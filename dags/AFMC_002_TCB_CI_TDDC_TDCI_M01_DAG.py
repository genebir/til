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
from dependencies.utils.date_util import get_base_date, get_base_ym

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1, tzinfo=timezone('Asia/Seoul')),
}

# Airflow의 전역변수 값 가져오기
secManageId  = Variable.get(key='sec_manager_id')

companies         = ['KT', 'SK', 'LG']
rds_hook          = RdsHook()
base_ym           = '202106'
# base_ym        = get_base_ym()
colec_path_itg_cd = '10001'  # 정기배치
########################################################################################################################
def selectJobId():
    findQuery = f"select t101.ci_wrk_id " \
                f"from   tcb_co_db.co_ci_wrk_id_bas t101 " \
                f"WHERE  t101.colec_path_itg_cd      = '{colec_path_itg_cd}' " \
                f"AND    t101.batch_trt_step_itg_cd  = '20004' " \
                f"AND    t101.batch_trt_reslt_itg_cd = '20001'" \
                f"AND    t101.batch_trt_yn = 'N' " \

    jobId = []
    with RdsConnection(secrets_manager_id=secManageId,
                       ) as rds_conn:
        cursor = rds_conn.connection.cursor()
        cursor.execute(findQuery)
        # jobId = cursor.fetchall()[0][0]
        JobIdTemp = cursor.fetchall()
        for i in JobIdTemp:
            jobId.append(i[0])

        cursor.close()
        rds_conn.connection.close()
    print("★★★★★★★★★★★★★★★★★★★★")
    print('JobId = ', jobId)
    print("★★★★★★★★★★★★★★★★★★★★")
    return jobId

# CI파트 작업종료 적재
def jobIdEnd(trtStepCd :str, trtResltCd :str, trtResltNm :str):
    ciJobId = selectJobId()

    for jobId in ciJobId:
        insertQuery = f"insert into tcb_co_db.co_ci_wrk_id_bas " \
                      f" select ci_wrk_id, online_svc_rqt_id, batch_trt_step_itg_cd, '{trtResltCd}' as batch_trt_reslt_itg_cd, base_ym, current_timestamp as reg_dt," \
                      f"         trt_date, comm_bizr_itg_cd, colec_path_itg_cd, ci_seq, batch_trt_step_nm, '{trtResltNm}' as batch_trt_reslt_nm," \
                      f"        'N' as batch_trt_yn" \
                      f" from tcb_co_db.co_ci_wrk_id_bas" \
                      f" where ci_wrk_id = '{jobId}'" \
                      f" and batch_trt_step_itg_cd = '{trtStepCd}';"
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

# 통합파트 작업 시, 참조 위해 임시데이터 적재 및 CI 배치처리여부 Y 업데이트
def jobIdIns(trtStepCd :str, trtResltCd :str, trtStepNm :str, trtResltNm :str):
    ciJobId = selectJobId()

    for jobId in ciJobId:
        insertQuery = f"insert into tcb_co_db.co_ci_wrk_id_bas " \
                      f" select '{jobId}' as ci_wrk_id, " \
                      f"        '_' as online_svc_rqt_id, " \
                      f"        '{trtStepCd}' as batch_trt_step_itg_cd, " \
                      f"        '{trtResltCd}' as batch_trt_reslt_itg_cd, " \
                      f"        base_ym, " \
                      f"        current_timestamp as reg_dt," \
                      f"        trt_date, " \
                      f"        comm_bizr_itg_cd, " \
                      f"        colec_path_itg_cd, " \
                      f"        ci_seq, " \
                      f"        '{trtStepNm}' as batch_trt_step_nm, " \
                      f"        '{trtResltNm}' as batch_trt_reslt_nm," \
                      f"        'N' as batch_trt_yn " \
                      f"from    tcb_co_db.co_ci_wrk_id_bas " \
                      f"where   ci_wrk_id = '{jobId}' " \
                      f"and     batch_trt_step_itg_cd = '20004' " \
                      f"limit 1"

        updateQuery = f"update tcb_co_db.co_ci_wrk_id_bas set batch_trt_yn = 'Y' " \
                      f"where ci_wrk_id = '{jobId}' " \
                      f"and batch_trt_step_itg_cd = '20004' "

        print("★★★★★★★★★★★★★★★★★★★★")
        print(insertQuery)
        print("★★★★★★★★★★★★★★★★★★★★")

        with RdsConnection(secrets_manager_id=secManageId,
                           ) as rds_conn:
            cursor = rds_conn.connection.cursor()
            cursor.execute(insertQuery)
            rds_conn.connection.commit()

            cursor.execute(updateQuery)
            rds_conn.connection.commit()

            cursor.close()
            rds_conn.connection.close()

########################################################################################################################
with DAG(
    default_args=default_args,
    dag_id='AFMC_002_TCB_CI_TDDC_TDCI_M01_DAG',
    description='처리계DB_수집통합영역데이터적재스케줄링',
    # schedule= '30 20 * * *',
    schedule='@once',
    tags=['procedure', 'monthly', 'batch'],
    catchup=False,
) as dag:
    start = EmptyOperator(task_id='START')
    end = EmptyOperator(task_id='END')

    #base_ym = (datetime.now(tz=timezone('Asia/Seoul')) - relativedelta(months=1)).strftime('%Y%m')

    with TaskGroup(group_id='SENSOR_GROUP') as sensor_group:
        for company in companies:
            ExternalTaskSensor(
                task_id=f'S.AFMC_001_TCB_DC_{company}_TELCO_TXN_M01_DAG',
                external_dag_id=f'AFMC_001_TCB_DC_{company}_TELCO_TXN_M01_DAG',
                external_task_id='END',
                mode='reschedule',
                execution_date_fn=lambda dt: dt - timedelta(minutes=10),
            )

    with TaskGroup(group_id='PROCEDURE_GROUP') as procedure_group:

        procedures = [
            {  # 처리계(수집통합)_고객이력적재
                'schema': 'TCB_DC_DB',
                'procedure_name': 'PEC_CI_CUST_HST_1',
                'parameter': (base_ym,colec_path_itg_cd),
            },
            {   #처리계(수집통합)_고객내역적재
                'schema':  'TCB_DC_DB',
                'procedure_name': 'PEC_CI_CUST_TXN_1',
                'parameter': (base_ym,colec_path_itg_cd),
            },
            # {  # 처리계(수집통합)_할부계약이력적재
            #     'schema': 'TCB_DC_DB',
            #     'procedure_name': 'PEC_CI_INSL_CONT_HST_1',
            #     'parameter': (base_ym,colec_path_itg_cd),
            # },
            # {  # 처리계(수집통합)_할부계약내역적재
            #     'schema': 'TCB_DC_DB',
            #     'procedure_name': 'PEC_CI_INSL_CONT_TXN_1',
            #     'parameter': (base_ym,colec_path_itg_cd),
            # },
            {  # 처리계(수집통합)_소액결제이력적재
                'schema': 'TCB_DC_DB',
                'procedure_name': 'PEC_CI_SMSTL_HST_1',
                'parameter': (base_ym,colec_path_itg_cd),
            },
            {  # 처리계(수집통합)_서비스계약이력적재
                'schema': 'TCB_DC_DB',
                'procedure_name': 'PEC_CI_SVC_CONT_HST_1',
                'parameter': (base_ym,colec_path_itg_cd),
            },
            {   #처리계(수집통합)_서비스계약내역적재
                'schema':  'TCB_DC_DB',
                'procedure_name': 'PEC_CI_SVC_CONT_TXN_1',
                'parameter': (base_ym,colec_path_itg_cd),
            },
            {  # 처리계(수집통합)_단말기정보내역적재
                'schema': 'TCB_DC_DB',
                'procedure_name': 'PEC_CI_HNDSET_INFO_TXN_1',
                'parameter': (base_ym, colec_path_itg_cd),
            },
            {  # 처리계(수집통합)_단말기할부계약이력적재
                'schema': 'TCB_DC_DB',
                'procedure_name': 'PEC_CI_HNDSET_INSL_CONT_HST_1',
                'parameter': (base_ym, colec_path_itg_cd),
            },
            {  # 처리계(수집통합)_분실파손보험 월별이력적재
                'schema': 'TCB_DC_DB',
                'procedure_name': 'PEC_CI_LOSDMG_INSUR_HST_1',
                'parameter': (base_ym, colec_path_itg_cd),
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

    # ciJobId = selectJobId()

    ciJob_End = PythonOperator(
        task_id=f'T.ciJob_End',
        python_callable=jobIdEnd,
        op_kwargs={'trtStepCd' :'20004', 'trtResltCd' :'20002', 'trtResltNm' :'작업종료'},
        dag=dag
    )

    diJob_Start = PythonOperator(
        task_id=f'T.diWaitJob_Insert',
        python_callable=jobIdIns,
        op_kwargs={'trtStepCd':'20005', 'trtResltCd':'20004', 'trtStepNm':'통합', 'trtResltNm':'작업대기'},
        dag=dag
    )

    start >> sensor_group >> procedure_group >> ciJob_End >> diJob_Start >> end