# DQ Ruleset을 실행하고 결과값을 Rds에 저장하는 DAG
#
# 실제 DAG_IDs:
# AFOD_TCB_CO_KT_DQ_CHK_RESLT_M01_DAG
# AFOD_TCB_CO_SK_DQ_CHK_RESLT_M01_DAG
# AFOD_TCB_CO_LG_DQ_CHK_RESLT_M01_DAG
#

import boto3
from time import sleep
from airflow.sensors.external_task import ExternalTaskSensor
from dependencies.utils.rds import RdsConnection
import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from pendulum import timezone
from pytz import timezone as tz
from airflow.utils.task_group import TaskGroup
import pandas as pd
import io
import json
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
from airflow.models.variable import Variable
from dateutil.relativedelta import relativedelta
from airflow.models.variable import Variable

default_args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2023, 1, 1, tzinfo=timezone('Asia/Seoul')),
}

secManageId    = Variable.get(key='sec_manager_mwaa')
s3_meta_bucket = Variable.get(key='s3_meta_bucket')  # cb-dev-glue-s3, cb-prd-glue-s3
IAM_ROLE       = Variable.get(key='iam_role_gule')   # IAM수행 Role
# 개발Role = 'arn:aws:iam::715016971379:role/AWSGlueServiceRole'
# 운영Role = 'arn:aws:iam::045685141620:role/CB-PRD-GLUE-ROLE'

def create_or_update_dq_ruleset(glue_client, ruleset: dict, **context) -> None:
    """
    dq ruleset을 생성하거나 업데이트하는 함수

    @param glue_client: boto3.client('glue', region_name='ap-northeast-2')
    @param ruleset: ruleset 정보가 담긴 dict
    @return:
    """

    try:
        # ruleset이 존재하는지 확인
        dq_response = glue_client.get_data_quality_ruleset(
            Name=ruleset['ruleset_name']
        )
        print(f'>>>>>>>>>>>>>>>> RULESET: {ruleset["ruleset_name"]} LOAD <<<<<<<<<<<<<<<<')

        # ruleset이 존재한다면 meta 정보와 비교, 다르다면 업데이트
        if dq_response['TargetTable']['TableName'].lower() != ruleset['table_name'].lower():
            print(f"""RULESET: {ruleset["ruleset_name"]}의 TargetTable이 Meta 정보와 다릅니다. 
                Meta 정보: {ruleset["table_name"]} 
                RULESET 정보: {dq_response['TargetTable']['TableName']}""")
        if dq_response['TargetTable']['DatabaseName'].lower() != ruleset['db_name'].lower():
            print(f""">>>>>>>>>>>>>>> RULESET: {ruleset["ruleset_name"]}의 TargetTable의 DatabaseName이 Meta 정보와 다릅니다. <<<<<<<<<<<<<<<< 
                Meta 정보: {ruleset["db_name"]} 
                RULESET 정보: {dq_response['TargetTable']['DatabaseName']}""")
            print(f"""RULESET: {ruleset["ruleset_name"]}의 TargetTable의 DatabaseName을
             Meta 정보와 동일하게 수정하기 위해 삭제 후 재생성합니다. <<<<<<<<<<<<<<<<""")

            glue_client.delete_data_quality_ruleset(
                Name=ruleset['ruleset_name']
            )
            create_dq = glue_client.create_data_quality_ruleset(
                Name=ruleset['ruleset_name'],
                Description=ruleset['ruleset_description'],
                Ruleset=ruleset['rules'],
                TargetTable={
                    'DatabaseName': ruleset['db_name'].lower(),
                    'TableName': ruleset['table_name'].lower()
                }
            )
            print(create_dq)
            print(f'>>>>>>>>>>>>>>>> RULESET: {ruleset["ruleset_name"]} 를 재생성했습니다. <<<<<<<<<<<<<<<<')

        if dq_response['Ruleset'] != ruleset['rules']:
            update_dq_response = glue_client.update_data_quality_ruleset(
                Name=ruleset['ruleset_name'],
                Description=ruleset['ruleset_description'],
                Ruleset=ruleset['rules']
            )
            print(dq_response)
            print(f'>>>>>>>>>>>>>>>> RULESET: {ruleset["ruleset_name"]} 정보가 UPDATE 되었습니다. <<<<<<<<<<<<<<<<')
        elif dq_response['Description'] != ruleset['ruleset_description']:
            update_dq_response = glue_client.update_data_quality_ruleset(
                Name=ruleset['ruleset_name'],
                Description=ruleset['ruleset_description'],
                Ruleset=ruleset['rules']
            )
            print(f'>>>>>>>>>>>>>>>> RULESET: {ruleset["ruleset_name"]} 정보가 UPDATE 되었습니다. <<<<<<<<<<<<<<<<')
    except glue_client.exceptions.EntityNotFoundException as e:
        # ruleset이 존재하지 않는다면 생성
        # ruleset의 TargetTable이 존재하는지 확인
        try:
            table_response = glue_client.get_table(
                DatabaseName=ruleset['db_name'].lower(),
                Name=ruleset['table_name'].lower()
            )
            print(table_response)
            if table_response['ResponseMetadata']['HTTPStatusCode'] == 200 and table_response['Table'][
                'DatabaseName'].lower() == ruleset['db_name'].lower():
                dq_response = glue_client.create_data_quality_ruleset(
                    Name=ruleset['ruleset_name'],
                    Description=ruleset['ruleset_description'],
                    Ruleset=ruleset['rules'],
                    TargetTable={
                        'DatabaseName': ruleset['db_name'].lower(),
                        'TableName': ruleset['table_name'].lower()
                    }
                )
                print(f'>>>>>>>>>>>>>>>> RULESET: {meta_info["RULESET_NAME"]} 를 생성했습니다. <<<<<<<<<<<<<<<<')
            else:
                print(f'>>>>>>>>>>>>>>>> RULESET: {meta_info["RULESET_NAME"]}을 생성에 실패했습니다. <<<<<<<<<<<<<<<<')
            pass
        except glue_client.exceptions.EntityNotFoundException as e:
            print(
                f'>>>>>>>>>>>>>>>> RULESET: {meta_info["RULESET_NAME"]}의 TargetTable({ruleset["table_name"].lower()})이 존재하지 않습니다. <<<<<<<<<<<<<<<<')
            pass


def dq_active(glue_client, database_name, table_name, ruleset_name, **context):
    """
    dq rule을 실행하는 함수

    @param glue_client: boto3.client('glue', region_name='ap-northeast-2')
    @param database_name: dq ruleset을 실행할 table의 database name
    @param table_name: dq ruleset을 실행할 table name
    @param ruleset_name: dq ruleset의 name
    @param context:
    @return:
    """
    dq = glue_client.start_data_quality_ruleset_evaluation_run(
        DataSource={
            'GlueTable': {
                'DatabaseName': database_name,
                'TableName': table_name,
                'AdditionalOptions': {
                    # 'catalogPartitionPredicate': f"yearmonth={(datetime.datetime.now(tz('Asia/Seoul'))-relativedelta(months=1)).strftime('%Y%m')}"
                    'catalogPartitionPredicate': f"yearmonth=202106"
                }
            }
        },
        RulesetNames=ruleset_name,  # must be list
        Role = IAM_ROLE
        #Role='arn:aws:iam::715016971379:role/AWSGlueServiceRole'
        #Role='arn:aws:iam::045685141620:role/CB-PRD-GLUE-ROLE'
    )
    print(dq)
    return dq['RunId']


def dq_status_check(glue_client, run_id, ruleset_kr_nm, **context):
    """
    dq rule의 status를 확인하는 함수

    @param glue_client: boto3.client('glue', region_name='ap-northeast-2')
    @param run_id: dq rule의 run_id
    @param ruleset_kr_nm: dq rule의 한글명
    @param context:
    @return:
    """
    # 상태값을 담을 변수
    response = None
    # 상태값이 PASS 혹은 FAIL 이 될 때까지 반복
    while True:
        # 상태값을 가져옴
        response = glue_client.get_data_quality_ruleset_evaluation_run(
            RunId=run_id
        )
        # 상태값이 SUCCEEDED, FAILED, STOPPED 중 하나라면 반복문 탈출
        # 그렇지 않다면 10초 대기 후 다시 상태값을 가져옴
        if response['Status'] == 'SUCCEEDED':
            break
        elif response['Status'] == 'FAILED' or response['Status'] == 'STOPPED':
            print(f"---------------DATA QUALITY CHECK STATUS: {response['Status']}---------------")
            break
        else:
            print(f"---------------DATA QUALITY CHECK STATUS: {response['Status']}---------------")
            sleep(10)
    # xcom key값 생성을 위해 변수에 담아줌
    print(response)
    table_ = response['DataSource']['GlueTable']['TableName']
    ruleset_ = response['RulesetNames'][0]
    xcom_value = {
        'result_id': response['ResultIds'][0],
        'run_id': run_id,
        'ruleset_kr_nm': ruleset_kr_nm
    }
    # xcom push
    context['ti'].xcom_push(key=f'{table_}_{ruleset_}', value=json.dumps(xcom_value))
    pass


def get_dq_result(glue_client, ruleset_list_, **context):
    """
    dq rule의 결과를 가져오는 함수
    @param context:
    @return:
    """
    # 오류 건 담을 변수
    err_result = []
    # result 테이블에 저장할 데이터를 담을 변수
    result_df = []
    # detail 테이블에 저장할 데이터를 담을 변수
    detail_df = []
    # xcom_pull을 통해 dq rule의 result_id와 run_id를 가져옴
    result_val_list = []
    for tmp_ruleset in ruleset_list_:
        result_val_list.append(
            context['ti'].xcom_pull(key=f"{tmp_ruleset['table_name']}_{tmp_ruleset['ruleset_name']}"))
    # 가져온 result_id와 run_id를 통해 dq rule의 결과값을 가져옴
    for val in result_val_list:
        val = json.loads(val)

        dq_result = glue_client.get_data_quality_result(
            ResultId=val['result_id']
        )
        print(dq_result)

        # Score가 100점이 안되는 오류건
        if dq_result['Score'] != 100:
            err_rule = [(rs['Description'].split(' ')[1].replace('"', ''), rs['Description'].split(' ')[0],
                         rs['EvaluationMessage'] if len(rs['EvaluationMessage']) < 2000 else rs['EvaluationMessage'][
                                                                                             :1989] + '...truncate',
                         rs['Name']) for rs in dq_result['RuleResults'] if rs['Result'] == 'FAIL']

            for err in err_rule:
                # print(err[0]) : select
                # print(err[1]) : CustomSql
                # print(err[2]) : Custom SQL response failed to satisfy the threshold
                # print(err[3]) : Rule_1

                err_result.append({
                    "db_name": dq_result['DataSource']['GlueTable']['DatabaseName'],
                    "table_name": dq_result['DataSource']['GlueTable']['TableName'],
                    # "column_name" : err[0],
                    "column_name": err[0] if err[0] != 'select' else 'CustomSql',
                    # CustomSql은 앞에 select로 컬럼명이 나오므로, CustomSql로 컬럼명 변경
                    "ruleset_name": dq_result['RulesetName'],
                    "rule_detail": err[1] if err[3] != 'Rule_1' else 'Key Check',  # Rule_1은 항상 PK체크
                    "err_msg": err[2]
                })

        # 가져온 결과값을 변수에 담음
        base_dt = datetime.datetime.strftime(datetime.datetime.now().astimezone(timezone('Asia/Seoul')), '%Y%m%d')
        start_dt = dq_result['StartedOn'].astimezone(timezone('Asia/Seoul'))
        end_dt = dq_result['CompletedOn'].astimezone(timezone('Asia/Seoul'))

        result_df.append((base_dt,
                          val['run_id'],
                          dq_result['RulesetName'],
                          val['ruleset_kr_nm'],
                          dq_result['DataSource']['GlueTable']['DatabaseName'],
                          dq_result['DataSource']['GlueTable']['TableName'],
                          dq_result['Score'],
                          start_dt,
                          end_dt))

        for rs in dq_result['RuleResults']:
            evaluated_metrics = str(rs['EvaluatedMetrics']).strip('{').strip('}')
            err_msg = rs.get('EvaluationMessage', '_')

            detail_df.append((base_dt,
                              val['run_id'],
                              rs['Name'],
                              rs['Description'],
                              rs['Result'],
                              evaluated_metrics if len(evaluated_metrics) > 0 else '_',
                              err_msg if len(err_msg) < 2000 else err_msg[:1989] + '...truncate'
                              ))
    context['ti'].xcom_push(key='err_result', value=json.dumps(err_result))
    print(f">>>>>>>>>>>>>>>>> err_result: {len(err_result)}")
    print(f">>>>>>>>>>>>>>>>> result_df: {len(result_df)}")
    print(f">>>>>>>>>>>>>>>>> detail_df: {len(detail_df)}")
    print(f">>>>>>>>>>>>>>>>> result_val_list: {len(result_val_list)}")

    # rds에 연결
    #with RdsConnection(secrets_manager_id='dev/proc/dtcbtrtdb/tcbmwaaid',
    with RdsConnection(secrets_manager_id=secManageId,  # 'dev/proc/dtcbtrtdb/tcbmwaaid'
                       ) as rds_conn:
        cursor = rds_conn.connection.cursor()
        # result 테이블에 데이터 삭제 (base_dt 기준)
        # cursor.execute("DELETE FROM tcb_co_db.co_dq_chk_reslt_txn WHERE base_date = %s", (base_dt,))
        # cursor.execute("DELETE FROM tcb_co_db.co_dq_chk_reslt_dtl WHERE base_date = %s", (base_dt,))
        # result 테이블에 데이터 저장
        cursor.executemany("INSERT INTO tcb_co_db.co_dq_chk_reslt_txn VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                           result_df)
        # detail 테이블에 데이터 저장
        cursor.executemany("INSERT INTO tcb_co_db.co_dq_chk_reslt_dtl VALUES (%s, %s, %s, %s, %s, %s, %s)", detail_df)
        rds_conn.connection.commit()
        cursor.close()
        rds_conn.connection.close()

        # DQ결과에 오류건 1건이라도 있으면, 오류발생, 오류가 발생 되야 이메일 전송
        if len(err_result) > 0:
            print('DQ Result has Error')
            raise Exception('DQ Result has Error')
        else:
            pass


def send_result(**context):
    email_info = Variable.get(key='email_info', deserialize_json=True)
    SMTP_SERVER = email_info['smtp_server']
    SMTP_PORT = email_info['smtp_port']
    smtp = None

    err_result = json.loads(context['ti'].xcom_pull(key='err_result'))

    table = """
        border-collapse: separate;
        border-spacing: 0;
        text-align: center;
        line-height: 1.5;
        border-top: 1px solid #ccc;
        border-left: 1px solid #ccc;
        margin : 20px 10px;
        width: 100%;
    """

    th = """
        width: 150px;
        padding: 10px;
        font-weight: bold;
        vertical-align: top;
        border-right: 1px solid #ccc;
        border-bottom: 1px solid #ccc;
        border-top: 1px solid #fff;
        border-left: 1px solid #fff;
        background: #eee;
        font-size: 12pt;
    """

    td = """
        padding: 10px;
        vertical-align: top;
        border-right: 1px solid #ccc;
        border-bottom: 1px solid #ccc;
        font-size: 10pt;
    """
    td_data = ""
    err_seq = 0
    for err in err_result:
        err_seq += 1
        td_data += f"""
            <tr>
                <td style='{td} width: 35px;'>{err_seq}</td>
                <td style='{td}'>{err['db_name']}</td>
                <td style='{td}'>{err['table_name']}</td>
                <td style='{td}'>{err['column_name']}</td>
                <td style='{td}'>{err['ruleset_name']}</td>
                <td style='{td}'>{err['rule_detail']}</td>
                <td style='{td}'>{err['err_msg']}</td>
            </tr>
        """

    print(td_data)

    try:
        smtp = smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT)
        from_addr = email_info['from_addr']
        print(from_addr)
        id = from_addr['id']
        pw = from_addr['pw']
        smtp.login(id, pw)

        msg = MIMEMultipart('TEST')
        html_text = f"""
        <head>
        <title>Page Title</title>
        </head>
        <body>

        <h1 style='text-align: center'>데이터 품질 점검 오류 항목 ({datetime.datetime.now().strftime('%Y%m%d')})</h1>
        <div style='display: flex;
                    justify-content: center;
                    align-item: center;
                    '>

        <table style='{table}'>
        <thead>
            <tr>
                <th style='{th} width: 30px;'>No</th>
                <th style='{th}'>데이터베이스명</th>
                <th style='{th}'>테이블명</th>
                <th style='{th}'>컬럼명</th>
                <th style='{th}'>룰셋명</th>
                <th style='{th} width: 110px;'>룰상세</th>
                <th style='{th} width: 200px;'>오류메시지</th>
            </tr>
        </thead>
        <tbody>
            {td_data}
        </tbody>
        </table>
        </body>
        """
        html = MIMEText(html_text, 'html')
        msg.attach(html)
        msg['Subject'] = f'{context["company"]}데이터 품질 점검결과'

        smtp.sendmail(
            from_addr=from_addr['id'],
            #to_addrs=email_info['to_addr'],    # 메일발신 1명
            to_addrs=[addr.replace(' ', '') for addr in email_info['to_addr'].split(',')],
            msg=msg.as_string()
        )

    except Exception as e:
        raise e
    finally:
        smtp.quit()
    pass


companies = ['KT', 'SK', 'LG']
for company in companies:
    with DAG(
            default_args=default_args,
            dag_id=f'AFOD_TCB_CO_{company}_DQ_CHK_RESLT_M01_DAG',
            schedule=None,
            tags=['dq', 'monthly', f'{company}'],
            description=f'AWS(GLUE)_{company}_DQ수행결과로그저장스케줄(온디맨드)',
            catchup=False
    ) as dag:
        start = EmptyOperator(
            task_id='START'
        )
        end = EmptyOperator(
            task_id='END',
            trigger_rule='none_failed'
        )
        mail_title = f'{company}'
        s3_client = boto3.client('s3', region_name='ap-northeast-2')
        glue_client = boto3.client('glue', region_name='ap-northeast-2')

        # sensor = ExternalTaskSensor(
        #             task_id=f'S.AFMC_TCB_DC_{company}_TELCO_TXN_M01_DAG',
        #             external_dag_id=f'AFMC_TCB_DC_{company}_TELCO_TXN_M01_DAG',
        #             external_task_id=f'T.{company}_UPDATE_DATA_CATALOG',
        #             mode='reschedule',
        #             execution_date_fn=lambda dt: dt,
        #             dag=dag
        #             )

        ruleset_list = []
        # DQ script 주석처리 해주는 부분, 여기 내용이 있으면 한글 부분까지 써주고
        # 없으면, Key 오류로 그걸 받아서 한글은 넣지 않고, Rule명만 주석에 넣음
        # flag에는 이전 점검항목에 대해 담고 있어서, ColumnExists에서 ColumnLength로 점검항목이 바뀌면 주석처리
        RULE_INFO = {
            'ColumnExists': '컬럼존재여부',
            'ColumnLength': '컬럼길이',
            'ColumnValues': '컬럼값',
            'IsComplete': '컬럼NOTNULL'
        }
        # ruleset 정보가 담긴 파일 읽어서 ruleset_list에 담기
        obj = s3_client.get_object(Bucket=f'{s3_meta_bucket}',  # glue 경로
                                   Key=f'dataquality/scripts/dq_od_{company.lower()}_ruleset_info.csv')
        df = pd.read_csv(io.BytesIO(obj['Body'].read()), encoding='cp949')
        df = df.fillna('')
        meta_info_key = df[['DB_NAME', 'TABLE_NAME', 'RULESET_NAME', 'RULESET_DESCRIPTION']].drop_duplicates().to_dict(
            'records')

        for meta_info in meta_info_key:
            ruleset = df.loc[(df.DB_NAME == meta_info['DB_NAME'].lower()) &
                             (df.TABLE_NAME == meta_info['TABLE_NAME'].lower()) &
                             (df.RULESET_NAME == meta_info['RULESET_NAME'].upper())]
            RULES = "Rules = [\n"
            flag = ""
            rule_count = 0
            for rule in ruleset['RULE_DETAIL']:
                if flag != rule.split(' ')[0]:
                    rule_count += 1
                    flag = rule.split(' ')[0]
                    try:
                        RULES += f"""# DQ Rule{rule_count} ({flag}) {RULE_INFO[flag]}\n"""
                    except KeyError:
                        RULES += f"""# DQ Rule{rule_count} ({flag})\n"""

                RULES += f"""{rule}, \n"""

            RULES = RULES[:-3]
            RULES += "\n]"
            print(RULES)

            ruleset_list.append({
                'db_name': meta_info['DB_NAME'],
                'table_name': meta_info['TABLE_NAME'],
                'ruleset_name': meta_info['RULESET_NAME'],
                'ruleset_description': meta_info['RULESET_DESCRIPTION'],
                'rules': RULES
            })

        table_list = list(set([rule_set['table_name'] for rule_set in ruleset_list]))

        group_list = []
        for table_name in table_list:
            tmp_group = TaskGroup(group_id=f"GROUP_{table_name.upper()}", dag=dag)
            for ruleset in ruleset_list:
                if ruleset['table_name'] == table_name:
                    create_or_update = PythonOperator(
                        task_id=f"T.{ruleset['ruleset_name'].upper()}_CREATE_OR_UPDATE_DQ_RULESET",
                        python_callable=create_or_update_dq_ruleset,
                        op_kwargs={
                            'glue_client': glue_client,
                            'ruleset': ruleset,
                        },
                        task_group=tmp_group,
                    )
                    dq_active_task = PythonOperator(task_id=f"T.{ruleset['ruleset_name'].upper()}_DQ_ACTIVE",
                                                    python_callable=dq_active,
                                                    op_kwargs={
                                                        'glue_client': glue_client,
                                                        'database_name': ruleset['db_name'],
                                                        'table_name': ruleset['table_name'],
                                                        'ruleset_name': [ruleset['ruleset_name']],  # must be list
                                                    },
                                                    task_group=tmp_group
                                                    )
                    dq_status_check_task = PythonOperator(
                        task_id=f"T.{ruleset['ruleset_name'].upper()}_DQ_STATUS_CHECK",
                        python_callable=dq_status_check,
                        op_kwargs={
                            'glue_client': glue_client,
                            "run_id": dq_active_task.output,
                            'ruleset_kr_nm': ruleset['ruleset_description']
                        },
                        task_group=tmp_group
                        )
                    create_or_update >> dq_active_task >> dq_status_check_task
            group_list.append(tmp_group)

        task_get_dq_result = PythonOperator(
            task_id='T.GET_DQ_RESULT',
            python_callable=get_dq_result,
            op_kwargs={
                'glue_client': glue_client,
                'ruleset_list_': ruleset_list,
            },
            dag=dag
        )

        task_send_email = PythonOperator(
            task_id='T.SEND_EMAIL',
            python_callable=send_result,
            op_kwargs={
                'company': company
            },
            trigger_rule='all_failed',
            dag=dag
        )

        if len(group_list) == 0:
            group_list.append(EmptyOperator(task_id='EMPTY_DQ_RULESET'))

        start >> group_list >> task_get_dq_result >> task_send_email >> end
        # start >> group_list >> task_get_dq_result >> end
        task_get_dq_result >> end