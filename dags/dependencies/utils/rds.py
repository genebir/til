"""
AIRFLOW에서 공통으로 사용하는 함수들을 정의한 파일

"""
import json
import boto3
import psycopg2
import sqlalchemy.engine
from sshtunnel import SSHTunnelForwarder
import configparser
from datetime import datetime
import socket

class KstTime:
    """
    한국 시간을 반환하는 클래스

    get_kst_now(): 한국 날짜 및 시간을 반환하는 함수
    get_kst_now_without_time(): 한국 날짜를 반환하는 함수
    get_kst_now_format(format: str): 한국 날짜를 인자 값에 맞는 포맷으로 반환하는 함수
    """
    def __init__(self):
        pass

    @staticmethod
    def get_kst_now() -> str:
        """
        한국 날짜 및 시간을 반환하는 함수
        @return: str
        """
        kst_time = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S")
        return kst_time


    @staticmethod
    def get_kst_now_without_time() -> str:
        """
        한국 날짜를 반환하는 함수
        @return: str
        """
        kst_time = datetime.now().astimezone().strftime("%Y-%m-%d")
        return kst_time


    @staticmethod
    def get_kst_now_format(format: str) -> str:
        """
        한국 날짜를 인자 값에 맞는 포맷으로 반환하는 함수
        @param format:
        @return: str
        """
        kst_time = datetime.now().astimezone().strftime(format)
        return kst_time

class RdsConnection:
    """
    RDS Connection을 생성하는 클래스

    ##########################################################################################
    # RDS Connection을 생성하는 클래스의 생성자
    #
    # @param secret      : secret manager에서 가져올 secret의 key 값
    # @param local       : local에서 테스트를 진행할 경우 True, 아닐 경우 False
    # @param secret_info : local에서 테스트를 진행할 경우 secret manager에서 가져올 secret의 정보
    #                      여기에 포함되는 정보는 host, port, username, password, dbname가 있어야합니다.
    # @param port        : local에서 테스트를 진행할 경우 RDS의 port 번호 (이건 안 쓰셔도 됩니다.)
    # @param remote_rds  : rdsconfig 파일의 key 값 (V13 -> postgresql 13버전 key, V14 -> postgresql 14버전 key)
    ##########################################################################################

    * connection_info(): secret manager에서 secret을 가져오는 함수
    * connect(): rds connection을 생성하는 함수
    * disconnect(): rds connection을 종료하는 함수
    * create_tunnel(): local에서 테스트를 진행할 경우 tunnel을 생성하는 함수

    ##########################################################################################
    # Local에서 테스트할 시 확인할 것
    #
    # 1. rdsconfig.ini 파일의 key 값과 생성자의 remote_rds의 인자 값이 같은지 확인
    # 2. 해당 정보와 RDS의 커넥션 정보가 일치하는 지 확인
    # 3. 해당 정보와 RDS의 커넥션 정보가 일치하지 않을 경우 rdsconfig.ini 파일의 정보를 수정 혹은 추가
    ##########################################################################################

    * 로컬에서 테스트 하실 경우 항상 local 인자를 True로 설정해주셔야해요.
    * local 인자를 True로 설정하지 않을 경우 tunnel을 생성하지 않기 때문에
    * RDS에 접속할 수 없습니다.
    * 그리고 local이 True일 경우에만 tunnel을 생성합니다.
    * tunnel을 생성하는 이유는 local에서 테스트를 진행할 경우
    * RDS에 접속하기 위해서는 SSH 터널링을 통해야 하기 때문입니다.

    * 또한 secret_info 를 추가한 경우는 현재 secret manager에 14버전이 등록이 안되어있어서
    * 하드코딩으로 넣어주기 위해 추가하였습니다.
    * secret_info는 dict 형태로 넣어주셔야해요.
    * secret_info = {
    *    "host": "host",
    *    "port": "port",
    *    "username": "user",
    *    "password": "password",
    *    "dbname": "dbname"
    * }
    * dict 형태로 넣어주셔야해요.
    """


    def __init__(self,
                 secrets_manager_id: str or None = None,
                 secret: str or None = None,
                 secret_info: dict or None = None,
                 remote_rds: str or None = None,
                 db_name: str or None = None,
                 ):
        """
        RDS Connection을 생성하는 클래스의 생성자
        @param secrets_manager_id:
        """
        if secrets_manager_id:
            self.secret = secrets_manager_id
        if secret:
            self.secret = secret

        if not self.secret:
            self.secret = 'CB-PRD-SM-TRT-MWAA-DB'
        # local이 True일 경우 local에서 테스트를 진행한다.
        # local이 False일 경우 secret manager에서 secret을 가져온다.

        self.secret_info: dict or None = secret_info
        # local이 True일 경우 tunnel을 생성
        # tunnel을 생성하는 이유는 local에서 테스트를 진행할 경우
        self.tunnel: SSHTunnelForwarder or None = None
        # remote_rds는 rdsconfig.ini 파일에 있는 remote_rds의 key 값
        self.remote_rds: str or None = remote_rds
        self.db_name: str or None = db_name


        if self.secret:
            self.connection_info = self.connection_info()

        # 임의로 접속정보를 넣을 경우 secret_info를 넣어준다.
        # secret_info는 dict 형태로 넣어준다.
        # secret_info = {
        #    "host": "host",
        #    "port": "port",
        #    "username": "user",
        #    "password": "password",
        #    "dbname": "dbname"
        # }
        if self.secret_info:
            self.connection_info = secret_info
        self.connection: psycopg2.connect = self.connect()
        pass

    def __del__(self):
        """
        RDS Connection을 생성하는 클래스의 소멸자
        @return:
        """
        if self.connection:
            self.disconnect()
        pass

    def connection_info(self) -> dict:
        """
        secret manager에서 secret을 가져오는 함수
        @return: dict
        """
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/secretsmanager.html#SecretsManager.Client.get_secret_value
        secretmanager = boto3.client('secretsmanager', region_name="ap-northeast-2")
        print(f"---------------Secret Manager Connect / {KstTime().get_kst_now()}---------------")
        secret = secretmanager.get_secret_value(SecretId=self.secret)
        print(f"---------------Secret Manager Get Secret Value / {KstTime().get_kst_now()}---------------")
        return json.loads(secret['SecretString'])

    def connect(self) -> psycopg2.connect:
        """
        rds connection을 생성하는 함수
        @return: psycopg2.connect
        """

        host = self.connection_info['host']
        port = self.connection_info['port']
        dbname = self.db_name if self.db_name else 'ptcbtrtdb'
        user = self.connection_info['username']
        password = self.connection_info['password']
        # connect
        # https://www.psycopg.org/docs/module.html
        # https://www.psycopg.org/docs/usage.html#connection-objects
        try:
            self.connection = psycopg2.connect(host=host, port=port, database=dbname, user=user, password=password, connect_timeout=60)
            print(f"---------------Connection to RDS Postgres instance succeeded / {KstTime().get_kst_now()}---------------")
            print(f"---------------RDS Postgres version: {self.connection.get_parameter_status('server_version')} / {KstTime().get_kst_now()}---------------")
            return self.connection
        except Exception as e:
            if self.connection:
                self.connection.close()
                print(f"---------------Connection to RDS Postgres instance closed---------------")
            raise e

    def disconnect(self):
        """
        rds connection을 종료하는 함수
        @return: None
        """
        # https://www.psycopg.org/docs/connection.html#connection.close
        if self.connection is not None and self.connection.closed == 0:
            self.connection.close()
            print(f"---------------Connection to RDS Postgres instance closed---------------")
        if self.tunnel is not None and self.tunnel.is_active:
            self.tunnel.stop()
            print(f"---------------SSH Tunnel Stop---------------")

    def simple_query(self,
                     query: str,
                     param: tuple or None = None,
                     fetch_type: None or str or int = None) ->  None or list or tuple:
        """
        query 실행 함수
        간단하게 쿼리를 실행할 수 있도록 생성한 함수
        @param param:
        @param query: 실행할 쿼리
        @param fetch_type: 'all' | 'one' | int | None
        @return:
        """
        # fetch_type이 str이고 'all' 또는 'one'이 아니면 예외 발생
        # fetch_type이 int이고 1보다 작으면 예외 발생
        if type(fetch_type) == str and fetch_type not in ['all', 'one']:
            raise Exception(f"fetch_type must be 'all' or 'one' or int or None")
        if type(fetch_type) == int and fetch_type < 1:
            raise Exception(f"fetch_type must be greater than 0")
        query = query.strip()
        print(f"---------------Query Type: {query.split(' ')[0].upper()} / {KstTime().get_kst_now()}---------------")
        print(f"---------------{KstTime().get_kst_now()}---------------")

        print(f"---------------Param: {param} / {KstTime().get_kst_now()}---------------")
        print(f"---------------Fetch Type: {fetch_type} / {KstTime().get_kst_now()}---------------")


        # https://www.psycopg.org/docs/cursor.html#cursor
        # https://www.psycopg.org/docs/cursor.html#cursor.fetchmany
        # https://www.psycopg.org/docs/cursor.html#cursor.fetchone
        # https://www.psycopg.org/docs/cursor.html#cursor.fetchall

        # connection이 없으면 connection을 생성
        if self.connection is None:
            self.connection = self.connect()
        # cursor 생성
        cursor = self.connection.cursor()
        print(f"---------------Cursor Created / {KstTime().get_kst_now()}---------------")
        # query 실행
        if param is None:
            cursor.execute(query)
            print(f"---------------Query Executed / {KstTime().get_kst_now()}---------------")
        else:
            cursor.execute(query, param)
            print(f"---------------Query Executed / {KstTime().get_kst_now()}---------------")

        result = None
        # fetch_type에 따라 fetch
        if type(fetch_type)==str and fetch_type=='all':
            result = cursor.fetchall()
        elif type(fetch_type)==str and fetch_type=='one':
            result = cursor.fetchone()
        elif type(fetch_type) == int:
            result = cursor.fetchmany(fetch_type)

        if result is not None:
            print(f"---------------Fetch Result: {len(result)} / {KstTime().get_kst_now()}---------------")

        # fetch_type이 None이면 fetch하지 않음
        cursor.close()
        print(f"---------------Cursor Closed / {KstTime().get_kst_now()}---------------")
        return result

    def engine(self) -> sqlalchemy.engine.Engine:
        from sqlalchemy import create_engine
        host = self.connection_info['host']
        port = self.connection_info['port']
        dbname = self.db_name if self.db_name else self.connection_info.get('dbname', 'ptcbtrtdb')
        user = self.connection_info['username']
        password = self.connection_info['password']
        engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{dbname}")
        return engine

    def __enter__(self):
        """
        with 구문을 사용하기 위한 함수
        @return: psycopg2.connect
        """
        try:
            self.connection = self.connect()
            return self
        except Exception as e:
            self.__exit__()
            raise e

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        with 구문을 사용하기 위한 함수
        @return:
        """
        # self.disconnect()
        if exc_type is not None:
            print(f"---------------Exception Type: {exc_type} / {KstTime().get_kst_now()}---------------")
            print(f"---------------Exception Value: {exc_val} / {KstTime().get_kst_now()}---------------")
            print(f"---------------Exception Traceback: {exc_tb} / {KstTime().get_kst_now()}---------------")
            print(f"---------------Exception Occurred / {KstTime().get_kst_now()}---------------")
            return False
        else:
            print(f"---------------No Exception / {KstTime().get_kst_now()}---------------")
            return True

