from airflow.decorators import dag
from airflow.decorators import task
from airflow.models import Variable
from airflow.hooks.base import BaseHook
import vertica_python
import pendulum
import boto3

AWS_ACCESS_KEY_ID = Variable.get('secret_AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = Variable.get('secret_AWS_SECRET_ACCESS_KEY')
endpoint_url = Variable.get('endpoint_url')

vertica_conn = BaseHook.get_connection('vertica_conn')
conn_info = {
    'host': vertica_conn.host,
    'port': vertica_conn.port,
    'user': vertica_conn.login,
    'password': vertica_conn.password,
    'database': vertica_conn.schema,
    'autocommit': True
    }

@dag(
    schedule_interval = None,
    start_date = pendulum.datetime(2025, 5, 1, tz = "UTC"),
    catchup = False,
    is_paused_upon_creation = True
    )
def group_log_fetch_dag():

    # загружаем файл из S3 в локальную директорию
    @task()
    def load_from_s3(**kwargs):

        session = boto3.session.Session()
        s3_client = session.client(
            service_name = 's3',
            endpoint_url = endpoint_url,
            aws_access_key_id = AWS_ACCESS_KEY_ID,
            aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
        )

        s3_client.download_file(
            Bucket = 'bucket_name',
            Key = 'group_log.csv',
            Filename = '/data/group_log.csv'
        )

        # выводим 10 строк файла в логи Airflow
        with open('/data/group_log.csv', 'r') as f:
            for _ in range(10):
                print(f.readline())

    # загружаем данные из файла в STG-слой DWH (Vertica)
    @task()
    def load_to_stg(**kwargs):

        with vertica_python.connect(**conn_info) as conn:
            cur = conn.cursor()
            cur.execute("""
                        COPY STG.group_log (group_id, user_id, event, datetime)
                        FROM LOCAL '/data/group_log.csv'
                        DELIMITER ','
                        NULL AS ''
                        SKIP 1
                        REJECTED DATA AS TABLE STG.group_log_rej;
                        """)

    load_from_s3 = load_from_s3()
    load_to_stg = load_to_stg()

group_log_fetch_dag = group_log_fetch_dag()