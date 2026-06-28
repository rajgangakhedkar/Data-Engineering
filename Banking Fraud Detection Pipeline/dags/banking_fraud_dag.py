"""
Airflow DAG for Banking Fraud Detection Pipeline.

This DAG orchestrates Snowflake stored procedures that:
1. Load raw banking files from external stages into Snowflake raw tables.
2. Curate valid transaction and login records.
3. Capture invalid/rejected records into fraud flag tables.
4. Detect fraud patterns such as unknown devices, repeated failed logins,
   and impossible travel.
"""

from airflow.sdk import dag, task
from pendulum import datetime
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator #pyright: ignore[reportMissingImports]

@dag(
    dag_id='banking',
    schedule='@daily',
    start_date=datetime(year=2026,month=4,day=22),
    catchup=False
)

def banking():
    
    coping = SQLExecuteQueryOperator(
        conn_id = 'snow_conn',
        task_id = 'coping',
        sql='CALL banking_data.sp.coping();',
        autocommit = True
    )

    transactions_00 = SQLExecuteQueryOperator(
        conn_id = 'snow_conn',
        task_id = 'transactions_00',
        sql='CALL banking_data.sp.transactions_00();',
        autocommit = True
    )

    transactions_01 = SQLExecuteQueryOperator(
        conn_id = 'snow_conn',
        task_id = 'transactions_01',
        sql='CALL banking_data.sp.transactions_01();',
        autocommit = True
    )

    transactions_02 = SQLExecuteQueryOperator(
        conn_id = 'snow_conn',
        task_id = 'transactions_02',
        sql='CALL banking_data.sp.transactions_02();',
        autocommit = True
    )

    logins_00 = SQLExecuteQueryOperator(
        conn_id = 'snow_conn',
        task_id = 'logins_00',
        sql='CALL banking_data.sp.logins_00();',
        autocommit = True
    )

    logins_01 = SQLExecuteQueryOperator(
        conn_id = 'snow_conn',
        task_id = 'logins_01',
        sql='CALL banking_data.sp.logins_01();',
        autocommit = True
    )

    logins_02 = SQLExecuteQueryOperator(
        conn_id = 'snow_conn',
        task_id = 'logins_02',
        sql='CALL banking_data.sp.logins_02();',
        autocommit = True
    )

    logins_03 = SQLExecuteQueryOperator(
        conn_id = 'snow_conn',
        task_id = 'logins_03',
        sql='CALL banking_data.sp.logins_03();',
        autocommit = True
    )

    logins_04 = SQLExecuteQueryOperator(
        conn_id = 'snow_conn',
        task_id = 'logins_04',
        sql='CALL banking_data.sp.logins_04();',
        autocommit = True
    )

    logins_05 = SQLExecuteQueryOperator(
        conn_id = 'snow_conn',
        task_id = 'logins_05',
        sql='CALL banking_data.sp.logins_05();',
        autocommit = True
    )

    logins_06 = SQLExecuteQueryOperator(
        conn_id = 'snow_conn',
        task_id = 'logins_06',
        sql='CALL banking_data.sp.logins_06();',
        autocommit = True
    )

    coping>>transactions_00>>transactions_01>>transactions_02>>logins_00>>logins_01>>logins_02>>logins_03>>logins_04>>logins_05>>logins_06

banking()