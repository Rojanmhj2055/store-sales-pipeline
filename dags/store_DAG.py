from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators import BashOperator
from airflow.operators import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.email_operator import EmailOperator
from datacleaner import data_cleaner

yesterday_date = datetime.strftime(datetime.now()-timedelta(1),'%Y-%m-%d')
default_args ={
    'owner': 'Rojan',
    'start_date':datetime(2023,1,4),
    'retries':1,
    'retry_delay': timedelta(seconds=5)
    
}

"""
we need our daily reporting so schedult_interval = @daily
Catchup
When the catchup parameter for a DAG is set to True, at the time the DAG is turned on
in Airflow the scheduler starts a DAG run for every data interval that has not been run
between the DAG's start_date and the current data interval. For example, if your DAG is 
scheduled to run daily and has a start_date of 1/1/2021, 
and you deploy that DAG and turn it on 2/1/2021,
Airflow will schedule and start all of the daily DAG runs for January.
Catchup is also triggered when you turn a DAG off for a period and then turn it on again.
"""
dag = DAG('store_DAG',default_args=default_args,schedule_interval='@daily',template_searchpath=['/usr/local/airflow/sql_files'],catchup=False)

##TASK 1 - check if we have the file or not]

t1 = BashOperator(task_id="check_file_exists",bash_command='shasum ~/store_files_airflow/raw_store_transactions.csv',retries=2,
                  retry_delay=timedelta(seconds=15),dag=dag)

## TASK 2 - Cleaned the data using pandas
t2 = PythonOperator(task_id="clean_raw_csv",python_callable=data_cleaner,dag=dag)

## TASK 3 - Create a sql table
t3 = MySqlOperator(task_id="create_mysql_table",mysql_conn_id="mysql_conn",sql="create_table.sql",dag=dag)

## TASK 4 - load the sql table 
t4 = MySqlOperator(task_id="insert_into_table",mysql_conn_id="mysql_conn",sql="insert_into_table.sql",dag=dag)

## TASK 5 - GROUP BY
t5 = MySqlOperator(task_id="select_from_table",mysql_conn_id="mysql_conn",sql="select_from_table.sql",dag=dag)

t6 = BashOperator(task_id='move_file1', bash_command='cat ~/store_files_airflow/location_wise_profit.csv && mv ~/store_files_airflow/location_wise_profit.csv ~/store_files_airflow/location_wise_profit_%s.csv' % yesterday_date,dag=dag)

t7 = BashOperator(task_id='move_file2', bash_command='cat ~/store_files_airflow/store_wise_profit.csv && mv ~/store_files_airflow/store_wise_profit.csv ~/store_files_airflow/store_wise_profit_%s.csv' % yesterday_date,dag=dag)

t8 = EmailOperator(task_id="send_Email", 
                   to='rojanmhj@gmail.com',
                   subject="Daily report generated",
                   html_content="""<h1>Congratulations! your store report is here</h1>""",
                   files=['/usr/local/airflow/store_files_airflow/location_wise_profit_%s.csv' % yesterday_date, '/usr/local/airflow/store_files_airflow/store_wise_profit_%s.csv' % yesterday_date],
                   dag=dag)

t9 = BashOperator(task_id='rename_raw', bash_command='mv ~/store_files_airflow/raw_store_transactions.csv ~/store_files_airflow/raw_store_transactions_%s.csv' % yesterday_date,dag=dag)

t1 >> t2 >> t3 >> t4 >> t5 >> [t6,t7] >> t8 >>t9