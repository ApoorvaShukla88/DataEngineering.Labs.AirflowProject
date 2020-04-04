from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd



#
# def read_file(**kwargs):
#     data = pd.read_csv('/Users/amishra/DEV/DataEngineering.Labs.AirflowProject/DataEngg-Airflow/USvideos.csv', index_col=0)
#     return data
#

def create_df():
    data = pd.read_csv('/Users/amishra/DEV/DataEngineering.Labs.AirflowProject/DataEngg-Airflow/USvideos.csv',
                       index_col=0)
    df = pd.DataFrame(data)
    print(df)


default_args = {
    'owner': 'Apoorva',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': None,
}

dag = DAG(
    'custom_dag',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    description='Custom DAG ',
)

download_operator = BashOperator(
    task_id='download_task',
    bash_command='kaggle datasets download datasnaek/youtube-new -p /Users/amishra/DEV/DataEngineering.Labs.AirflowProject/DataEngg-Airflow',
    dag=dag,
)

unzip_operator = BashOperator(
    task_id='unzip_downloaded_files',
    bash_command='unzip /Users/amishra/DEV/DataEngineering.Labs.AirflowProject/DataEngg-Airflow/youtube-new.zip',
    dag=dag,
)


# read_operator = PythonOperator(
#     task_id='read_task',
#     provide_context=True,
#     python_callable=read_file,
#     dag=dag,
# )

create_df_op = PythonOperator(
    task_id='create_Dataframe',
    provide_context=True,
    python_callable=create_df,
    dag=dag,
)


download_operator >> unzip_operator >> create_df_op
