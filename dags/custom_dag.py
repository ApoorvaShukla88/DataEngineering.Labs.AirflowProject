from datetime import timedelta
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd





data = pd.read_csv('/Users/amishra/Downloads/youtube-new/FRvideos.csv', index_col=0)

def create_df():
    df = pd.DataFrame(data)
    print(df)


default_args = {
    'owner': 'Apoorva',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'custom_dag',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    description='Custom DAG ',
)

read_operator = PythonOperator(
    task_id='read_task',
    provide_context=True,
    python_callable=data,
    dag=dag,
)


create_df_op = PythonOperator(
    task_id='create_Dataframe',
    provide_context=True,
    python_callable=create_df,
    dag=dag,
)








