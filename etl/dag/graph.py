from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator




default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 6, 1),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('etl_social', default_args=default_args)


extract = BashOperator(
                        task_id='extract',
                        bash_command='echo "teste"',
                        dag=dag
)

tag_profission = BashOperator(
                        task_id='tag_profission',
                        bash_command='echo "teste"',
                        dag=dag

)

tag_sentiment = BashOperator(
                        task_id='tag_sentiment',
                        bash_command='echo "text"',
                        dag=dag
)

load = BashOperator(
                        task_id='load',
                        bash_command='echo "text"',
                        dag=dag
)

tag_profission.set_upstream(extract)
tag_sentiment.set_upstream(tag_profission)
load.set_upstream(tag_sentiment)