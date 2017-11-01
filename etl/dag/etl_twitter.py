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

dag = DAG('etl_social', default_args=default_args, schedule_interval=None)


extract = BashOperator(
                        task_id='extract',
                        bash_command='cd ~/ETL_twiiter; export PYTHONPATH=.; python etl/task/extract.py -i ~/twitter_dados/input_twitter/ -o ~/twitter_dados/clean_tweets/',
                        dag=dag
)

tag_sentiment = BashOperator(
                        task_id='tag_sentiment',
                        bash_command='cd ~/ETL_twiiter; export PYTHONPATH=.; python etl/task/tag_sentiments.py -i ~/twitter_dados/clean_tweets/ -o ~/twitter_dados/tag_sentiments/ -cl etl/data/class_nb.bin',
                        dag=dag

)


indexes = BashOperator(
                        task_id='indexer',
                        bash_command='cd ~/ETL_twiiter; export PYTHONPATH=.; python etl/task/indexer.py -i ~/twitter_dados/tag_sentiments/ -id tweets_01 -es search-tamoios-es-mwuewcbovzuktgu5lfy52spm7u.us-west-2.es.amazonaws.com',
                        dag=dag
)

apaga_dados = BashOperator(
                        task_id='apaga_dados',
                        bash_command='rm ~/twitter_dados/input_twitter/*; rm ~/twitter_dados/clean_tweets/*; rm ~/twitter_dados/tag_sentiments/*'
)

tag_sentiment.set_upstream(extract)
indexes.set_upstream(tag_sentiment)
apaga_dados.set_upstream(indexes)