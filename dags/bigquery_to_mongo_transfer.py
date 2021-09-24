import airflow
from airflow import DAG
from datetime import timedelta

from mongo_plugin import BigQueryToMongoDB

default_args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2)
}

dag = DAG(
    dag_id='bq_to_mongo_transfer',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    dagrun_timeout=timedelta(minutes=60),
    tags=['BigQuery', 'MongoDB']
)

BQ_TO_MONGO = BigQueryToMongoDB(
    dag=dag,
    task_id='bq_to_mongo',
    gcp_conn_id = 'google_cloud_default',
    bq_dataset_id = '<my-data-set>',
    bq_table_id = '<my-table-id>',
    bq_selected_fields= None,
    bq_next_index_variable= 'bq_to_mongo_next_index',
    max_results = 1000,
    mongo_conn_id = 'mongo_default',
    mongo_db = '<my-database>',
    mongo_collection = '<my-collections>',
    mongo_method = 'insert',
    mongo_replacement_filter = '<query-filter>',
    mongo_upsert = True
)
