from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'Airflow',
}

with DAG(
    dag_id='example_spark_operator',
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['test'],
) as dag:
    # [START howto_operator_spark_submit]
    
    python_submit_job = SparkSubmitOperator(
        application="/workspace/PysparkMongoAirflow/utils/demographicToDB.py", task_id="demo_job", packages = "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"
    )

    python_submit_job 
    