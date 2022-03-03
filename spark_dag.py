from airflow.models import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
import pendulum

args = {
    'owner': 'Airflow',
    "start_date": pendulum.datetime(2021, 11, 1, tz="UTC"),
}

with DAG(
    dag_id='example_spark_operator',
    default_args=args,
    schedule_interval="@daily",
    tags=['test'],
) as dag:
    # [START howto_operator_spark_submit]

    data_tolake_job = SparkSubmitOperator(
        application="workspace/PysparkMongoAirflow/utils/tolake.py", task_id="tolake_job",
        application_args=['{{ dag_run.conf["exec_date"] if dag_run.conf.get("exec_date") else ds}}']
    )

    config_tolake_job = SparkSubmitOperator(
        application="workspace/PysparkMongoAirflow/utils/configtolake.py", task_id="configtolake_job",
        application_args=['{{ dag_run.conf["exec_date"] if dag_run.conf.get("exec_date") else ds}}']
    )
    
    demographic_job = SparkSubmitOperator(
        application="/workspace/PysparkMongoAirflow/utils/demographicToDB.py", task_id="demo_job", 
        packages = "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1",
        application_args=['{{ dag_run.conf["exec_date"] if dag_run.conf.get("exec_date") else ds}}']
    )

    activities_job = SparkSubmitOperator(
        application="/workspace/PysparkMongoAirflow/utils/activityToDB.py", task_id="act_job", 
        packages = "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1",
        application_args=['{{ dag_run.conf["exec_date"] if dag_run.conf.get("exec_date") else ds}}']
    )

    promotions_job = SparkSubmitOperator(
        application="/workspace/PysparkMongoAirflow/utils/promotionToDB.py", task_id="promo_job", 
        packages = "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1",
        application_args=['{{ dag_run.conf["exec_date"] if dag_run.conf.get("exec_date") else ds}}']
    )

    data_tolake_job >> demographic_job
    data_tolake_job >> activities_job
    data_tolake_job >> config_tolake_job >> promotions_job
    