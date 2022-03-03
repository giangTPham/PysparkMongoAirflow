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

    data_tolake_job = SparkSubmitOperator(
        application="workspace/PysparkMongoAirflow/utils/tolake.py", task_id="tolake_job"
    )

    config_tolake_job = SparkSubmitOperator(
        application="workspace/PysparkMongoAirflow/utils/configtolake.py", task_id="configtolake_job"
    )
    
    demographic_job = SparkSubmitOperator(
        application="/workspace/PysparkMongoAirflow/utils/demographicToDB.py", task_id="demo_job", packages = "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"
    )

    activities_job = SparkSubmitOperator(
        application="/workspace/PysparkMongoAirflow/utils/activityToDB.py", task_id="act_job", packages = "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"
    )

    promotions_job = SparkSubmitOperator(
        application="/workspace/PysparkMongoAirflow/utils/promotionToDB.py", task_id="promo_job", packages = "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"
    )

    data_tolake_job >> config_tolake_job >> demographic_job >> activities_job >> promotions_job
    