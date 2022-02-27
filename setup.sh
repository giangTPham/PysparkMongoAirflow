# Airflow needs a home. `~/airflow` is the default, but you can put it
# somewhere else if you prefer (optional)
export AIRFLOW_HOME=~/airflow

# Install Airflow using the constraints file
AIRFLOW_VERSION=2.2.4
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
# For example: 3.6
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
# For example: https://raw.githubusercontent.com/apache/airflow/constraints-2.2.4/constraints-3.6.txt
pip3 install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

pip install apache-airflow-providers-apache-spark

# initialize the database
airflow db init

airflow users create \
    --username admin \
    --role Admin \
    --firstname Peter \
    --lastname Parker \
    --email anhptvhe150038@fpt.edu.vn
# start the web server, default port is 8080
airflow webserver --port 8088 -D

# start scheduler
airflow scheduler