# Airflow and Spark

Learn how to use Airflow to schedule and run Spark jobs.

We will be using [Gitpod](https://www.gitpod.io/) as our dev environment so that you can quickly learn and test without having to worry about OS inconsistencies. If you have not already opened this in gitpod, then `CTR + Click` the button below and get started! <br></br>
[![Open in Gitpod](https://gitpod.io/button/open-in-gitpod.svg)](https://gitpod.io/#https://github.com/giangTPham/PysparkMongoAirflow) 

## 1. Set up Airflow and MongoDB

### 1.1 - Start Airflow
We will be using the quick start script that Airflow provides [here](https://airflow.apache.org/docs/apache-airflow/stable/start/local.html).

```bash
bash setup.sh
```

### 1.2 - Start MongoDB

Start by installing Docker extension from Gitpod with user interface similar to Visual Studio Code. Then:

```bash
cd mongodb-docker
docker-compose up -d
cd ..
```

## 2. Start Spark in standalone mode

### 2.1 - Start master

```bash
./spark-3.1.1-bin-hadoop2.7/sbin/start-master.sh
```
Check port 8080 to get master URL

### 2.2 - Start worker

Open port 8081 in the browser, copy the master URL, and paste in the designated spot below

```bash
./spark-3.1.1-bin-hadoop2.7/sbin/start-worker.sh <master-URL>
```
## 3. Add user to Postgres:

```bash
psql
postgres=$ CREATE USER airflow PASSWORD 'airflow';
postgres=$ CREATE DATABASE airflow;
postgres=$ GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow;

```

`Ctrl D` to finish.

## 4. Reconfig Airflow Executor and Database

### 4.1 - Update airflow.cfg

Access and modify airflow.cfg

```bash
cd ~/airflow
vi airflow.cfg
```
Locate and change `executor` and `sql_alchemy_conn` value by pressing `Esc` then `I`:

```bash
executor = LocalExecutor
sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@localhost:5432/airflow
```

pressing `Esc` then `:wq!` to save.

### 4.2 - Re-initialize database

Kill current running port

```bash
npx kill-port 8088
npx kill-port 8793
```

Initialize database, add user, start airflow web UI and scheduler

```bash
# --initialize the airflow database--
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
airflow scheduler -D
```


## 5. Move spark_dag.py to ~/airflow/dags

### 5.1 - Create ~/airflow/dags

```bash
mkdir ~/airflow/dags
```

### 5.2 - Move spark_dag.py

```bash
mv spark_dag.py ~/airflow/dags
```

## 6. Open port 8080 to see Airflow UI and check if `example_spark_operator` exists. 
If it does not exist yet, give it a few seconds to refresh.

## 7. Spark Configuration

### 7.1 - Update spark_default 
Under the `Admin` section of the menu, select `spark_default` and update the host to the Spark master URL. Save once done

### 7.2 - Turn on DAG
Select the `DAG` menu item and return to the dashboard. Unpause the `example_spark_operator`, and then click on the `example_spark_operator`link. 

## 8. Trigger the DAG 
Trigger from the tree view and click on the graph view afterwards

## 9. Review Logs
Once the jobs have run, you can click on each task in the graph view and see their logs. In their logs, we should see value of Pi that each job calculated, and the two numbers differing between Python and Scala

## 10. Trigger DAG from command line

### 10.1 - Open a new terminal and run `airflow dags`

```bash
airflow dags trigger example_spark_operator
```

### 10.2 - If we want to trigger only one task

```bash
airflow tasks run example_spark_operator python_submit_job now
```

And that wraps up our basic walkthrough on using Airflow to schedule Spark jobs.
