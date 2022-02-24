
import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import SparkSession, Window
import numpy as np
from delta.tables import *
spark = SparkSession.builder.\
        appName("pyspark-notebook2").\
        config("spark.mongodb.input.uri","mongodb://admin:admin@127.0.0.1:27017/test.demographic").\
        config("spark.mongodb.output.uri","mongodb://admin:admin@127.0.0.1:27017/test.demographic").\
        config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0").\
        getOrCreate()
date = '2021-11-01'
# date = input("enter date: ")
des_user = "../PysparkMongoAirflow/Final_Project/data/datalake/"+date+"/user/*.parquet"
users = spark.read.parquet(des_user).withColumn("dob", to_date('birthdate', "yyyy-M-dd")). \
        withColumn('updated_time',  to_timestamp('updatedTime', "yyyy-MM-dd HH:mm:ss.SSS")) 

windowSpec  = Window.partitionBy("userid").orderBy(col('updated_time').desc())
data = users.withColumn('row', row_number().over(windowSpec)).filter(col("row") == 1) \
            .select('userid', 'dob', 'profileLevel', 'gender', 'updated_time').orderBy('userid') \
            .cache()

# userids =np.array_str(np.array(data.select('userid').collect()).reshape(-1))#[6:-1]
# # pipeline = "{'$in': {'userid', [" + userids  +"] }}"
# # print(pipeline)
# print(userids)