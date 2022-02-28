import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import SparkSession, Window

spark = SparkSession.builder.\
        appName("pyspark-notebook2").\mongodb://admin:admin@127.0.0.1:27017/test.demographi
        config("spark.mongodb.input.uri","c").\
        config("spark.mongodb.output.uri","mongodb://admin:admin@127.0.0.1:27017/test.demographic").\
        config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0").\
        getOrCreate()

date = '2021-11-03'
# date = input("enter date: ")
des_user = "../PysparkMongoAirflow/Final_Project/data/datalake/"+date+"/user/*.parquet"

users = spark.read.parquet(des_user).withColumn("dob", to_date('birthdate', "yyyy-M-dd")). \
        withColumn('updated_time',  to_timestamp('updatedTime', "yyyy-MM-dd HH:mm:ss.SSS")) 

windowSpec  = Window.partitionBy("userid").orderBy(col('updated_time').desc())
data = users.withColumn('row', row_number().over(windowSpec)).filter(col("row") == 1) \
            .select('userid', 'dob', 'profileLevel', 'gender', 'updated_time').orderBy('userid') \
            .cache()
users_db = spark.read\
        .format("mongo")\
        .option("uri","mongodb://admin:admin@127.0.0.1:27017/test")\
        .option("database", 'test') \
        .option("collection", "demographic") \
        .load().cache()
database_count = users_db.count()
if database_count == 0:
        print('count = 0')
        data.write.format("mongo")\
            .option("uri","mongodb://admin:admin@127.0.0.1:27017/test")\
            .option("database", 'test').mode("overwrite")\
            .option("collection", "demographic").save()
else: 
        users_db = users_db.select('userid', 'dob', 'profileLevel', 'gender', 'updated_time').cache()
        all_data = data.unionAll(users_db).cache()
        final_result =all_data.withColumn('row', row_number().over(windowSpec)).filter(col("row") == 1) \
            .select('userid', 'dob', 'profileLevel', 'gender', 'updated_time').orderBy('userid') \
            .cache()
        print(final_result.count())
        final_result.write.format("mongo")\
        .option("uri","mongodb://admin:admin@127.0.0.1:27017/test")\
        .option("database", 'test').mode("overwrite")\
        .option("collection", "demographic").save()
spark.stop()