import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import SparkSession, Window
import logging

def updateToDB(date):
    spark = SparkSession.builder.master("local[1]").\
        appName("promotion").\
        config("spark.mongodb.input.uri","mongodb://admin:admin@127.0.0.1:27017/test.promotions").\
        config("spark.mongodb.output.uri","mongodb://admin:admin@127.0.0.1:27017/test.promotions").\
        config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0").\
        getOrCreate()

    promotion = spark.read.option("header", True)\
                .option("inferSchema", True)\
                .parquet('/workspace/PysparkMongoAirflow/Final_Project/data/datalake/'+date+'/promotion/*.parquet')

    config = spark.read.option("header", True)\
                .option("inferSchema", True)\
                .parquet('/workspace/PysparkMongoAirflow/Final_Project/data/datalake/configs/*.parquet')

    data = promotion.join(config, promotion['campaignID'] == config['campaignID'], 'left').drop(config['campaignID'])
    data = data.withColumn('getnow', current_timestamp())
    windowSpec  = Window.partitionBy("userid").orderBy(col('getnow').desc())
    data_up = data.withColumn('row', row_number().over(windowSpec)).filter(col("row") == 1) \
            .select('userid', 'voucherCode', 'status', 'campaignID', 'time', 'campaignType', 'expireDate', 'expireTime', 'getnow').orderBy('userid') \
            .cache()

    data_db = spark.read\
        .format("mongo")\
        .option("uri","mongodb://admin:admin@127.0.0.1:27017/test")\
        .option("database", 'test') \
        .option("collection", "promotions") \
        .load().cache()

    database_count = data_db.count()

    if database_count == 0:
        print('count = 0')
        data_up.write.format("mongo")\
            .option("uri","mongodb://admin:admin@127.0.0.1:27017/test")\
            .option("database", 'test').mode("overwrite")\
            .option("collection", "promotions").save()
    else: 
        data_db = data_db.select('userid', 'voucherCode', 'status', 'campaignID', 'time', 'campaignType', 'expireDate', 'expireTime', 'getnow').cache()
        all_data = data.unionAll(data_db).cache()
        final_result =all_data.withColumn('row', row_number().over(windowSpec)).filter(col("row") == 1) \
            .select('userid', 'voucherCode', 'status', 'campaignID', 'time', 'campaignType', 'expireDate', 'expireTime', 'getnow').orderBy('userid') \
            .cache()
        print(final_result.count())
        final_result.write.format("mongo")\
        .option("uri","mongodb://admin:admin@127.0.0.1:27017/test")\
        .option("database", 'test').mode("overwrite")\
        .option("collection", "promotions").save()
    
    spark.stop()


if __name__ == '__main__':
    date = sys.argv[1]
    updateToDB(date)
