import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession, Window, DataFrame
from functools import reduce
from datetime import datetime
from datetime import date
from dateutil.relativedelta import relativedelta

def time(campaignType, time_use, days, expireTime, expire_date):
    # datetime object containing current date and time
   
    if (campaignType == 1):
        if (expire_date < datetime.now()):
            return 'expired'
        else:
            return 'not_expire'
        
    elif (campaignType == 2):
        if (time_use + relativedelta(days=days)) < expire_date:
            return 'not_expire'
        else:
            if (expire_date < datetime.now()):
                return 'expired'
            else:
                return 'not_expire'

def updateToDB(date):
    spark = SparkSession.builder.\
        appName("promotion").\
        master("local[1]").\
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

    #question1
    # user_promo = data.filter(data['campaignID'] == 1000).select(data['userid'])
    # user_promo.show()

    data1 = data.filter(data['status'] == "GIVEN")

    df = data1.withColumn("expire_date",to_timestamp("expireDate"))\
        .drop(data1['expireDate']) \
        .withColumn('time_use', to_timestamp("time")) \
        .drop(data1['time'])

    df1 = df.withColumn('days', (col('expireTime') / (24*3600)).cast("Integer"))

    voucher = udf(lambda a,b,c,d,e: time(a,b,c,d,e), StringType())
    df_final = df1.withColumn("check_voucher",voucher(col('campaignType'), col('time_use'), col('days'), col('expireTime'), col('expire_date')))

    #question 2
    # op2 = df_final.filter(df_final['check_voucher'] == 'expired')

    #question 3
    # op3 = df_final.filter((df_final['campaignID'] == 1008) & (df_final['check_voucher'] == 'expired'))

    df_final.write.format("mongo")\
        .option("uri","mongodb://admin:admin@127.0.0.1:27017/test")\
        .option("database", 'test').mode("overwrite")\
        .option("collection", "promotions").save()

    windowSpec  = Window.partitionBy("userid").orderBy(col('time_use').desc())
    data_up = df_final.withColumn('row', row_number().over(windowSpec)).filter(col("row") == 1) \
            .select('userid', 'voucherCode', 'status', 'campaignID', 'campaignType', 'expire_date', 'expireTime', 'time_use', 'days', 'check_voucher').orderBy('userid') \
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
        data_db = data_db.select('userid', 'voucherCode', 'status', 'campaignID', 'campaignType', 'expire_date', 'expireTime', 'time_use', 'days', 'check_voucher').cache()
        all_data = data_up.unionAll(data_db).cache()
        final_result =all_data.withColumn('row', row_number().over(windowSpec)).filter(col("row") == 1) \
            .select('userid', 'voucherCode', 'status', 'campaignID', 'campaignType', 'expire_date', 'expireTime', 'time_use', 'days', 'check_voucher').orderBy('userid') \
            .cache()
        print(final_result.count())
        final_result.write.format("mongo")\
        .option("uri","mongodb://admin:admin@127.0.0.1:27017/test")\
        .option("database", 'test').mode("overwrite")\
        .option("collection", "promotions").save()
    
    spark.stop()


if __name__ == '__main__':
    # date = '2021-11-01'
    date = sys.argv[1]
    updateToDB(date)
