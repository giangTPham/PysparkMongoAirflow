import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession, Window, DataFrame
from functools import reduce

def updateToDB(date):
    spark = SparkSession.builder.master("local[1]").\
        appName("activity").\
        config("spark.mongodb.input.uri","mongodb://admin:admin@127.0.0.1:27017/test.activities").\
        config("spark.mongodb.output.uri","mongodb://admin:admin@127.0.0.1:27017/test.activities").\
        config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0").\
        getOrCreate()
    transaction = spark.read.option("header", True)\
                .option("inferSchema", True)\
                .parquet('/workspace/PysparkMongoAirflow/Final_Project/data/datalake/'+date+'/transaction/*.parquet')
    transaction = transaction.filter(transaction['transStatus'] == 1)
    transaction = transaction.withColumn('transactionTime', to_timestamp('transactionTime'))
    transaction = transaction.select('userId', 'transactionTime', 'appId', 'transType', 'amount', 'pmcId')
    window = Window.partitionBy('userId').orderBy("transactionTime").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    transaction_activities = transaction.withColumn('firstPaymentDate', when(col('transType')==3, to_date(col('transactionTime'))).otherwise(None))\
                                .withColumn('lastPaymentDate', when(col('transType')==3, to_date(col('transactionTime'))).otherwise(None))\
                                .withColumn('firstActiveDate', to_date('transactionTime'))\
                                .withColumn('lastActiveDate', to_date('transactionTime'))\
                                .withColumn('modifiedDate', to_date(col('transactionTime')))
    transaction_activities = transaction_activities.withColumn('firstPaymentDate', min('firstPaymentDate').over(window))\
                                            .withColumn('lastPaymentDate', max('lastPaymentDate').over(window))
    transaction_activities = transaction_activities.withColumn("lastActiveTransactionType", last("transType").over(window))
    payment = transaction.filter(col("transType") == 3).groupBy('userId').agg(last('appId').alias('lastPayAppId'))
    transaction_activities = transaction_activities.join(payment, 'userId', 'outer')
    transaction_activities = transaction_activities.withColumn('appIds',collect_set("appId").over(window))                  
    paymentPmc = transaction.filter(col("transType") == 3).groupBy('userId').agg(collect_set(col('pmcId')).alias('payPmcIds'))
    transaction_activities = transaction_activities.join(paymentPmc, 'userId', 'outer')
    transaction_activities = transaction_activities.select('userId', 'firstPaymentDate','lastPaymentDate', 'firstActiveDate',\
        'lastActiveDate', 'appIds', 'lastActiveTransactionType', 'lastPayAppId', 'payPmcIds', 'modifiedDate').dropDuplicates() 

    userIds = str([int(row.userId) for row in transaction_activities.select("userId").collect()])
    pipeline = "{$match: {'userId': {$in:" + userIds + "}}}"

    schema_for_read = StructType([
        StructField("userId", IntegerType()),
        StructField("firstPaymentDate", TimestampType()),
        StructField("lastPaymentDate", TimestampType()),
        StructField("firstActiveDate", TimestampType()),
        StructField("lastActiveDate", TimestampType()),
        StructField("appIds", ArrayType(IntegerType())),
        StructField("lastActiveTransactionType", IntegerType()),
        StructField("lastPayAppId", IntegerType()),
        StructField("payPmcIds", ArrayType(IntegerType())),
        StructField("modifiedDate", TimestampType())
    ])
    
    full_activities = spark.read\
        .option("pipeline", pipeline)\
        .schema(schema_for_read)\
        .format("mongo")\
        .load()

    database_count = full_activities.count()
    if database_count == 0:
        transaction_activities.withColumn("_id", col('userId')).write.format("mongo")\
            .option("uri","mongodb://admin:admin@127.0.0.1:27017/test")\
            .option("database", 'test').mode("append")\
            .option("collection", "activities").save()
        return
    full_activities = full_activities.select('userId', 'firstPaymentDate','lastPaymentDate', 'firstActiveDate',\
     'lastActiveDate', 'appIds', 'lastActiveTransactionType', 'lastPayAppId', 'payPmcIds', 'modifiedDate')
    full_activities = full_activities.withColumn('firstPaymentDate', col('firstPaymentDate').cast('date'))\
                        .withColumn('firstActiveDate', col('firstActiveDate').cast('date'))\
                        .withColumn('lastActiveDate', col('lastActiveDate').cast('date'))\
                        .withColumn('lastPaymentDate', col('lastPaymentDate').cast('date'))\
                        .withColumn('modifiedDate', col('modifiedDate').cast('date')).cache()

    # filter from full_activities the user_id with modifiedDate greater than date
    from datetime import datetime
    date = datetime.strptime(date, "%Y-%m-%d").date()
    no_update_user = full_activities.filter(col("modifiedDate") > date).select('userId').cache()
    # filter it out of the transaction_activities
    if no_update_user.count() != 0:
        transaction_activities = transaction_activities.join(no_update_user, ['userId'], 'leftanti').cache()


    # continue
    combination = full_activities.union(transaction_activities).cache()
    window_combine = Window.partitionBy('userId').orderBy("lastActiveTransactionType").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    combination = combination.withColumn('lastActiveTransactionType', last('lastActiveTransactionType', True).over(window_combine))\
                        .withColumn('lastPayAppId', last('lastPayAppId', True).over(window_combine)).cache()
    flatten = udf(lambda l: list(set([x for i in l for x in i])), ArrayType(IntegerType()))
    combination = combination.groupBy('userId').agg(min('firstPaymentDate').alias('firstPaymentDate')\
                                            ,max('lastPaymentDate').alias('lastPaymentDate')\
                                            ,min('firstActiveDate').alias('firstActiveDate')\
                                            ,max('lastActiveDate').alias('lastActiveDate')\
                                            ,flatten(collect_set('appIds')).alias('appIds')\
                                            ,first('lastActiveTransactionType').alias('lastActiveTransactionType')\
                                            ,first('lastPayAppId').alias('lastPayAppId')\
                                            ,flatten(collect_set('payPmcIds')).alias('payPmcIds')\
                                            ,max('modifiedDate').alias('modifiedDate')).cache()
    combination = combination.withColumn("_id", col("userId")).cache()
    combination.write.format("mongo")\
    .option("uri","mongodb://admin:admin@127.0.0.1:27017/test")\
    .option("database", 'test').mode("append")\
    .option("collection", "activities").save()

    spark.stop()

if __name__ == '__main__':
    date = sys.argv[1]
    updateToDB(date)
