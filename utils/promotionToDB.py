from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType


if __name__ == '__main__':

    spark = SparkSession \
        .builder \
        .appName("demographic") \
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.coll") \
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.coll") \
        .getOrCreate()

    schema = StructType([
        StructField("userid", IntegerType()),
        StructField("voucherCode", StringType()),
        StructField("status", StringType()),
        StructField("campaignID", IntegerType()),
        StructField("time", DateType()),
    ])

    df = spark.read.format('com.mongodb.spark.sql.DefaultSource').schema(schema).load()

    df.createOrReplaceTempView('promotion')

    
    
