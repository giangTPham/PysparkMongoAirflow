import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession, Window

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[1]").appName("finalProject-sp-af").getOrCreate()
    date = '2021-11-01'
    # date = input("enter date: ")
    source = "/workspace/PysparkMongoAirflow/Final_Project/data/"

    promotion = spark.read.option("header", True).option("inferSchema", True).option("delimiter", '\t') \
                        .csv(source + "source/promotions/"+date+"/*.csv").cache()
    transaction = spark.read.option("header", True).option("inferSchema", True).option("delimiter", '\t') \
                        .csv(source +"source/transactions/"+date+"/*.csv").cache()
    user = spark.read.option("header", True).option("inferSchema", True).option("delimiter", '\t') \
                        .csv(source +"source/users/"+date+"/*.csv").cache()

    promotion.write.format("parquet").mode('overwrite') \
                .save(source+"datalake/"+date+"/promotion")
    transaction.write.format("parquet").mode('overwrite') \
                .save(source+"datalake/"+date+"/transaction")
    user.write.format("parquet").mode('overwrite') \
                .save(source+"datalake/"+date+"/user")
    spark.stop()