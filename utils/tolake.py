import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession, Window

import click
import sys

def main():
    spark = SparkSession.builder.master("local[1]").appName("finalProject-sp-af").getOrCreate()
    date = sys.argv[1]
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



if __name__ == '__main__':
    main()