import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession, Window

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[1]").appName("campaignfile").getOrCreate()
    source = "../PysparkMongoAirflow/Final_Project/data/"

    
    campaign = spark.read.option("header", True).option("inferSchema", True).option("delimiter", '\t').csv("/workspace/PysparkMongoAirflow/Final_Project/data/source/configs/campaign.csv").cache()

    campaign.write.parquet(source+"datalake/configs/")
    