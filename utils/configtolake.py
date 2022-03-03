import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession, Window

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[1]").appName("finalProject").getOrCreate()
    
    source = "/workspace/PysparkMongoAirflow/Final_Project/data/"

    config = spark.read.option("header", True).option("inferSchema", True).option("delimiter", '\t') \
                        .csv(source + "source/configs/*.csv").cache()
    
    config.write.format("parquet").mode('overwrite') \
            .save(source+"datalake/configs")
    spark.stop()
   
