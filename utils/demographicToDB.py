from pyspark.sql import SparkSession
# spark = SparkSession \
#     .builder \
#     .appName("demographic") \
#     .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.coll") \
#     .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.coll") \
#     .getOrCreate()