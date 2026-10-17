from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Bronze Layer") \
    .getOrCreate()

bronzeDF = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("hdfs://localhost:9000/user/talentum/bronze/Job_Analytics_2.csv")

print("Bronze Rows:", bronzeDF.count())
print("Bronze Columns:", len(bronzeDF.columns))

bronzeDF.write \
    .mode("overwrite") \
    .parquet("hdfs://localhost:9000/user/talentum/bronze_layer")

print("Bronze Layer Created Successfully")

spark.stop()
