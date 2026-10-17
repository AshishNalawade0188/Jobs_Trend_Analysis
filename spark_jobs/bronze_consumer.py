from pyspark.sql import SparkSession

# Create Spark Session
spark = SparkSession.builder \
    .appName("Bronze Consumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Read data from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "job_market") \
    .option("startingOffsets", "earliest") \
    .load()

# Convert Kafka value to String
bronze_df = kafka_df.selectExpr("CAST(value AS STRING) AS json_data")

# Write to HDFS as Parquet
query = bronze_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", "/user/talentum/bronze") \
    .option("checkpointLocation", "/user/talentum/checkpoint") \
    .start()

query.awaitTermination()
