from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Silver Layer") \
    .getOrCreate()

silverDF = spark.read.parquet(
    "hdfs://localhost:9000/user/talentum/bronze_layer"
)

print("Rows Before Silver :", silverDF.count())

# Remove duplicate rows
silverDF = silverDF.dropDuplicates()

# Remove rows having important NULL values
silverDF = silverDF.filter(
    col("Job_Title").isNotNull() &
    col("Company_Name").isNotNull() &
    col("Salary_Min").isNotNull() &
    col("Salary_Max").isNotNull() &
    col("Posted_Date").isNotNull()
)

print("Rows After Silver :", silverDF.count())

silverDF.write \
    .mode("overwrite") \
    .parquet("hdfs://localhost:9000/user/talentum/silver_layer")

print("Silver Layer Created Successfully")

spark.stop()
