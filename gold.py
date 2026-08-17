from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# =====================================================
# Create Spark Session
# =====================================================

spark = SparkSession.builder \
    .appName("Gold Layer") \
    .getOrCreate()

# =====================================================
# Read Silver Layer
# =====================================================

goldDF = spark.read.parquet(
    "hdfs://localhost:9000/user/talentum/silver_layer"
)

print("Rows Before Gold :", goldDF.count())

# =====================================================
# 1. Fix Salary_Min > Salary_Max
# =====================================================

goldDF = goldDF.withColumn(
    "Temp_Salary",
    when(col("Salary_Min") > col("Salary_Max"), col("Salary_Min"))
)

goldDF = goldDF.withColumn(
    "Salary_Min",
    when(col("Salary_Min") > col("Salary_Max"), col("Salary_Max"))
    .otherwise(col("Salary_Min"))
)

goldDF = goldDF.withColumn(
    "Salary_Max",
    when(col("Temp_Salary").isNotNull(), col("Temp_Salary"))
    .otherwise(col("Salary_Max"))
)

goldDF = goldDF.drop("Temp_Salary")

print("After Salary Fix :", goldDF.count())

# =====================================================
# 2. Remove Impossible Salary Values
# =====================================================

goldDF = goldDF.filter(
    (col("Salary_Min") >= 0) &
    (col("Salary_Max") <= 10000000)
)

print("After Salary Range :", goldDF.count())

# =====================================================
# 3. Cap Applicants Count
# =====================================================

goldDF = goldDF.withColumn(
    "Applicants_Count",
    when(col("Applicants_Count") > 100000, 100000)
    .otherwise(col("Applicants_Count"))
)

print("After Applicants Fix :", goldDF.count())

# =====================================================
# 4. Cap Interview Rounds
# =====================================================

goldDF = goldDF.withColumn(
    "Interview_Rounds",
    when(col("Interview_Rounds") > 20, 20)
    .otherwise(col("Interview_Rounds"))
)

print("After Interview Fix :", goldDF.count())

# =====================================================
# 5. Fix Selection Rate
# =====================================================

goldDF = goldDF.withColumn(
    "Selection_Rate",
    when(col("Selection_Rate") > 100, 100)
    .when(col("Selection_Rate") < 0, 0)
    .otherwise(col("Selection_Rate"))
)

print("After Selection Rate Fix :", goldDF.count())

# =====================================================
# 6. Business Features
# =====================================================

goldDF = goldDF.withColumn(
    "Average_Salary",
    (col("Salary_Min") + col("Salary_Max")) / 2
)

goldDF = goldDF.withColumn(
    "Salary_Category",
    when(col("Average_Salary") < 500000, "Low")
    .when(col("Average_Salary") < 1000000, "Medium")
    .otherwise("High")
)

goldDF = goldDF.withColumn(
    "Hiring_Status",
    when(col("Applicants_Count") > 1000, "High Hiring")
    .otherwise("Normal Hiring")
)

print("Rows After Gold :", goldDF.count())

# =====================================================
# Save Gold Layer to HDFS
# =====================================================

goldDF.write \
    .mode("overwrite") \
    .parquet("hdfs://localhost:9000/user/talentum/gold_layer")

print("Gold Layer saved to HDFS.")

# =====================================================
# Save CSV to HDFS
# =====================================================

goldDF.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("hdfs://localhost:9000/user/talentum/gold_csv")

print("Gold CSV saved to HDFS.")

print("Gold Layer Created Successfully")

spark.stop()
