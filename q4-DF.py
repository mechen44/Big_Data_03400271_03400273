from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import col, count, sum, round, row_number, year, lit, to_timestamp, when
from pyspark.sql.window import Window
#------------------Q2 DataFrame Implementation----------------------#
print("DATAFRAME START")
username="mirtospatha"
spark = SparkSession.builder \
    .appName("DataFrame Query 2 Implementation") \
    .getOrCreate()

sc = spark.sparkContext
job_id = sc.applicationId

input_dir = f"hdfs://hdfs-namenode:9000/user/root/data/"
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/data/q2-DF_{job_id}"

police_stations = spark.read.option("header", "true").csv(input_dir + "LA_Police_Stations.csv")
crime_1 = spark.read.option("header", "true").csv(input_dir + "LA_Crime_Data_2010_2019.csv")

crime_2 = spark.read.option("header", "true").csv(input_dir + "LA_Crime_Data_2020_2025.csv")
crime = crime_1.union(crime_2)

crime = crime \
    .filter(col("AREA NAME").isNotNull() & col("DATE OCC").isNotNull()) \
    .withColumn("DATE OCC", to_timestamp(col("DATE OCC"), 'MM/dd/yyyy hh:mm:ss a')) \
    .withColumn("YEAR", year(col("DATE OCC"))) \
    .groupBy("YEAR", "AREA NAME") \
    .agg(
        count("*").alias("TOTAL_CASES"),
        sum(when(col("Status Desc").isin(
            "Juv Arrest", "Juv Other", "Adult Arrest", "Adult Other"
        ), 1).otherwise(0)).alias("CLOSED_CASES")
    ) \
    .withColumn("CLOSE_RATE", round(col("CLOSED_CASES") / col("TOTAL_CASES") * 100, 2))

window_spec = Window.partitionBy("YEAR").orderBy(col("CLOSE_RATE").desc())
ranked = crime.withColumn("RANK", row_number().over(window_spec)) \
                       .filter(col("RANK") <= 3) \
                       .orderBy("YEAR", "RANK")
print("DATAFRAME RESULT")
ranked.show(ranked.count(), truncate=False)
ranked.write.mode("append").csv(output_dir, header=True)
