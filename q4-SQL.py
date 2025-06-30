from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import col, count, sum, round, row_number, year, lit, to_timestamp, when
from pyspark.sql.window import Window
# ---------------------- SQL Version --------------------------------------------

print("SQL STARTED")
username="mirtospatha"
spark = SparkSession.builder \
    .appName("SQL Query 2 Implementation") \
    .getOrCreate()

sc = spark.sparkContext
job_id = sc.applicationId

input_dir = f"hdfs://hdfs-namenode:9000/user/root/data/"
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/data/q2-SQL_{job_id}"

police_stations = spark.read.option("header", "true").csv(input_dir + "LA_Police_Stations.csv")
crime_1 = spark.read.option("header", "true").csv(input_dir + "LA_Crime_Data_2010_2019.csv")

crime_2 = spark.read.option("header", "true").csv(input_dir + "LA_Crime_Data_2020_2025.csv")
crime = crime_1.union(crime_2)
crime.createOrReplaceTempView("crime")

sql_result = spark.sql("""
    SELECT YEAR, `AREA NAME`,
           COUNT(*) AS TOTAL_CASES,
           SUM(CASE WHEN `Status Desc` IN ('Juv Arrest', 'Juv Other', 'Adult Arrest', 'Adult Other') THEN 1 ELSE 0 END) AS CLOSED_CASES,
           ROUND(SUM(CASE WHEN `Status Desc` IN ('Juv Arrest', 'Juv Other', 'Adult Arrest', 'Adult Other') THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS CLOSE_RATE
    FROM (
        SELECT 
        *, EXTRACT(YEAR FROM TO_TIMESTAMP(`DATE OCC`, 'MM/dd/yyyy hh:mm:ss a')) AS YEAR
        FROM crime
        )
    WHERE `AREA NAME` IS NOT NULL AND YEAR IS NOT NULL
    GROUP BY YEAR, `AREA NAME`
""")
sql_result.createOrReplaceTempView("summary")
# Add ranking
sql_result.createOrReplaceTempView("summary")
final_sql = spark.sql("""
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY YEAR ORDER BY CLOSE_RATE DESC) AS RANK
        FROM summary
    ) WHERE RANK <= 3
    ORDER BY YEAR, RANK
""")
print("SQL RESULT")
final_sql.show(final_sql.count(),truncate=False)
# Save SQL result
final_sql.write.mode("append").csv(output_dir, header=True)

spark.stop()