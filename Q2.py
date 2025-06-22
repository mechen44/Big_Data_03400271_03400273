from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import col, count, sum, round, row_number, year, lit, to_timestamp, when
from pyspark.sql.window import Window
#------------------Q2 RDD Implementation----------------------#
print("RDD STARTED")
username="mirtospatha"
spark = SparkSession.builder \
    .appName("RDD Query 2 Implementation") \
    .getOrCreate()

sc = spark.sparkContext
job_id = sc.applicationId

input_dir = f"hdfs://hdfs-namenode:9000/user/root/data/"
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/data/q2-RDD_{job_id}"

police_stations = spark.read.option("header", "true").csv(input_dir + "LA_Police_Stations.csv").rdd
crime_1 = spark.read.option("header", "true").csv(input_dir + "LA_Crime_Data_2010_2019.csv").rdd

crime_2 = spark.read.option("header", "true").csv(input_dir + "LA_Crime_Data_2020_2025.csv").rdd

def extract_year_area(x):
    y = year(col("DATE OCC"))
    return ((y, x['AREA NAME']), 1)

def is_closed_case(x):
    return x['Status Desc'] in ["Juv Arrest", "Juv Other", "Adult Arrest", "Adult Other"]

all_cases = crime_1.union(crime_2).map(extract_year_area)
closed_cases = crime_1.filter(is_closed_case).union(crime_2.filter(is_closed_case)).map(extract_year_area)

total_counts = all_cases.reduceByKey(lambda a, b: a + b)           # (year, area) → total
closed_counts = closed_cases.reduceByKey(lambda a, b: a + b)       # (year, area) → closed

# (year, area) → (closed, total)
joined = closed_counts.join(total_counts)

# (year, area) → percentage
percentages = joined.map(lambda x: (x[0][0], (x[0][1], x[1][0] / x[1][1])))  # (year, (area, ratio))

grouped_by_year = percentages.groupByKey().mapValues(list)

def top_3_ranked(year_data):
    year, area_ratios = year_data
    sorted_top3 = sorted(area_ratios, key=lambda x: -x[1])[:3]  # ranked by descending order of percentage
    return [(year, rank+1, area, ratio * 100) for rank, (area, ratio) in enumerate(sorted_top3)]

ranked = grouped_by_year.flatMap(top_3_ranked)

final_result = ranked.sortBy(lambda x: (x[0], x[1])) # sort by year and percentage

print("RDD RESULT")
for row in final_result.coalesce(1).collect():
    print(f"Έτος: {row[0]}, Τμήμα: {row[2]}, Ποσοστό: {row[3]}%, Ranking: {row[1]}")

final_rdd.coalesce(1).saveAsTextFile(output_dir)

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

crime_1_clean = crime_1 \
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

crime_2_clean = crime_2 \
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

crime = crime_1_clean.union(crime_2_clean)
window_spec = Window.partitionBy("YEAR").orderBy(col("CLOSE_RATE").desc())
ranked = crime.withColumn("RANK", row_number().over(window_spec)) \
                       .filter(col("RANK") <= 3) \
                       .orderBy("YEAR", "RANK")
print("DATAFRAME RESULT")
ranked.show(ranked.count(), truncate=False)
ranked.write.mode("append").csv(output_dir, header=True)


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


'''
# Juv Arrest, UNK, Juv Other, Adult Arrest, Adult Other, Invest Cont
crime_1_clean = crime_1 \
                .filter(lambda x: x['Status Desc'] in ["Juv Arrest", "Juv Other", "Adult Arrest", "Adult Other"]) \
                .map(lambda x: [datetime.strptime(x['DATE OCC'], "%m/%d/%Y %I:%M:%S %p").year, x['AREA NAME']], 1)

#crime_2_clean = crime_2 \
#                .filter(lambda x: x['Status Desc'] in ["Juv Arrest", "Juv Other", "Adult Arrest", "Adult Other"]) \
#                .map(lambda x: [x['AREA NAME'], [datetime.strptime(x['DATE OCC'], "%m/%d/%Y %I:%M:%S %p").year, x['Status Desc']]]) \
#                .mapValues()
#print("CRIME2")
#for c in crime_2_clean.take(5):
#    print(c)
#crime_clean = crime_clean.
#final_data = crime.union(ps_rdd).rdd

police_clean = police_stations.map(lambda x: [x['DIVISION']])
#all_cases = crime_1.union(crime_2)
#closed_cases = crime_1_clean.union(crime_2_clean)
all_cases = crime_1
closed_cases = crime_1_clean
total_count = all_cases.reduceByKey(lambda a, b: a + b)           # (year, area) → total
print("TOTALCOUNT")
for c in total_count.take(5):
    print(c)
closed_count = closed_cases.reduceByKey(lambda a, b: a + b)       # (year, area) → closed
print("CLOSEDCOUNT")
for c in closed_count.take(5):
    print(c)

# (year, area) → (closed, total)
joined = closed_count.join(total_count)

# (year, area) → percentage
percentages = joined.map(lambda x: (x[0][0], (x[0][1], x[1][0] / x[1][1])))  # (year, (area, ratio))
grouped_by_year = percentages.groupByKey()

for row in grouped_by_year.take(5):
    print(row)

def top_3_ranked(year_data):
    year, area_ratios = year_data
    sorted_top3 = sorted(area_ratios, key=lambda x: -x[1])[:3]  # φθίνουσα κατά ποσοστό
    return [(year, rank+1, area, round(ratio * 100, 2)) for rank, (area, ratio) in enumerate(sorted_top3)]

ranked = grouped_by_year.flatMap(top_3_ranked)
final_result = ranked.sortBy(lambda x: (x[0], x[1]))  # κατά έτος και ranking

for row in final_result.collect():
    print(f"Έτος: {row[0]}, Τμήμα: {row[2]}, Ποσοστό: {row[3]}%, Ranking: {row[1]}")
'''