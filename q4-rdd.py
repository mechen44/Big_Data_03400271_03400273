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

for row in final_result.collect():
    print(f"Έτος: {row[0]}, Τμήμα: {row[2]}, Ποσοστό: {row[3]}%, Ranking: {row[1]}")
'''
