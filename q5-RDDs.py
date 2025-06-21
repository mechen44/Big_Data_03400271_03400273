from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

username = "mariarentzou"
spark = SparkSession \
    .builder \
    .appName("RDD Query 5 execution") \
    .getOrCreate()

sc = spark.sparkContext

input_dir = f"hdfs://hdfs-namenode:9000/user/root/data/"
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/data/q5-RDD"

income_df=spark.read.option("header", "true").csv(input_dir + "LA_income_2015.csv")
population_df = spark.read.option("header", "true").csv(input_dir + "2010_Census_Populations_by_Zip_Code.csv")

income_df = income_df.withColumn("Estimated Median Income",
                                 regexp_replace(col("Estimated Median Income"), "[$,]", ""))

income_rdd = income_df.rdd.filter(lambda x: x["Zip Code"] and x["Estimated Median Income"]) \
    .map(lambda x: (x["Zip Code"], float(x["Estimated Median Income"])))

population_rdd = population_df.rdd.filter(lambda x: x["Zip Code"] and x["Average Household Size"]) \
    .map(lambda x: (x["Zip Code"], float(x["Average Household Size"])))

joined_rdd = income_rdd.join(population_rdd)

income_per_individual = joined_rdd.map(lambda x: (x[0], round(x[1][0] / x[1][1], 2)))

for zip_code, income in income_per_individual.take(10):
    print(f"Zip Code {zip_code}: ${income} per person")



