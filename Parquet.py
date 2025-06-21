from pyspark.sql import SparkSession
#from pyspark.sql.functions import col
username = "mariarentzou"
spark = SparkSession \
    .builder \
    .appName("Question 2") \
    .getOrCreate()

sc = spark.sparkContext

input_dir = f"hdfs://hdfs-namenode:9000/user/root/data/"
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/"

# Read CSV files from HDFS
LA_crime = spark.read.option("header", "true").csv(input_dir + "LA_Crime_Data_2010_2019.csv")
LA_crime_2 = spark.read.option("header", "true").csv(input_dir + "LA_Crime_Data_2020_2025.csv")
LAPD = spark.read.option("header", "true").csv(input_dir + "LA_Police_Stations.csv")
Med_HH_Income = spark.read.option("header", "true").csv(input_dir + "LA_income_2015.csv")
ZipCode = spark.read.option("header", "true").csv(input_dir + "2010_Census_Populations_by_Zip_Code.csv")
MOcodes = spark.read.text(input_dir + "MO_codes.txt")

MOcodes_df = (MOcodes.selectExpr("split(value, ' ', 2)[0] as MO_code",
                                   "split(value, ' ', 2)[1] as Description"))

# Save as parquet to HDFS
LA_crime.write.mode("overwrite").parquet(output_dir + "LA_crime.parquet")
LA_crime_2.write.mode("overwrite").parquet(output_dir + "LA_crime_2.parquet")
LAPD.write.mode("overwrite").parquet(output_dir + "LAPD.parquet")
Med_HH_Income.write.mode("overwrite").parquet(output_dir + "Med_HH_Income.parquet")
ZipCode.write.mode("overwrite").parquet(output_dir + "ZipCode.parquet")
MOcodes_df.write.mode("overwrite").parquet(output_dir + "MOcodes.parquet")

MOcodes_df.show(10, truncate=False)
LAPD.show(10, truncate=False)
LA_crime.show(10, truncate=False)

spark.stop()