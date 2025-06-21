from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, round

username = "mariarentzou"
spark = SparkSession \
    .builder \
    .appName("DF parquet Query 5 execution") \
    .getOrCreate()

sc = spark.sparkContext

input_dir = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/"

income_df=spark.read.option("header", True).parquet(input_dir + "Med_HH_Income.parquet")
population_df = spark.read.option("header", True).parquet(input_dir + "ZipCode.parquet")

income_df = income_df.withColumn("Estimated Median Income",
                                 regexp_replace(col("Estimated Median Income"), "[$,]", "").cast("double"))

# Clean population column
population_df = population_df.withColumn("Average Household Size",
                                         col("Average Household Size").cast("double"))

joined_df = income_df.join(population_df, on="Zip Code", how="inner")

income_per_individual=joined_df.withColumn("Median Income per Individual", round(col("Estimated Median Income")/col("Average Household Size")))

income_per_individual.select("Zip Code", "Median Income per Individual").show(truncate=False)