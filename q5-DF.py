from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.functions import round


username = "mariarentzou"
spark = SparkSession \
    .builder \
    .appName("DF Query 5 execution") \
    .getOrCreate()

sc = spark.sparkContext

input_dir = f"hdfs://hdfs-namenode:9000/user/root/data/"
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/data/q5-RDD"

income_df=spark.read.option("header", True).csv(input_dir + "LA_income_2015.csv")
population_df = spark.read.option("header", True).csv(input_dir + "2010_Census_Populations_by_Zip_Code.csv")

income_df = income_df.withColumn("Estimated Median Income",
                                 regexp_replace(col("Estimated Median Income"), "[$,]", "").cast("double"))

# Clean population column
population_df = population_df.withColumn("Average Household Size",
                                         col("Average Household Size").cast("double"))

joined_df = income_df.join(population_df, on="Zip Code", how="inner")
joined_df.explain(True)

income_per_individual=joined_df.withColumn("Median Income per Individual", round(col("Estimated Median Income")/col("Average Household Size")))

income_per_individual.select("Zip Code", "Median Income per Individual").show(truncate=False)


