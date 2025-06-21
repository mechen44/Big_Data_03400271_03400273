from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType
from pyspark.sql.functions import col, when, lower

username = "mariarentzou"
spark = SparkSession \
    .builder \
    .appName("DF Query 1 execution") \
    .getOrCreate()
sc = spark.sparkContext

input_dir = f"hdfs://hdfs-namenode:9000/user/root/data/"
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/data/q3-RDD"

crime_1=spark.read.format('csv') \
    .options(header='true') \
    .load(input_dir + "LA_Crime_Data_2010_2019.csv")

crime_2 =spark.read.format('csv') \
    .options(header='true') \
    .load(input_dir + "LA_Crime_Data_2020_2025.csv")

crime=crime_1.union(crime_2)

crime_filtered = crime.filter((lower(col("Crm Cd Desc")).contains("aggravated assault")) &
                              (col("Vict Age").isNotNull()))
def age_group(x):
    try:
        age = int(x["Vict Age"])
        if age > 64:
            return "Ηλικιωμένοι", 1
        elif age >= 25:
            return "Ενήλικοι", 1
        elif age >=18:
            return "Νεαροί ενήλικοι", 1
        elif age <18:
            return "Παιδιά", 1
    except:
        return None

crime_grouped = crime_filtered.withColumn(
    "Age Group",
    when(col("Vict Age") > 64, "Ηλικιωμένοι")
    .when(col("Vict Age") >= 25, "Ενήλικοι")
    .when(col("Vict Age") > 18 , "Νεαροί ενήλικοι")
    .otherwise("Παιδιά"))

# Group by age group and count
age_group_counts = crime_grouped.groupBy("Age Group").count().orderBy(col("count").desc())

# Show results
age_group_counts.show(truncate=False)