from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, when, lower

username = "mariarentzou"
spark = SparkSession \
    .builder \
    .appName("DF udf Query 1 execution") \
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
# Define function
def get_age_group(age):
    try:
        age = int(age)
        if age > 64:
            return "Ηλικιωμένοι"
        elif age >= 25:
            return "Ενήλικοι"
        elif age >= 18:
            return "Νεαροί ενήλικοι"
        else:
            return "Παιδιά"
    except:
        return None

# Convert it to a UDF
age_group_udf = udf(get_age_group, StringType())

crime_grouped = crime_filtered.withColumn("Age Group", age_group_udf(col("Vict Age")))

age_group_counts = crime_grouped.groupBy("Age Group").count().orderBy(col("count").desc())

# Show results
age_group_counts.show(truncate=False)