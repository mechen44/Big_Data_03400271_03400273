from pyspark.sql import SparkSession

username = "mariarentzou"
spark = SparkSession \
    .builder \
    .appName("RDD Query 1 execution") \
    .getOrCreate()

sc = spark.sparkContext

input_dir = f"hdfs://hdfs-namenode:9000/user/root/data/"
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/data/q3-RDD"

crime_1=spark.read.option("header", "true").csv(input_dir + "LA_Crime_Data_2010_2019.csv")
crime_2 = spark.read.option("header", "true").csv(input_dir + "LA_Crime_Data_2020_2025.csv")
crime=crime_1.union(crime_2).rdd

crime_filtered=crime.filter(lambda x: x["Vict Age"] and 'aggravated assault' in x["Crm Cd Desc"].lower())

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

age_group_counts = crime_filtered \
    .map(age_group) \
    .filter(lambda x: x is not None) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: -x[1])\
    .collect()  # Bring results to driver

# Print each group and count
for group, count in age_group_counts:
    print(f"{group}: {count}")