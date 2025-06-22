from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, lower, upper, radians, sin, cos, atan2, sqrt, pow, row_number, avg, count
from pyspark.sql.window import Window

username = "mirtospatha"

# Initialize Spark session
spark = SparkSession.builder \
    .appName("DF Query 4 execution horizontal") \
    .config("spark.executor.instances", "2") \
    .config("spark.executor.cores", "4") \
    .config("spark.executor.memory", "8g") \
    .getOrCreate()

sc = spark.sparkContext
job_id = sc.applicationId

input_dir = f"hdfs://hdfs-namenode:9000/user/root/data/"
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/query4_df_output_{job_id}"

# Read crime data
crime_1 = spark.read.option("header", "true").csv(input_dir + "LA_Crime_Data_2010_2019.csv")
crime_2 = spark.read.option("header", "true").csv(input_dir + "LA_Crime_Data_2020_2025.csv")
crime_df = crime_1.union(crime_2)

# Read police stations
police_df = spark.read.option("header", "true").csv(input_dir + "LA_Police_Stations.csv")

# Read MO codes
mocodes = spark.read.text(input_dir + "MO_codes.txt")
mo_codes_df = mocodes.selectExpr(
    "split(value, ' ', 2)[0] as MO_code",
    "split(value, ' ', 2)[1] as Description")

# Filter MO codes for guns/weapons
mo_codes_filtered = mo_codes_df.filter(
    (lower(col("Description")).contains("gun")) | (lower(col("Description")).contains("weapon"))).select("MO_code")

# Prepare crime data with exploded MO codes
crime_df = crime_df.withColumn("mo_code", explode(split(col("Mocodes"), " "))).filter(col("mo_code") != "")
crime_df = crime_df.filter((col("LAT") != 0) & (col("LON") != 0))

# Join with MO codes for weapon/gun related incidents
crime_weapons = crime_df.join(
    mo_codes_filtered,
    crime_df["mo_code"] == mo_codes_filtered["MO_code"],
    "inner"
).dropDuplicates(["DR_NO"]).explain(True)

# Standardize area names
crime_weapons = crime_weapons.withColumn("AREA_NAME_UPPER", upper(col("AREA NAME")))
police_df = police_df \
    .withColumn("lat_deg", col("Y")) \
    .withColumn("lon_deg", col("X")) \
    .withColumn("DIVISION_UPPER", upper(col("DIVISION")))

# Join crime and police data on area/division
joined_df = crime_weapons.join(
    police_df,
    crime_weapons["AREA_NAME_UPPER"] == police_df["DIVISION_UPPER"]).explain(True)

# Haversine formula
R = 6371000  # Earth radius in meters
joined_df = joined_df \
    .withColumn("dlat", radians(col("lat_deg") - col("LAT"))) \
    .withColumn("dlon", radians(col("lon_deg") - col("LON"))) \
    .withColumn("lat1", radians(col("LAT"))) \
    .withColumn("lat2", radians(col("lat_deg")))

joined_df = joined_df.withColumn("a",
    pow(sin(col("dlat") / 2), 2) +
    cos(col("lat1")) * cos(col("lat2")) *
    pow(sin(col("dlon") / 2), 2))

joined_df = joined_df.withColumn("c", 2 * atan2(sqrt(col("a")), sqrt(1 - col("a"))))
joined_df = joined_df.withColumn("distance", col("c") * R)

# Get closest police station per incident
part = Window.partitionBy("DR_NO").orderBy(col("distance").asc())
closest_df = joined_df.withColumn("rn", row_number().over(part)).filter(col("rn") == 1)

# Final aggregation
result = closest_df.groupBy("DIVISION_UPPER") \
    .agg(
        count("*").alias("weapon_incidents"),
        avg(col("distance") / 1000).alias("average_distance_km")
    ) \
    .orderBy(col("weapon_incidents").desc())

# Show results
results = result.select(
    col("DIVISION_UPPER").alias("division"),
    col("average_distance_km").cast("double"),
    col("weapon_incidents")).collect()

for row in results:
    print(f"{row['division']} {row['average_distance_km']:.3f} {row['weapon_incidents']}")


# Save result as CSV (no Parquet)
result.write.option("header", "true").mode("overwrite").csv(output_dir)