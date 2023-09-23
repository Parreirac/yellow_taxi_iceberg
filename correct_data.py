from pyspark import SparkConf
import pyspark
from pyspark.sql import SparkSession
import time
import os
import sys
import re
import io
from  pyspark.sql.types import StructType,StructField,IntegerType,TimestampType,IntegerType,DoubleType,StringType,FloatType
from pyspark.sql.functions import col,year,month

import glob

CustomSchema = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("lpep_pickup_datetime", TimestampType(), True),
    StructField("lpep_dropoff_datetime", TimestampType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("RatecodeID", IntegerType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("passenger_count",DoubleType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("ehail_fee", IntegerType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("trip_type", IntegerType(), True),
    StructField("congestion_surcharge", DoubleType(), True)])

conf = SparkConf() \
        .setAppName("Apache Iceberg with PySpark") \
        .setAll([
# from https://iceberg.apache.org/spark-quickstart/#adding-a-catalog
            ("spark.jars" ,"/opt/tdp/spark3/jars/iceberg-spark-runtime-3.2_2.12-1.3.1.jar"),
            ("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"), \
            ("spark.sql.catalog.spark_catalog","org.apache.iceberg.spark.SparkSessionCatalog"), \
            ("spark.sql.catalog.spark_catalog.type","hive"), \
            ("spark.sql.catalog.local","org.apache.iceberg.spark.SparkCatalog"), \
            ("spark.sql.catalog.local.type","hadoop"), \
            ("spark.sql.catalog.local.warehouse","hdfs:///user/tdp_user/warehouse_hadoop_iceberg"), \
            ("spark.sql.defaultCatalog","local")])  #,
#from Romain





spark = SparkSession.builder.config(conf=conf).getOrCreate()

#spark.jars /opt/tdp/spark3/jars/iceberg-spark-runtime-3.2_2.12-1.3.1.jar
#spark.sql.extensions org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
#spark.sql.catalog.spark_catalog org.apache.iceberg.spark.SparkSessionCatalog
#spark.sql.catalog.spark_catalog.type hive
#spark.sql.catalog.local org.apache.iceberg.spark.SparkCatalog
#spark.sql.catalog.local.type hadoop
#spark.sql.catalog.local.warehouse hdfs:///user/tdp_user/warehouse_hadoop_iceberg
#spark.sql.defaultCatalog local
#spark.kerberos.access.hadoopFileSystems hdfs:///


#you should see the wholeStage codegen stage indexes in the plan.
#spark.conf.set("spark.sql.codegen.wholeStage", "true")  # TODO retester !
#spark.conf.set("spark.sql.parquet.enableVectorizedReader","false")  

sc = spark.sparkContext
sc.setLogLevel('ERROR')
# Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN

print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% \n %%%%%%%%%%%%%%%%%%%%%%%%%%ù")


df = spark.createDataFrame([], CustomSchema)

df.printSchema()


df.writeTo("local.nyc.taxis").create()

#df.printSchema()

#from pyspark.sql import SQLContext
#parquet_path = "hdfs:///user/tdp_user/data/nyc_green_taxi_trip/green_tripdata_*.parquet"
#parquet_path = "hdfs:/user/tdp_user/data/nyc_green_taxi_trip/green_tripdata_2021-12.parquet"
#parquet_path = "hdfs://mycluster:8020/user/tdp_user/data/nyc_green_taxi_trip/green_tripdata_2021-12.parquet"
#parquet_path = "hdfs://mycluster:8020/user/tdp_user/data/nyc_green_taxi_trip/green_tripdata_202*-*.parquet"
parquet_path = "/user/tdp_user/data/nyc_green_taxi_trip/green_tripdata_2*-*.parquet"

#parquet_path2 = "hdfs://mycluster:8020/user/tdp_user/data/nyc_green_taxi_trip/green_tripdata_2021-11.parquet"

# /user/tdp_user/data/nyc_green_taxi_trip/green_tripdata_2021-12.parquet
# Load Parquet files as a DataFrame

# from https://stackoverflow.com/questions/35750614/pyspark-get-list-of-files-directories-on-hdfs-path
URI           = sc._gateway.jvm.java.net.URI
Path          = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSystem    = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
Configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration


fs = FileSystem.get(URI("hdfs://mycluster:8020"), Configuration())
status = fs.globStatus(Path(parquet_path))


# Résoudre les caractères génériques en une liste de chemins absolus
#chemins_absolus = glob.glob(parquet_path)


print("\n\n\n")

# Parcours des fichiers et traitement
# https://www.perceptivebits.com/a-comprehensive-guide-to-finding-files-via-spark/
for fileStatus in status:
    print("********* ",fileStatus.getPath())
    path = str(fileStatus.getPath().toUri().getRawPath())


#print(f"\n\n****   Open : ", chemin_absolu)
    trips_df1  = spark.read.format("parquet") \
                           .load(path)

    trips_df1.printSchema()
    trips_df1.writeTo("local.nyc.taxis").append()

#for chemin_absolu in chemins_absolus:
#     print(f"\n\n****   Open : ", chemin_absolu)
#     if os.path.isfile(chemin_absolu):
#         print(f"\n\n****   Open : ", chemin_absolu)
#         trips_df1  = spark.read.format("parquet") \
#                           .load(chemin_absolu)
#         trips_df1.writeTo("local.nyc.taxis").append()

df = spark.table("local.nyc.taxis")

spark.sql(select * from local.nyc.table.history").show()




# Extraire l'année et le mois à partir du champ de la date
df_with_year_month = df.withColumn("year", year(col("lpep_pickup_datetime")))\
                       .withColumn("month", month(col("lpep_pickup_datetime")))


#                       .withColumn("ehail_fee", col("ehail_fee").cast("double").alias("ehail_fee")) \
#                       .withColumn("trip_type", col("trip_type").cast("double").alias("trip_type")) \
#                       .withColumn("payment_type", col("payment_type").cast("double").alias("payment_type"))

# Récupérer les combinaisons uniques année-mois
year_month_combinations = df_with_year_month.select("year", "month").distinct().collect()
#print("col: ",year_month_combinations)


# Pour chaque combinaison année-mois, filtrer les données et écrire un fichier Parquet distinct
for row in year_month_combinations:
    year_value = row["year"]
    month_value = row["month"]

    filtered_df = df_with_year_month.filter((col("year") == year_value) & (col("month") == month_value))

    output_path = f"hdfs://mycluster:8020/user/tdp_user/data/my_nyc_green_taxi_trip/green_tripdata_{year_value}-{month_value:02d}.parquet"
    print("\n\n***** Ecriture de ",output_path)

    filtered_df = filtered_df.drop("year", "month") #.withColumn("passenger_count",col("passenger_count").cast(DoubleType()).alias("passenger_count"))

    print(output_path," count: ",filtered_df.count())

#    filtered_df = filtered_df.select(*map(lambda col: filtered_df[col].cast(converter[col]), filtered_df.columns))
#[passenger_count], Expected: double, Found: INT64    

#    .createOrReplaceTempView("CastExample")

#    df_ = spark.sql("SELECT INT(VendorID),DATE(lpep_pickup_datetime),DATE(lpep_dropoff_datetime),STRING(store_and_fwd_flag),INT(RatecodeID),\
#                     INT(PULocationID),INT(DOLocationID),INT(passenger_count),DOUBLE(trip_distance),DOUBLE(fare_amount),DOUBLE(extra),\
#                     DOUBLE(mta_tax),DOUBLE(tip_amount),DOUBLE(tolls_amount),INT(ehail_fee),DOUBLE(improvement_surcharge),DOUBLE(total_amount),\
#                     INT(payment_type),INT(trip_type),DOUBLE(congestion_surcharge) from CastExample")

    filtered_df.write.mode("overwrite").parquet(output_path)
spark.stop()
