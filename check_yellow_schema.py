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
    StructField("vendor_name", StringType(), True),
    StructField("Trip_Pickup_DateTime", StringType(), True),
    StructField("Trip_Dropoff_DateTime", StringType(), True),
    StructField("Passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("Start_lon", DoubleType(), True),
    StructField("Start_lat", DoubleType(), True),
    StructField("Rate_Code",DoubleType(), True),
    StructField("store_and_forward", DoubleType(), True),
    StructField("End_Lon", DoubleType(), True),
    StructField("End_Lat", DoubleType(), True),
    StructField("Payment_Type", StringType(), True),
    StructField("Fare_Amt", DoubleType(), True),
    StructField("surcharge", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("Tip_Amt", DoubleType(), True),
    StructField("Tolls_Amt", DoubleType(), True),
    StructField("Total_Amt", DoubleType(), True)])


conf = SparkConf() \
        .setAppName("Apache Iceberg with PySpark yellow corr") \
        .setAll([
# from https://iceberg.apache.org/spark-quickstart/#adding-a-catalog
            ("spark.jars" ,"/opt/tdp/spark3/jars/iceberg-spark-runtime-3.2_2.12-1.3.1.jar"),
            ("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"), \
            ("spark.sql.catalog.spark_catalog","org.apache.iceberg.spark.SparkSessionCatalog"), \
            ("spark.sql.catalog.spark_catalog.type","hive"), \
            ("spark.sql.catalog.local","org.apache.iceberg.spark.SparkCatalog"), \
            ("spark.sql.catalog.local.type","hadoop"), \
            ("spark.sql.catalog.local.warehouse","hdfs:///user/tdp_user/warehouse_hadoop_iceberg"), \
            ("spark.sql.defaultCatalog","local"),
#from Romain
#            ("spark.kerberos.access.hadoopFileSystems","hdfs:///")  #,
#            ("spark.yarn.access.hadoopFileSystems","hdfs://mycluster")

        ])\


spark = SparkSession.builder.config(conf=conf).getOrCreate()



sc = spark.sparkContext
sc.setLogLevel('ERROR')
# Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN

print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% \n %%%%%%%%%%%%%%%%%%%%%%%%%%ù")

#df.printSchema()

#from pyspark.sql import SQLContext
#parquet_path = "hdfs:///user/tdp_user/data/nyc_green_taxi_trip/green_tripdata_*.parquet"
#parquet_path = "hdfs:/user/tdp_user/data/nyc_green_taxi_trip/green_tripdata_2021-12.parquet"
#parquet_path = "hdfs://mycluster:8020/user/tdp_user/data/nyc_green_taxi_trip/green_tripdata_2021-12.parquet"
#parquet_path = "hdfs://mycluster:8020/user/tdp_user/data/nyc_green_taxi_trip/green_tripdata_202*-*.parquet"
parquet_path = "/user/tdp_user/data/nyc_yellow_taxi_trip/yellow_tripdata_20*.parquet"

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

yellow_stat = {}

print("\n\n\n")

# Parcours des fichiers et traitement
# https://www.perceptivebits.com/a-comprehensive-guide-to-finding-files-via-spark/
for fileStatus in status:
    print("********* ",fileStatus.getPath())
    path = str(fileStatus.getPath().toUri().getRawPath())


#print(f"\n\n****   Open : ", chemin_absolu)
    trips_df1  = spark.read.format("parquet") \
                           .load(path)

    schema_json = trips_df1.schema.json()


    if False:
      schema_json = schema_json.replace("\"type\":\"double\"","\"type\":\"string\"").lower()
      schema_json = schema_json.replace("\"type\":\"long\"","\"type\":\"string\"")
      schema_json = schema_json.replace("\"type\":\"integer\"","\"type\":\"string\"")

      schema_json = schema_json.replace("vendor_name","vendor_id")
      schema_json = schema_json.replace("vendorid","vendor_id")

      schema_json = schema_json.replace("\"pickup_datetime","\"tpep_pickup_datetime")
      schema_json = schema_json.replace("trip_pickup_datetime","tpep_pickup_datetime")

      schema_json = schema_json.replace("\"dropoff_datetime","\"tpep_dropoff_datetime")
      schema_json = schema_json.replace("trip_dropoff_datetime","tpep_dropoff_datetime")

      schema_json = schema_json.replace("total_amt","total_amount")
      schema_json = schema_json.replace("tolls_amt","tolls_amount")
      schema_json = schema_json.replace("tip_amt","tip_amount")
      schema_json = schema_json.replace("fare_amt","fare_amount")


      schema_json = schema_json.replace("rate_code","ratecodeid")
      schema_json = schema_json.replace("store_and_forward","store_and_fwd_flag")


    count = yellow_stat.get(schema_json,0)
    yellow_stat[schema_json] = count + 1


for key, value in yellow_stat.items():
    print(key, '  ', value, '\n')

spark.stop()

