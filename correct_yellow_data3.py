from pyspark import SparkConf
import pyspark
from pyspark.sql import SparkSession
import time
import os
import sys
import re
import io
from  pyspark.sql.types import StructType,StructField,IntegerType,TimestampType,IntegerType,DoubleType,StringType,FloatType,DateType
from pyspark.sql.functions import col,year,month,lit,round,when

#import glob

CustomSchemaNew = StructType([
    StructField("VendorID", StringType(),True), # IntegerType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime",  TimestampType(), True),
    StructField("passenger_count", DoubleType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID",StringType(),True),# DoubleType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", IntegerType()),
    StructField("DOLocationID", IntegerType()),
    StructField("Payment_Type", StringType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("airport_fee", DoubleType(), True) ])



CustomSchemaOld = StructType([
    StructField("VendorID", StringType(),True), # IntegerType(), True),
    StructField("tpep_pickup_datetime",   TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", DoubleType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("Start_lon", DoubleType(), True),
    StructField("Start_lat", DoubleType(), True),

    StructField("RatecodeID",StringType(),True), # DoubleType(), True),
    StructField("store_and_fwd_flag", StringType(), True),

    StructField("End_Lon", DoubleType(), True),
    StructField("End_Lat", DoubleType(), True),

    StructField("Payment_Type", StringType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("total_amount", DoubleType(), True)  ])



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


locIdConverterPath = "/user/tdp_user/data/taxis_zone.parquet"
locIdConverter_df  = spark.read.format("parquet").load(locIdConverterPath)

locIdConverter_df = locIdConverter_df.drop(*("zone","borough"))

locIdConverter_df.printSchema()

spark.sql("CREATE TABLE IF NOT EXISTS local.nyc.tempo_yellow (\
VendorID string, tpep_pickup_datetime timestamp, tpep_dropoff_datetime timestamp, passenger_count double,trip_distance double,\
RatecodeID string, store_and_fwd_flag string, PULocationID integer, DOLocationID integer, Payment_Type string, fare_amount double,\
extra double, mta_tax double, tip_amount double, tolls_amount double, improvement_surcharge double, total_amount double, \
congestion_surcharge double, airport_fee double) \
USING iceberg PARTITIONED BY (months(tpep_pickup_datetime));")

#df_new.writeTo("local.nyc.tempo_yellow").createOrReplace()

#spark.sql("ALTER TABLE local.nyc.tempo_yellow  ADD PARTITION FIELD months(tpep_pickup_datetime)")
#spark.sql("ALTER TABLE local.nyc.tempo_yellow  ADD PARTITION FIELD years(tpep_pickup_datetime)")



#df_old = spark.createDataFrame([], CustomSchemaOld)

#df.printSchema()

#df.writeTo("local.nyc.yellow_taxis_steps").create()

#df.printSchema()

#from pyspark.sql import SQLContext
#parquet_path = "hdfs:///user/tdp_user/data/nyc_green_taxi_trip/green_tripdata_*.parquet"
#parquet_path = "hdfs:/user/tdp_user/data/nyc_green_taxi_trip/green_tripdata_2021-12.parquet"
#parquet_path = "hdfs://mycluster:8020/user/tdp_user/data/nyc_green_taxi_trip/green_tripdata_2021-12.parquet"
#parquet_path = "hdfs://mycluster:8020/user/tdp_user/data/nyc_green_taxi_trip/green_tripdata_202*-*.parquet"
parquet_path = "/user/tdp_user/data/nyc_yellow_taxi_trip/yellow_tripdata_20*-12.parquet"


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

countGeoJoin = 0

print("\n\n\n")

# Parcours des fichiers et traitement
# https://www.perceptivebits.com/a-comprehensive-guide-to-finding-files-via-spark/


for fileStatus in status:
    print("********* ",fileStatus.getPath())
    path = str(fileStatus.getPath().toUri().getRawPath())


#print(f"\n\n****   Open : ", chemin_absolu)
    trips_df1  = spark.read.format("parquet") \
                           .load(path)


    if len(trips_df1.columns)== 18 or trips_df1.columns[-1][0]=='_':
      # old case
       if len(trips_df1.columns)== 19:
           trips_df1 = trips_df1.drop(trips_df1.columns[-1])
       trips_df1  = trips_df1.withColumnRenamed(trips_df1.columns[0],CustomSchemaOld[0].name) \
                             .withColumnRenamed(trips_df1.columns[1],CustomSchemaOld[1].name) \
                             .withColumnRenamed(trips_df1.columns[2],CustomSchemaOld[2].name) \
                             .withColumnRenamed(trips_df1.columns[3],CustomSchemaOld[3].name) \
                             .withColumnRenamed(trips_df1.columns[4],CustomSchemaOld[4].name) \
                             .withColumnRenamed(trips_df1.columns[5],CustomSchemaOld[5].name) \
                             .withColumnRenamed(trips_df1.columns[6],CustomSchemaOld[6].name) \
                             .withColumnRenamed(trips_df1.columns[7],CustomSchemaOld[7].name) \
                             .withColumnRenamed(trips_df1.columns[8],CustomSchemaOld[8].name) \
                             .withColumnRenamed(trips_df1.columns[9],CustomSchemaOld[9].name) \
                             .withColumnRenamed(trips_df1.columns[10],CustomSchemaOld[10].name) \
                             .withColumnRenamed(trips_df1.columns[11],CustomSchemaOld[11].name) \
                             .withColumnRenamed(trips_df1.columns[12],CustomSchemaOld[12].name) \
                             .withColumnRenamed(trips_df1.columns[13],CustomSchemaOld[13].name) \
                             .withColumnRenamed(trips_df1.columns[14],CustomSchemaOld[14].name) \
                             .withColumnRenamed(trips_df1.columns[15],CustomSchemaOld[15].name) \
                             .withColumnRenamed(trips_df1.columns[16],CustomSchemaOld[16].name) \
                             .withColumnRenamed(trips_df1.columns[17],CustomSchemaOld[17].name) \

       trips_df1.show(5)

       trips_df1 = trips_df1.withColumn("latitude", round(trips_df1["Start_lat"] * 1000))\
                            .withColumn("longitude", round(trips_df1["Start_lon"] * 1000))

       trips_df2 =  trips_df1.join(locIdConverter_df,on = ["latitude","longitude"],how='inner')\
                .drop(*("Start_lon","Start_lat","longitude","latitude")).withColumnRenamed("LocationID", "PULocationID")

       #trips_df2.show(5)


       trips_df2 = trips_df2.withColumn("latitude", round(trips_df2["End_Lat"] * 1000))\
                            .withColumn("longitude", round(trips_df2["End_Lon"] * 1000))


       #trips_df3 =  trips_df2.join(locIdConverter_df,[trips_df2.End_Lon == locIdConverter_df.longitude,trips_df2.End_Lat == locIdConverter_df.latitude],'inner')\
       trips_df3 = trips_df2.join(locIdConverter_df,on = ["latitude","longitude"],how='inner')\
                .drop(*("End_Lon","End_Lat","longitude","latitude")).withColumnRenamed("LocationID", "DOLocationID")


       trips_df3 = trips_df3.withColumn('improvement_surcharge', lit(None).cast(DoubleType())) \
                            .withColumn('congestion_surcharge', lit(None).cast(DoubleType())) \
                            .withColumn('airport_fee', lit(None).cast(DoubleType()))


       trips_df3 = trips_df3.withColumn(trips_df3.columns[1],col(trips_df3.columns[1]).cast(TimestampType())) \
                            .withColumn(trips_df3.columns[2],col(trips_df3.columns[2]).cast(TimestampType()))

       countGeoJoin = countGeoJoin + 2 *trips_df2.count()


       trips_df1 = trips_df3

#       trips_df3.writeTo("local.nyc.tempo_yellow").append()

#      trips_df1.select("CREATE VIEW v(c1,c2,c3,c4,c5,c6,c7,c8,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18,c19) as select ")

    else:
       trips_df1  = trips_df1.withColumnRenamed(trips_df1.columns[0],CustomSchemaNew[0].name) \
                             .withColumnRenamed(trips_df1.columns[1],CustomSchemaNew[1].name) \
                             .withColumnRenamed(trips_df1.columns[2],CustomSchemaNew[2].name) \
                             .withColumnRenamed(trips_df1.columns[3],CustomSchemaNew[3].name) \
                             .withColumnRenamed(trips_df1.columns[4],CustomSchemaNew[4].name) \
                             .withColumnRenamed(trips_df1.columns[5],CustomSchemaNew[5].name) \
                             .withColumnRenamed(trips_df1.columns[6],CustomSchemaNew[6].name) \
                             .withColumnRenamed(trips_df1.columns[7],CustomSchemaNew[7].name) \
                             .withColumnRenamed(trips_df1.columns[8],CustomSchemaNew[8].name) \
                             .withColumnRenamed(trips_df1.columns[9],CustomSchemaNew[9].name) \
                             .withColumnRenamed(trips_df1.columns[10],CustomSchemaNew[10].name) \
                             .withColumnRenamed(trips_df1.columns[11],CustomSchemaNew[11].name) \
                             .withColumnRenamed(trips_df1.columns[12],CustomSchemaNew[12].name) \
                             .withColumnRenamed(trips_df1.columns[13],CustomSchemaNew[13].name) \
                             .withColumnRenamed(trips_df1.columns[14],CustomSchemaNew[14].name) \
                             .withColumnRenamed(trips_df1.columns[15],CustomSchemaNew[15].name) \
                             .withColumnRenamed(trips_df1.columns[16],CustomSchemaNew[16].name) \
                             .withColumnRenamed(trips_df1.columns[17],CustomSchemaNew[17].name) \
                             .withColumnRenamed(trips_df1.columns[18],CustomSchemaNew[18].name) \

       trips_df1 = trips_df1.withColumn(trips_df1.columns[1],col(trips_df1.columns[1]).cast(TimestampType())) \
                            .withColumn(trips_df1.columns[2],col(trips_df1.columns[2]).cast(TimestampType()))




    df = trips_df1
    df = df.withColumn("VendorID", when(df.VendorID == "CMT","1").when(df.VendorID == "VTS","2").when(df.VendorID == "DDS","3").otherwise(df.VendorID))




    df = df.withColumn("store_and_fwd_flag", when(df.store_and_fwd_flag == "null","") \
                                        .when(df.store_and_fwd_flag == "nan","") \
                                        .when(df.store_and_fwd_flag == "1","Y") \
                                        .when(df.store_and_fwd_flag == "1.0","Y") \
                                        .when(df.store_and_fwd_flag == "0","N") \
                                        .when(df.store_and_fwd_flag == "0.0","N") \
                                        .otherwise(df.store_and_fwd_flag))

#  Cre CRE CRD Credit CREDIT, CAS  Cas CSH CASH Cash, Dis DIS Dispute, No NOC No Charge NA

    df = df.withColumn("Payment_Type", when(df.Payment_Type == 'CRE', "1").when(df.Payment_Type == 'Cre',"1").when(df.Payment_Type == 'CRD',"1").when(df.Payment_Type == 'CREDIT',"1")\
                                  .when(df.Payment_Type == 'Credit' ,"1") \
                                  .when(df.Payment_Type == 'CAS',"2").when(df.Payment_Type == 'Cas',"2").when(df.Payment_Type == 'CSH',"2").when(df.Payment_Type == 'CASH',"2")\
                                  .when(df.Payment_Type == 'Cash' ,"2") \
                                  .when(df.Payment_Type == 'DIS',"4").when(df.Payment_Type == 'Dis',"4").when(df.Payment_Type == 'Dispute', "4") \
                                  .when(df.Payment_Type == 'UNK',"0").when( df.Payment_Type ==  'Unknown',"0").when( df.Payment_Type ==  'UNKNOWN' , "0").when(df.Payment_Type == 'NA', "0") \
                                  .when(df.Payment_Type == 'No',"3").when( df.Payment_Type == 'NOC',"3").when( df.Payment_Type == 'No Charge', "3") \
                                  .otherwise( df.Payment_Type))




#       trips_df1.show(5) 
    df.writeTo("local.nyc.tempo_yellow").append()

print("il y a eu ",countGeoJoin, " jointures géographiques")

df  = spark.table("local.nyc.tempo_yellow") #  "local.nyc.yellow_taxis")

#df.select('VendorID').distinct().show()
#df.select('RatecodeID').distinct().show()
#df.select('store_and_fwd_flag').distinct().show()
#df.select('Payment_Type').distinct().show(200)

#df.groupBy('Payment_Type').count().show(200)


df.groupBy('Payment_Type').count().show(200)
#df.groupBy('store_and_fwd_flag').count().show(200)
#df.groupBy('VendorID').count().show()

#--
df = df.withColumn("year", year("tpep_pickup_datetime")) \
       .withColumn("month", month("tpep_pickup_datetime"))

# Définissez le chemin de sortie pour les fichiers Parquet partitionnés
#output_path = "chemin_vers_le_dossier_de_sortie"
#output_path = f"hdfs://mycluster:8020/user/tdp_user/data/my_nyc_yellow_taxi_trip_corrected/yellow_tripdata_{year_value}-{month_value:02d}.parquet"
output_path_tmp = f"hdfs://mycluster:8020/user/tdp_user/data/nyc_yellow_taxi_trip_corrected_/"

# Écrivez le DataFrame partitionné en Parquet
df.write.partitionBy("year", "month").parquet(output_path_tmp)

#--

parquet_path=f"hdfs://mycluster:8020/user/tdp_user/data/nyc_yellow_taxi_trip_corrected_/year=*/month=*"

output_path = f"hdfs://mycluster:8020/user/tdp_user/data/nyc_yellow_taxi_trip_corrected/"

status = fs.globStatus(Path(parquet_path))


for fileStatus in status:
    print("********* ",fileStatus.getPath())
    path = str(fileStatus.getPath().toUri().getRawPath())


    print(f"\n\n****   Open : ", path)
    trips_df1  = spark.read.format("parquet").load(path)

    # Divisez la chaîne en utilisant '/' comme séparateur
    parts = path.split('/')

    # Parcourez les parties pour extraire les valeurs de year et month
    year = None
    month = None

    for part in parts:
       if part.startswith("year="):
         year = part.replace("year=", "")
       elif part.startswith("month="):
         month = part.replace("month=", "")

    # Vérifiez si year et month ont été extraits avec succès
    if year is not None and month is not None:
        print(f"Year: {year}, Month: {month}")
        output_path = f"hdfs://mycluster:8020/user/tdp_user/data/my_nyc_yellow_taxi_trip_corrected/yellow_tripdata_{year}-{month:02d}.parquet"
        trips_df1.write.mode("overwrite").parquet(output_path)

    else:
        print("Impossible d'extraire year et month.")

#spark.sql("select * from local.nyc.tempo_yellow").show()




# Extraire l'année et le mois à partir du champ de la date
#df_with_year_month = df.withColumn("year", year(col("tpep_pickup_datetime")))\
#                       .withColumn("month", month(col("tpep_pickup_datetime")))


#                       .withColumn("ehail_fee", col("ehail_fee").cast("double").alias("ehail_fee")) \
#                       .withColumn("trip_type", col("trip_type").cast("double").alias("trip_type")) \
#                       .withColumn("payment_type", col("payment_type").cast("double").alias("payment_type"))

# Récupérer les combinaisons uniques année-mois
#year_month_combinations = df_with_year_month.select("year", "month").distinct().collect()
#print("col: ",year_month_combinations)


# Pour chaque combinaison année-mois, filtrer les données et écrire un fichier Parquet distinct

    # print(output_path," count: ",filtered_df.count())

#    filtered_df = filtered_df.select(*map(lambda col: filtered_df[col].cast(converter[col]), filtered_df.columns))
#[passenger_count], Expected: double, Found: INT64    

#    .createOrReplaceTempView("CastExample")

#    df_ = spark.sql("SELECT INT(VendorID),DATE(lpep_pickup_datetime),DATE(lpep_dropoff_datetime),STRING(store_and_fwd_flag),INT(RatecodeID),\
#                     INT(PULocationID),INT(DOLocationID),INT(passenger_count),DOUBLE(trip_distance),DOUBLE(fare_amount),DOUBLE(extra),\
#                     DOUBLE(mta_tax),DOUBLE(tip_amount),DOUBLE(tolls_amount),INT(ehail_fee),DOUBLE(improvement_surcharge),DOUBLE(total_amount),\
#                     INT(payment_type),INT(trip_type),DOUBLE(congestion_surcharge) from CastExample")

spark.stop()




A
A
A
A
A
A
Ai
A
A
A
A


A
A

