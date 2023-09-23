  # les schemas
  
  green
  
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


yellow 

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


On passe des lat/long a un int avec une transfo du type :
    CONCAT(
        LPAD(ROUND(latitude / 10, 0), 4, '0'), -- Arrondir et ajouter les zéros muets
        LPAD(ROUND(longitude / 10, 0), 4, '0') -- Arrondir et ajouter les zéros muets
    ) AS unique_id

# les requetes
  
## initiales (green) 

# select  
   Les memes que GB mais sans GB

   pas de select simple car va tout prendre
   
   select distinct champs from table

    
# group by separer les avg et les count
    ("Query 0: Average trip distance by vendorID", "SELECT vendorID, AVG(trip_distance) AS avg_distance FROM df GROUP BY vendorID"),    
   
    ("Query 1: Total trips by year", "SELECT YEAR(lpep_pickup_datetime), COUNT(*) AS total_trips FROM df GROUP BY YEAR(lpep_pickup_datetime)"),
    ("Query 6: Total trips by pickup borough", "SELECT PULocationID, COUNT(*) AS total_trips FROM df GROUP BY PULocationID"),
    ("Query 10: Total trips by dropoff borough", "SELECT DOLocationID, COUNT(*) AS total_trips FROM df GROUP BY DOLocationID"),
    
    
    ("Query 2: Average fare amount by payment type", "SELECT payment_type, AVG(fare_amount) AS avg_fare FROM df GROUP BY payment_type"),
    ("Query 4: Average trip distance by vendorID", "SELECT vendorID, AVG(trip_distance) AS avg_distance FROM df GROUP BY vendorID"),
    ("Query 7: Average trip duration by passenger count", "SELECT passenger_count, AVG(UNIX_TIMESTAMP(lpep_dropoff_datetime)-UNIX_TIMESTAMP(lpep_pickup_datetime)) AS avg_duration FROM df GROUP BY passenger_count"),    
    ("Query 9: Average total amount by trip type", "SELECT trip_type, AVG(total_amount) AS avg_amount FROM df GROUP BY trip_type"),
        
# where
    ("Query 3: Total trips with passenger count greater than 4", "SELECT COUNT(*) AS total_trips FROM df WHERE passenger_count > 4"),
    ("Query 5: Total trips with tip amount greater than 10", "SELECT COUNT(*) AS total_trips FROM df WHERE tip_amount > 10"),
    ("Query 8: Total trips with rate code 1", "SELECT COUNT(*) AS total_trips FROM df WHERE RatecodeID = 1"),

# todo couper en deux faire les dense_rank d'un coté et la jointure de l'autre
    ("Query 11: ", "WITH top_destination_origin AS (SELECT \
        DOLocationID  AS dropoff_location, PULocationID AS pickup_location, COUNT(*)/1000 AS trip_count_k,\
        AVG(df.trip_distance) AS avg_trip_distance,AVG(df.passenger_count) AS avg_passenger_count,\
        AVG(df.fare_amount) AS avg_fare_amount,DENSE_RANK() OVER(PARTITION BY DOLocationID ORDER BY COUNT(*) DESC) AS trip_count_rank \
    FROM df GROUP BY dropoff_location, pickup_location), \
    top_destination AS (SELECT\
        DOLocationID  AS dropoff_location,\
        DENSE_RANK() OVER(ORDER BY COUNT(*) DESC) AS trip_count_rank,\
        COUNT(*)/1000 AS trip_count_k\
    FROM df GROUP BY dropoff_location)\
    SELECT\
      tdo.dropoff_location,tdo.pickup_location,tdo.trip_count_k,tdo.avg_trip_distance,tdo.avg_passenger_count,tdo.avg_fare_amount\
    FROM top_destination td \
    INNER JOIN \
    top_destination_origin tdo\
    ON tdo.dropoff_location= td.dropoff_location\
    WHERE td.trip_count_rank IN (1, 2, 3, 4) AND tdo.trip_count_rank IN (1, 2, 3, 4, 5) ORDER BY  \
    td.trip_count_rank,tdo.trip_count_rank")



# jointure
  la compexe du dessus
  transformer la table calculée (position -> location ID, zone quartier.
  transformer cette table en supprimant lat/long et en faisant un unique

  
# passer de green a yellow 

  vendorID => vendor_name
  lpep_pickup_datetime => Trip_Pickup_DateTime
  
  CONCAT(
        LPAD(ROUND(latitude / 10, 0), 4, '0'), -- Arrondir et ajouter les zéros muets
        LPAD(ROUND(longitude / 10, 0), 4, '0') -- Arrondir et ajouter les zéros muets
    ) AS unique_id
    
    
*********  hdfs://mycluster:8020/user/tdp_user/data/nyc_yellow_taxi_trip/yellow_tripdata_2010-01.parquet
*********  hdfs://mycluster:8020/user/tdp_user/data/nyc_yellow_taxi_trip/yellow_tripdata_2010-01.parquet
root
 |-- vendor_id: string (nullable = true)
 |-- pickup_datetime: string (nullable = true)
 |-- dropoff_datetime: string (nullable = true)
 |-- passenger_count: long (nullable = true)
 |-- trip_distance: double (nullable = true)
 |-- pickup_longitude: double (nullable = true)
 |-- pickup_latitude: double (nullable = true)
 |-- rate_code: string (nullable = true)
 |-- store_and_fwd_flag: string (nullable = true)
 |-- dropoff_longitude: double (nullable = true)
 |-- dropoff_latitude: double (nullable = true)
 |-- payment_type: string (nullable = true)
 |-- fare_amount: double (nullable = true)
 |-- surcharge: double (nullable = true)
 |-- mta_tax: double (nullable = true)
 |-- tip_amount: double (nullable = true)
 |-- tolls_amount: double (nullable = true)
 |-- total_amount: double (nullable = true)

pyspark.sql.utils.AnalysisException: Cannot write incompatible data to table 'local.nyc.yellow_taxis_steps':
- Cannot safely cast 'Rate_Code': string to double
- Cannot safely cast 'store_and_forward': string to double
[tdp_user@edge-01 ~]$ /opt/tdp/spark3/bin/spark-submit correct_yellow_data.py 


wget -O zone.zip https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip
unzip zone.zip
hdfs dfs -copyFromLocal taxi_zones.shp /user/tdp_user/data/

pip3 install shapely
et pip3 install fiona

pip3 install pyshp
pip install pyproj pour les conv de coordonnées

pas trop de doc, mais on va juste lire le shapefile

https://towardsdatascience.com/geospatial-operations-at-scale-with-dask-and-geopandas-4d92d00eb7e8

After July 2016, to provide a degree of anonymity when releasing data to the public, the Taxi and Limousine Commission (TLC) only provides the starting and ending “taxi zones” of a trip, and a shapefile that specifies the boundaries

SELECT t1.lat, t1.long, t2.id
FROM table1 AS t1
INNER JOIN table2 AS t2
ON t1.lat = t2.lat AND t1.long = t2.long;


https://geopandas.org/en/stable/getting_started/introduction.html
sur le meme jeux de données !



https://chih-ling-hsu.github.io/2018/05/14/NYC#i-data-analytic-toolpackage-used
https://minimaxir.com/2015/11/nyc-ggplot2-howto/


todo : pour la comparaison, regarder les partitions des tables iceberg. voir 02_Ho_Icebergs_Best_Secret.pdf
https://pt.slideshare.net/databricks/understanding-query-plans-and-spark-uis
https://pt.slideshare.net/doriwaldman/iceberg-introductionpptx

 45 City Island 3375
 
 https://opendata.cityofnewyork.us/
 
 https://data.cityofnewyork.us/Transportation/2009-Yellow-Taxi-Trip-Data/6phq-6kwz
 https://data.cityofnewyork.us/Transportation/2010-Yellow-Taxi-Trip-Data/ry9a-ubra
 
 
 
 +--------+
|VendorID|
+--------+
|       5|
|       6|
|       1|
|       2|
|       4|
|       3|
|     CMT|
|     VTS|
|     DDS|
+--------+

+----------+
|RatecodeID|
+----------+
|       1.0|
|         3|
|        99|
|      null|
|         5|
|         6|
|       5.0|
|       6.0|
|      99.0|
|       4.0|
|       2.0|
|         1|
|         4|
|       3.0|
|         2|
|         7|
|         8|
|        28|
|         0|
|       208|
+----------+
only showing top 20 rows

+------------------+
|store_and_fwd_flag|
+------------------+
|              null|
|                 Y|
|                 N|
|                 0|
|                 1|
|               1.0|
|               0.0|
|               nan|
|                  |
+------------------+

+------------+
|Payment_Type|
+------------+
|           3|
|           0|
|           1|
|           4|
|           2|
|           5|
|         CSH|
|         CRD|
|         CAS|
|         CRE|
|         Cre|
|         Dis|
|         Cas|
|         No |
|         DIS|
|         NOC|
|   No Charge|
|        CASH|
|      Credit|
|        Cash|
+------------+
only showing top 20 rows

 
