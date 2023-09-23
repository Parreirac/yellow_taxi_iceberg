import pyspark
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession
from  pyspark.sql.types import StructType,StructField,IntegerType,TimestampType,IntegerType,DoubleType,StringType,FloatType
from pyspark.sql.functions import col


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



# Create a Spark session
spark = SparkSession.builder \
    .appName("NYC Taxi Data Analysis") \
    .getOrCreate()



#you should see the wholeStage codegen stage indexes in the plan.
#spark.conf.set("spark.sql.codegen.wholeStage", "true")  # TODO retester !
#spark.conf.set("spark.sql.parquet.enableVectorizedReader","false")  

sc = spark.sparkContext
#sc.setLogLevel('ERROR')
# Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN


from pyspark.sql import SQLContext
#sqlContext = SQLContext(sc)


# Path to the folder containing the Parquet files of the dataset
#parquet_path = "hdfs:///user/tdp_user/data/nyc_green_taxi_trip/green_tripdata_*.parquet"
#parquet_path = "hdfs:/user/tdp_user/data/nyc_green_taxi_trip/green_tripdata_2021-12.parquet"
parquet_path = "hdfs://mycluster:8020/user/tdp_user/data/nyc_green_taxi_trip/green_tripdata_2021-12.parquet"


#parquet_path = "hdfs://mycluster:8020/user/tdp_user/data/nyc_green_taxi_trip/green_tripdata_201*-*.parquet"
#parquet_path2 = "hdfs://mycluster:8020/user/tdp_user/data/nyc_green_taxi_trip/green_tripdata_2021-11.parquet"


# /user/tdp_user/data/nyc_green_taxi_trip/green_tripdata_2021-12.parquet
# Load Parquet files as a DataFrame

try:
     trips_df1  = spark.read.format("parquet") \
                      .load(parquet_path)
#                     .option("mergeSchema","true").load(parquet_path)

#                     .schema(CustomSchema) \



#     trips_df = spark.read.format("parquet").option("mergeSchema", "true").load(parquet_path)
#     trips_df = spark.read.format("parquet").load(parquet_path)

     print("cast ",parquet_path)

     trips_df = trips_df1.withColumn("RatecodeID", col("RatecodeID").cast("float")) \
                         .withColumn("ehail_fee", col("ehail_fee").cast("float")) \
                         .withColumn("trip_type", col("trip_type").cast("float")) \
                         .withColumn("payment_type", col("payment_type").cast("float"))

#     for col in trips_df.columns:
#           if col in ["RatecodeID","ehail_fee","trip_type","payment_type"]:
#                   trips_df = trips_df.withColumn(col, trips_df[col].cast("int"))

     #print("***",trips_df.columns)
     #trips_df = trips_df.withColumn("RatecodeID", double_cast_udf(col("RatecodeID"))) \
     #                   .withColumn("ehail_fee", double_cast_udf(col("ehail_fee"))) \
     #                   .withColumn("trip_type", double_cast_udf(col("trip_type"))) \
     #                   .withColumn("payment_type", double_cast_udf(col("payment_type")))

#.load(parquet_path)
#                     .option("mergeSchema", "true")\
#                     .schema(CustomSchema)\
# .load(parquet_path)
#,parquet_path2)
     trips_df.createOrReplaceTempView("df")
#    sqlContext.read.parquet(parquet_path) \

except Exception as e:
    print("parquet_path=",parquet_path)
    print("Error reading Parquet file:", str(e))
    raise SystemExit

trips_df.printSchema()

# register temp table so it can be queried with SQL
#    trips_df.registerTempTable("df")
#    trips_df.createOrReplaceTempView("df")

# List of queries to execute
queries = [
#    ("Query 0: Average fare amount by payment type", "SELECT payment_type, AVG(fare_amount) AS avg_fare FROM df GROUP BY payment_type"),
    ("Query 0: Average trip distance by vendorID", "SELECT vendorID, AVG(trip_distance) AS avg_distance FROM df GROUP BY vendorID"),
#    ("Query 1: Total trips by year", "SELECT year, COUNT(*) AS total_trips FROM df GROUP BY year"),
    ("Query 1: Total trips by year", "SELECT YEAR(lpep_pickup_datetime), COUNT(*) AS total_trips FROM df GROUP BY YEAR(lpep_pickup_datetime)"),
    ("Query 2: Average fare amount by payment type", "SELECT payment_type, AVG(fare_amount) AS avg_fare FROM df GROUP BY payment_type"),
    ("Query 3: Total trips with passenger count greater than 4", "SELECT COUNT(*) AS total_trips FROM df WHERE passenger_count > 4"),
    ("Query 4: Average trip distance by vendorID", "SELECT vendorID, AVG(trip_distance) AS avg_distance FROM df GROUP BY vendorID"),
    ("Query 5: Total trips with tip amount greater than 10", "SELECT COUNT(*) AS total_trips FROM df WHERE tip_amount > 10"),
#("Query 6: Total trips by pickup borough", "SELECT pickup_borough, COUNT(*) AS total_trips FROM df GROUP BY pickup_borough"),
    ("Query 6: Total trips by pickup borough", "SELECT PULocationID, COUNT(*) AS total_trips FROM df GROUP BY PULocationID"),
#    ("Query 7: Average trip duration by passenger count", "SELECT passenger_count, AVG(trip_duration) AS avg_duration FROM df GROUP BY passenger_count"),
    ("Query 7: Average trip duration by passenger count", "SELECT passenger_count, AVG(UNIX_TIMESTAMP(lpep_dropoff_datetime)-UNIX_TIMESTAMP(lpep_pickup_datetime)) AS avg_duration FROM df GROUP BY passenger_count"),

#    ("Query 8: Total trips with rate code 1", "SELECT COUNT(*) AS total_trips FROM df WHERE rate_code = 1"),
    ("Query 8: Total trips with rate code 1", "SELECT COUNT(*) AS total_trips FROM df WHERE RatecodeID = 1"),
    ("Query 9: Average total amount by trip type", "SELECT trip_type, AVG(total_amount) AS avg_amount FROM df GROUP BY trip_type"),
#    ("Query 10: Total trips by dropoff borough", "SELECT dropoff_borough, COUNT(*) AS total_trips FROM df GROUP BY dropoff_borough")
    ("Query 10: Total trips by dropoff borough", "SELECT DOLocationID, COUNT(*) AS total_trips FROM df GROUP BY DOLocationID")
    # Add more queries here if needed
]

# Execute queries and measure execution time
for query_name, query_string in queries:
    print(f"\n\nRunning {query_name}")


    #execution_plan = spark.sql(query_string).explain(True)
    #print(execution_plan)

    #print(spark.sql(query_string).explain("cost"))

    #execution_plan_with_cost = "" + spark.sql(query_string).explain("cost")

    #print("ex_p... ", execution_plan_with_cost)

    #print("size in Bytes from plan: ",my_explain(spark.sql(query_string)))



    start_time0 = time.time()
    result = spark.sql(query_string)
    end_time = time.time()


    execution_time = end_time - start_time0
    print(f"Execution Time: {execution_time:.2f} seconds")

    start_time = time.time()
    print(result.explain())
    end_time = time.time()


    execution_time = end_time - start_time
    print(f"Explain Time: {execution_time:.2f} seconds")


    start_time = time.time()
    print("size from plan: ",my_explain(result))
    end_time = time.time()

    execution_time = end_time - start_time
    print(f"Explain cost Time : {execution_time:.2f} seconds")

    # Calculate data size processed in kilobytes (KB)
    #data_size_kb = result.rdd.flatMap(lambda x: x).map(lambda x: len(str(x))).sum() / 1024
    #print(f"Data Size Processed: {data_size_kb:.2f} KB")


    start_time = time.time()
    result.show()
    end_time = time.time()

    execution_time = end_time - start_time0
    print(f"Total Time : {execution_time:.2f} seconds")



# Stop the Spark session
spark.stop()


