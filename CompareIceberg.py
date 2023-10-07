# coding=utf-8
import pyspark
from pyspark import SparkConf
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession
import time
import os
import sys
import re
import io
from  pyspark.sql.types import StructType,StructField,IntegerType,TimestampType,IntegerType,DoubleType,StringType,FloatType
from pyspark.sql.functions import col,concat,lpad,round
from datetime import datetime

import statistics as stats

# Number of run for stat on query, must be > 1
NUM_MES = 10

# Number of Size Test
NUM_RUN = 50


import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

# Nombre de lignes dans le jeu de données
def generate_df(num_rows = 1000):
# Génération de dates uniformément réparties sur une période donnée
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 12, 31)
    date_list = [start_date + timedelta(days=random.randint(0, (end_date - start_date).days)) for  in range(numrows)]

    # Génération de données gaussiennes (normales)
    mean_gaussian = 50  # Moyenne de la distribution gaussienne
    std_deviation_gaussian = 10  # Écart-type de la distribution gaussienne
    gaussian_data = np.random.normal(mean_gaussian, std_deviation_gaussian, num_rows)

    # Génération de données binomiales
    n_binomial = 5  # Nombre d'essais dans la distribution binomiale
    p_binomial = 0.3  # Probabilité de succès dans la distribution binomiale
    binomial_data = np.random.binomial(n_binomial, p_binomial, num_rows)

    n_binomial = 10  # Nombre d'essais dans la distribution binomiale
    p_binomial = 0.3  # Probabilité de succès dans la distribution binomiale
    binomial_data_2 = np.random.binomial(n_binomial, p_binomial, num_rows)


    n_binomial = 100  # Nombre d'essais dans la distribution binomiale
    p_binomial = 0.3  # Probabilité de succès dans la distribution binomiale
    binomial_data_3 = np.random.binomial(n_binomial, p_binomial, num_rows)

    # Création d'un DataFrame avec les données générées
    data = {
    "Date": date_list,
    "value": gaussian_data,
    "low_card": binomial_data,
    "medium_card": binomial_data_2,
    "high_card": binomial_data_3
    }
    return pd.DataFrame(data)


conf = SparkConf() \
        .setAppName("yellow taxis with Iceberg") \
        .setAll([
            ("spark.jars" ,"/opt/tdp/spark3/jars/iceberg-spark-runtime-3.2_2.12-1.3.1.jar"),
            ("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"), \
            ("spark.sql.catalog.spark_catalog","org.apache.iceberg.spark.SparkSessionCatalog"), \
            ("spark.sql.catalog.spark_catalog.type","hive"), \
            ("spark.sql.catalog.local","org.apache.iceberg.spark.SparkCatalog"), \
            ("spark.sql.catalog.local.type","hadoop"), \
            ("spark.sql.catalog.local.warehouse","hdfs:///user/tdp_user/warehouse_hadoop_iceberg"), \
            ("spark.sql.defaultCatalog","local"),
            ("spark.eventLog.enabled","true"),
            ("spark.eventLog.dir","hdfs://mycluster:8020/spark3-logs"),
#             ("spark.eventLog.dir","hdfs:///spark3-logs"),
          ])



# Create a Spark session
spark = SparkSession.builder.config(conf=conf).getOrCreate()



sc = spark.sparkContext
sc.setLogLevel('ERROR')
# Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN


from pyspark.sql import SQLContext
#sqlContext = SQLContext(sc)


# List of queries to execute
queries = [
    "SELECT * WHERE low_card = 5",
    "SELECT * WHERE medium_card = 5",
    "SELECT * WHERE high_card = 5",
    "SELECT low_card, AVG(value) GROUP BY low_card",
    "SELECT medium_card, AVG(value) GROUP BY medium_card",
    "SELECT high_card, AVG(value) GROUP BY high_card",
]

# for debug reduce queries number
#queries = [queries[0]]

size = trips_df.count()

print("mesures as : ")
print("Query name ; size from plan; query (s); std ; explain (s); std; show (s); std; total (s); std\n\n")

res_by_queries = {}

output_buffer = io.StringIO()
original_stdout = sys.stdout

for i in range(NUM_RUN):
    df = generate_df(1000*(i+1))
    df.createOrReplaceTempView("df")
    print(df.describe())
    spark.sql("DROP TABLE  IF EXISTS local.nyc.df PURGE")
    df.writeTo("local.nyc.df").using("iceberg").create()

    #"requete à blanc"
    spark.sql("SELECT COUNT(*) FROM local.nyc.df")

    for query_string in queries:
        print(f"\n\nRunning new test")

        show_duration_iceberg = []
        show_duration_base = []

        for i in range(NUM_MES):
            result = spark.sql(query_string+"FROM local.nyc.df")


            start_time = time.time()
            result.show()
            end_time = time.time()

            show_duration_iceberg.append(end_time - start_time)
            sys.stdout = output_buffer

        sys.stdout = original_stdout

        for i in range(NUM_MES):
            result = spark.sql(query_string + "FROM df")

            start_time = time.time()
            result.show()
            end_time = time.time()

            show_duration_base.append(end_time - start_time)
            sys.stdout = output_buffer

        sys.stdout = original_stdout


        print(query_string, ";", stats.mean(show_duration_iceberg), ";", stats.stdev(show_duration_iceberg), ";",
      stats.mean(show_duration_base), ";", stats.stdev(show_duration_base), ";")




        old_dic = res_by_queries.get(query_string, {})
        old_dic[1000*(i+1)] = f"{1000*(i+1)};{stats.mean(show_duration_iceberg)};{stats.stdev(show_duration_iceberg)};{stats.mean(show_duration_base)};{stats.stdev(show_duration_base)};"
        res_by_queries[query_string] = old_dic

# Stop the Spark session

print("\n\n")
for key, value in res_by_queries.items():
    print("\n", key)
    for k, v in value.items():
        print(v)

spark.stop()


