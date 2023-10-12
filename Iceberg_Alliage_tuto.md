# Iceberg for Spark, Performance Comparison Tutorial

## Introduction
In the previous tutorial, you learned how to install Iceberg for Spark, and you tested some scala commands. In this tutorial, we'll explore its performance against standard Spark tables using Python. 

## Dataset Generation
We'll start by generating a synthetic dataset with the following columns:
- A Gaussian value
- Three categorical variables with varying cardinalities, obtained from binomial distributions

The dataset generation function is as follows:

```python
def gen_binomial(n,p):
    return sum(1 for _ in range(n) if random.random() < p)

data = [( random.normalvariate(50,10),
          gen_binomial(4,.5),
		  gen_binomial(50,.5),
		  gen_binomial(100,.5)) for i in range(int(current_size))]

schema = StructType([StructField("Value", FloatType(), False),
                     StructField("low_card", IntegerType(), False),
					 StructField("medium_card", IntegerType(), False),
					 StructField("high_card", IntegerType(), False)])
base = spark.createDataFrame(data, schema)
```

We estimate the dataset size to be around 16 bytes per row. To quickly change the dataset's size, we'll add its current content at each iteration, effectively doubling the dataset size. This approach allows us to observe how performance scales with dataset size.

## SQL Querying
To query this dataset using SQL, we need to create a temporary view:

```python
base.createOrReplaceTempView("view_name")
```

To create an Iceberg table, we use the following command:

```python
spark.sql("DROP TABLE IF EXISTS local.nyc.df PURGE")
base.writeTo("local.db.table").using("iceberg").create()
```
It is crucial to avoid using TRUNCATE in this context because it creates an empty snapshot, but the old data remains in the table, which can fill up your hard disk.

We will run a set of sample SQL queries on our dataset, testing the performance of Iceberg versus Spark default format. Here are the sample queries:

```python
queries = [
    ("Q1", "SELECT *", "WHERE low_card = 2 "),
    ("Q2", "SELECT *", "WHERE medium_card = 25 "),
    ("Q3", "SELECT *", "WHERE high_card = 50 "),
    ("Q4", "SELECT low_card, AVG(value)", "GROUP BY low_card "),
    ("Q5", "SELECT medium_card, AVG(value)", "GROUP BY medium_card "),
    ("Q6", "SELECT high_card, AVG(value)", "GROUP BY high_card "),
]
```

## Parameters
We will execute these queries in loops and measure the query execution times. The key parameters used are:
- `NUM_ROWS`: The number of rows in the primary dataset.
- `NUM_RUN`: The number of times the dataset size changes.
- `NUM_MES`: The number of times each query is repeated for measurement (must be greater than 1).

The source code is avalable [script](here).


Before launching a spark-submit, remember to correctly set the environment variables 
```bash
export HADOOP_CONF_DIR=etchadoopconf
export SPARK_HOME=opttdpspark3
/opt/tdp/spark3/bin/spark-submit Compare.py | tee results.txt
```

## Performance Testing

With the default parameter values, the calculation takes 3 minutes on a decent laptop. For each query we give the duration for spark (default table format) divided by the duration for the Iceberg table (other statistics are also measured). An extract of the results is given below.




| Size (bytes) | Q1 | Q2 | Q3 | Q4  | Q5 | Q6  |
|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
|2400000 |0.8648 |0.7920|0.7351|2.1665|2.1718 | 2.5051
4800000 | 1.1848 |1.0124|1.1924|3.0267 |3.1304|2.8520
9600000 | 1.5971 |1.4240|1.3928|3.0736 |4.3235|4.5640
19200000 |1.5474 |1.9046|1.4480|3.6401 |4.9791 |5.9599


For a small table, `WHERE` is a little slower, `GROUP BY` is already twice as fast, Iceberg is of little use.
For medium tables, `WHERE` becomes a little faster, while `GROUP BY` becomes noticeably faster than for a default table.
We can observe that most of the time a `GROUP BY` with a large cardinality increases the gain.

The `WHERE` benefit from data pruning, while the `GROUP BY` is marked by the bloom filter which is all the more efficient as the cardinality is large.




## Conclusion
The tutorial aims to help you understand Iceberg's performance benefits compared to Spark tables. Note that running memory-intensive tasks on small clusters can lead to memory saturation, so it's essential to monitor memory usage and avoid computationally intensive operations. Spark recommends keeping tasks under 1 GB in size to ensure smooth execution.

Feel free to modify parameters and explore different scenarios to better understand how Iceberg can enhance your large-scale data processing pipelines. For example you can perform multiple experiments: 
* adjust settings, 
* disable garbage collection (GC), 
* save and check the size of the dataset on disk, 
* modify queries,
* measure the impact of the number of snapshots when creating the Iceberg table.
> Note : when working with large datasets, you may need to adjust system parameters to accommodate higher memory requirements, such as changing the system's overcommit memory setting:
```bash
sudo su
echo 1 > /proc/sys/vm/overcommit_memory
```



