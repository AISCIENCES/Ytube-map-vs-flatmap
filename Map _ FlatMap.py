# Databricks notebook source
from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("Map vs FlatMap")
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

# MAP
rdd = sc.textFile("/FileStore/tables/number.csv")
rdd = rdd.map(lambda x: int(x) + 1)
rdd = rdd.filter(lambda x: x > 3 )
rdd.collect()

# COMMAND ----------

# FLAT MAP
rdd = sc.textFile("/FileStore/tables/number2.csv")
rdd = rdd.flatMap(lambda x: x.split(','))
rdd = rdd.map(lambda x: int(x) + 1)
rdd = rdd.filter(lambda x: x > 3 )
rdd.collect()
