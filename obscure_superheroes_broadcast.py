#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 30 15:10:57 2020

@author: srishti
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("data/Marvel-names.txt")
lines = spark.read.text("data/Marvel-graph.txt")
#lines.show()


## agg because alias
## agg allows multiple aggregations, alias can be considered agg
connections = lines.withColumn("id", func.split(lines.value, " ")[0])\
              .withColumn("connections", func.size(func.split(func.col("value"), " ")) -1)\
              .groupBy("id")\
              .agg(func.sum("connections")\
              .alias("connections"))
             
connections.show()               

## first returns 1st row
"""
n = connections.sort("connections").first()[1]
name = connections.filter(func.col("connections") == n)
name.join(names,"id").show()
#names.filter(name[0].contains(func.col("id"))).show()
"""