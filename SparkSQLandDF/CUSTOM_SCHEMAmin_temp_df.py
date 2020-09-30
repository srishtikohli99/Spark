#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 30 12:03:37 2020

@author: srishti
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()


#name, type, nulllable = true
#1,2,3,4 columns
schema = StructType([ \
                     StructField("stationID", StringType(), True), \
                     StructField("date", IntegerType(), True), \
                     StructField("measure_type", StringType(), True), \
                     StructField("temperature", FloatType(), True)])

df = spark.read.schema(schema).csv("../data/1800.csv")
df.printSchema()

minTemps = df.filter(df.measure_type == "TMIN")
minTemps.groupBy("stationID").min("temperature").show()

##agg when some op to be applied in every row of that aggregation
minTemps.groupBy("stationID").agg(func.min(minTemps.temperature* 0.1 * (9.0 / 5.0) + 32.0).alias("temperature")).sort("temperature").show()
#minTempsByStation.show()

spark.stop()