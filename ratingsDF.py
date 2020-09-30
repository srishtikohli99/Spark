#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 30 12:58:54 2020

@author: srishti
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

schema = StructType([ \
                    StructField("userID", IntegerType(), True),\
                    StructField("movieID", IntegerType(), True),\
                    StructField("rating", IntegerType(), True),\
                    StructField("timestamp", LongType(), True)])
    
movieDF = spark.read.option("sep","\t").schema(schema).csv("data/ml-100k/u.data")
##desc for descending
topMovieIds = movieDF.groupBy("movieID").count().orderBy(func.desc("count")).show(10)

spark.stop()