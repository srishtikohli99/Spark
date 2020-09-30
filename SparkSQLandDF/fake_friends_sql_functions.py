#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 30 09:59:40 2020

@author: srishti
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("../data/fakefriends-header.csv")
    
print("Here is our inferred schema:")
people.printSchema()

### functions can be applied on DF

people.groupBy("age").avg("friends").show()
people.groupBy("age").avg("friends").sort("age").show()


##imported as func to avoid any clash between python and pyspark funcs
people.groupBy("age").agg(func.sum("friends")/func.count("friends")).sort("age").show()


##name a column for later use

people.groupBy("age").agg((func.sum("friends")/func.count("friends")).alias("friends_avg")).sort("friends_avg").show()




spark.stop()