#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 29 22:05:49 2020

@author: srishti
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()


#reading as dataframe
people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("data/fakefriends-header.csv")
    
    
# dataframe operation
people.printSchema()

##register dataframe as a table
people.createOrReplaceTempView("people")


## Native sql can be applied on dfs registered as table
results = spark.sql("SELECT age, int(sum(friends)/count(friends)) FROM people GROUP BY age ORDER BY age")


#result of sql is RDD
for result in results.collect():
    print(result)
    
spark.stop()