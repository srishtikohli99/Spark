#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 30 11:14:36 2020

@author: srishti
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

df = spark.read.text("../data/Book")
#print(type(df))

#df.show()
#df.value means df=dataframe df and value is the column name by default
words = df.select((func.explode(func.split(df.value, "\\W+")).alias("word")))
#words.show()

#now word because we named alias word ... if not set, col is the name
#lowercaseWords = words.select(func.lower(words.col).alias("word"))
lowercaseWords = words.select(func.lower(words.word).alias("word"))
result = lowercaseWords.groupBy("word").count().alias("count").sort("count")
result.show(result.count())

                        
#print(words)  
#print(df.iloc[:,:])       
    
#words.printSchema()
spark.stop()