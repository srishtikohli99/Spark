#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 29 19:55:17 2020

@author: srishti
"""

from pyspark import SparkConf, SparkContext

def normalizeWords(line):
    fields = line.split(',')
    return (int(fields[0]),float(fields[2]))

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

lines = sc.textFile("data/customer-orders.csv")
orderDetails = lines.map(normalizeWords)
sumOrders = orderDetails.reduceByKey(lambda x,y: x+y)
sortedResults = sumOrders.map(lambda x: (x[1],x[0])).sortByKey()

results = sortedResults.collect()

for result in results:
    print(str(result[1])+ ":\t\t" + str(result[0]))

