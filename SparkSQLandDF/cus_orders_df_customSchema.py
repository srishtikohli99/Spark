#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 30 12:47:58 2020

@author: srishti
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

spark = SparkSession.builder.appName("TotalSpentByCustomer").master("local[*]").getOrCreate()

# Create schema when reading customer-orders
customerOrderSchema = StructType([ \
                                  StructField("cust_id", IntegerType(), True),
                                  StructField("item_id", IntegerType(), True),
                                  StructField("amount_spent", FloatType(), True)
                                  ])

# Load up the data into spark dataset
customersDF = spark.read.schema(customerOrderSchema).csv("../data/customer-orders.csv")

customersDF.groupBy("cust_id").sum("amount_spent").sort("sum(amount_spent)").show(customersDF.count())