
#from pyspark import SparkConf, SparkContext

###some code for self


"""
import pandas as pd
import numpy as np

df = pd.read_csv("data/1800.csv")
listi =[]
for i in range(len(df)):
    if df.iloc[i, 2]=="TMIN":
        listi.append(df.iloc[i, 0])
        print("here")
    
print(np.unique(np.array(listi)))
"""     

 

"""
conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

lines = sc.textFile("data/1800.csv")
parsedLines = lines.map(parseLine)
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))
results = minTemps.collect();

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
"""

### my code
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)


def parseLine(line):
    fields = line.split(",")
    stationID = fields[0]
    fieldType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, fieldType, temperature)

lines = sc.textFile("../data/1800.csv")
parsedLines = lines.map(lambda x: parseLine(x))
minTemps = parsedLines.filter(lambda x: x[1] == "TMIN")
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
minTemps = stationTemps.reduceByKey(lambda x,y: min(x,y))
print(minTemps.collect())