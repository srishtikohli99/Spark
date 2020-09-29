from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

def parseLine2(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return numFriends

lines = sc.textFile("data/fakefriends.csv")
rdd = lines.map(parseLine)
#print(rdd.count())
#print("--------------------------")
#print(rdd.mapValues(lambda x: (x, 1)).countByValue())
#print("--------------------------")

print("------------------------------")
print(rdd.take(1))
print(lines.map(parseLine2).take(1))
print(rdd.mapValues(lambda x: (x, 1)).take(1))
print("------------------------------")


totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
results = averagesByAge.collect()
for result in results:
    print(result)


#print statements are explanatory.