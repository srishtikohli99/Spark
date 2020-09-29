#for all simple boilerplate imports
from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")  #set local machine as master node no distribution as fo now
#for job to identify in the UI app name
sc = SparkContext(conf = conf)

lines = sc.textFile("data/ml-100k/u.data")
#line is rdd.. sc.textFile reads line by line the mentioned dataset and stores in lines
#lines consist of (no. of lines in u.data) values and each value is a string
ratings = lines.map(lambda x: x.split()[2])
# will give rating (at index 2 after split of each value)
#ratings will have only ratings
result = ratings.countByValue()
#counts number of each rating values // like groupby..how many times each unique value in rdd occurs

sortedResults = collections.OrderedDict(sorted(result.items())) #sorting based on key..just python no spark here
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
