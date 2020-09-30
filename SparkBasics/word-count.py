from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("../data/Book")
words = input.flatMap(lambda x: x.split())
print(words.take(5))
print("----------------------------------------------")
print(input.map(lambda x: x.split()).take(5))
wordCounts = words.countByValue()
print("----------------------------------------------")
print(type(words.collect()))


for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))