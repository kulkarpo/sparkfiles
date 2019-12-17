from pyspark import SparkContext, SparkConf

def parse_data(data):
    splitset = data.split()
    return splitset[1]

conf = SparkConf().setMaster("local").setAppName("TotalAmountSpentByCustomer")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:/Users/pk/Documents/Studies/selflearn/gitspark/advanced/exercise1/ml-100k/u.data")

movieset = lines.map(parse_data)

#counting
temp = movieset.map(lambda x: (x, 1)).reduceByKey(lambda x,y : x + y)
sortedMovieSet = temp.map(lambda x: (x[1], x[0])).sortByKey()
result = sortedMovieSet.collect()


for res in result:
    print(res)
