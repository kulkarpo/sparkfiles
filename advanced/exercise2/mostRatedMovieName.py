from pyspark import SparkContext, SparkConf

def parse_data(data):
    splitset = data.split()
    return splitset[1]

# method to formulate required data to be broadcasted
def parseMoviename():
    movies = {} # init empty dict
    with open("/Users/pk/Documents/Studies/selflearn/gitspark/advanced/exercise1/ml-100k/u.item", encoding = "ISO-8859-1") as f:
        for line in f:
            splitset = line.split('|')
            movies[int(splitset[0])] = splitset[1]
    return movies

conf = SparkConf().setMaster("local").setAppName("PopularMoviesName")
sc = SparkContext(conf = conf)

# before starting to parse , broadcast movie name data to all the nodes
movieDict = sc.broadcast(parseMoviename())

# Now, find the popularity of the movies
lines = sc.textFile("file:/Users/pk/Documents/Studies/selflearn/gitspark/advanced/exercise1/ml-100k/u.data")
rdd = lines.map(parse_data)

popularMovies = rdd.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
sortedPopularMovies = popularMovies.map(lambda x: (x[1], int(x[0]))).sortByKey()

sortedPopularMoviesnames = sortedPopularMovies.map(lambda x: (movieDict.value[x[1]], x[0]))

#result = sortedPopularMovies.collect()
result = sortedPopularMoviesnames.collect()

for res in result:
    print(res)