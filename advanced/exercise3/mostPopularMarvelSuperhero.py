from pyspark import SparkContext, SparkConf

def parse_graph(line):
    splitD = line.split()
    heroID = int(splitD[0])
    relations = len(splitD) - 1
    return (heroID, relations)


#broadcast the hero id and Names
def parse_names():
    heroes = {}
    with open("/Users/pk/Documents/Studies/selflearn/gitspark/advanced/exercise3/Marvel-Names.txt", encoding="ISO-8859-1") as f:
        for line in f:
            splitD = line.split()
            heroes[int(splitD[0])] = splitD[1]
    return heroes

conf = SparkConf().setMaster("local").setAppName("PopularSuperhero")
sc = SparkContext(conf = conf)

# BRoadcast Names of superheroes
superHeroes = sc.broadcast(parse_names())

"""
To find the popularity , we look a super hero appearances with how many
others
"""

# find popularity of superheros
lines = sc.textFile("file:/Users/pk/Documents/Studies/selflearn/gitspark/advanced/exercise3/Marvel-Graph.txt")

rdd = lines.map(parse_graph)
popularity = rdd.map(lambda x : (x[0], x[1])).reduceByKey(lambda x, y: x+y )
sortedPopularity = popularity.map(lambda x: (x[1], x[0])).sortByKey()

popularNames = sortedPopularity.map(lambda x: (superHeroes.value[x[1]], x[0]))

#result = sortedPopularity.collect()
result = popularNames.collect()

for res in result:
    print(res)

# to extarct only the popular most
mostPopular = sortedPopularity.max()
mostPopularName = superHeroes.value[mostPopular[1]]

print("The most popular super hero is " + mostPopularName + " has " + str(mostPopular[0] )+ " appearances")
