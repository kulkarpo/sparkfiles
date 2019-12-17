from pyspark import SparkConf, SparkContext
import collections


def parse_rdd(entry):
    temp = entry.split(',')
    age = int(temp[2])
    friends = int(temp[3])
    return (age, friends)


conf = SparkConf().setMaster("local").setAppName("avgfriends")
sc = SparkContext(conf = conf)
rdd = sc.textFile('file:/Users/pk/Documents/Studies/selflearn/sparkfiles/fakefriends.csv')
lines = rdd.map(parse_rdd)

# (age, number_of_friends) tuple => (age, (number of friends, 1))
af = lines.mapValues(lambda x: (x, 1))
# (age_x, (total number of friends, number of age_x))
totalfriendsbyage = af.reduceByKey(lambda x,y : (x[0]+y[0], x[1]+y[1]))

# age_x, (t, n)) => (age, t/n)
avgfriends = totalfriendsbyage.mapValues(lambda x: int(x[0]/x[1]))

results = avgfriends.collect()
# print results
for result in results:
    print(result)
 

    
