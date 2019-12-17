from pyspark import SparkContext,  SparkConf
import collections

def parse_line(line):
    """
    parse the line and return required fields for
    performing analytics

    every entry has
    ITE00100554,18000101,TMAX,-75,,,E,
    [0],        [1],     [2], [3],[4],[5], [6]

    To analyse the minimum temperature by location
    [0] : location
    [1] : date
    [2] : min/ max
    [3] : respective temperature
    .
    .

    We need, [0], [2], [3] fields
    """
    split_record = line.split(",")
    location = split_record[0]
    min_max = split_record[2]
    temp = float(split_record[3])

    return (location, min_max, temp)


conf = SparkConf().setMaster("local").setAppName("minTempByLocation")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:/Users/pk/Documents/Studies/selflearn/gitspark/exercise3/1800.csv")
rdd = lines.map(parse_line)

# Now that your rdd has required data
# 1. retain/filter only min temperatures

max_temperatures = rdd.filter(lambda x:  "TMAX" in x[1])

# 2. Now that you have only min temps, keep only location and temperature reading pair (loc, #reading)
max_temp_pair = max_temperatures.map(lambda x: (x[0], x[2]))

# 3. Reduce by key(location), have the minimum temp for every loc
maxbyloc = max_temp_pair.reduceByKey(lambda x, y: max(x, y))

# 4. let the magic happen! spark action - collect()
result = maxbyloc.collect()

# Print result
for res in result:
    print(""+ res[0]+" : "+str(res[1]))