from pyspark import SparkContext, SparkConf
import collections
import re

def normalize_text(text):
    words = re.compile(r'\W+', re.UNICODE).split(text.lower())
    return words

conf = SparkConf().setMaster("local").setAppName("Word-Count-the-hard-but-right-way-using-spark")
sc = SparkContext(conf = conf)

rdd_lines = sc.textFile("file:/Users/pk/Documents/Studies/selflearn/gitspark/exercise4/Book.txt")
words = rdd_lines.flatMap(normalize_text)

# instead of using conntByValue() use map and reduceByKey to find frequency
temp = words.map(lambda x: (x, 1)).reduceByKey(lambda x,y: x + y)
#temp1 = temp.reduceByKey(lambda x,y: x + y)

# Now to easily browse the most frequent and least frequent words at a  glance

sorted_res = temp.map(lambda x: (x[1], x[0])).sortByKey()
results = sorted_res.collect()

for result in results:
    print(""+result[1] + " : " + str(result[0]))


