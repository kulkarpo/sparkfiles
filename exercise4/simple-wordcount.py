from pyspark import SparkContext, SparkConf
import collections
"""
Most brute-force approach: 
to get the word count, you should split the whole text into words
its assumed that split by space is giving word count 

also in cases of punctuations like
hello! :1 
hello, :1 
hello :1
"""
conf = SparkConf().setMaster("local").setAppName("Simple-Wordcount")
sc = SparkContext(conf = conf)



lines = sc.textFile("file:/Users/pk/Documents/Studies/selflearn/gitspark/exercise4/Book.txt")
words = lines.flatMap(lambda x: x.split())

# to get frequency for every word -> countByValue()
freq = words.countByValue()

# print
for word, fre in freq.items():
    # ignore if any word in the textfile is encoded other than ascii
    validword = word.encode('ascii', 'ignore')
    # check after encoding if the word is not null
    if(validword):
        print(validword.decode() + " :   " + str(fre))





