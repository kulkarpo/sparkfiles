from pyspark import SparkConf, SparkContext
import re
import collections

def normalize_text(text):
    """
    1. fix case issues with words -> convert everything to lowercase
    2. split
    3. consider only words: one or more alphabets [\W+] (ignore punctuations)
    """
    res_word = re.compile(r'\W+', re.UNICODE).split(text.lower())
    return res_word


conf = SparkConf().setMaster("local").setAppName("Word-count-with-text-normalization-in-its-simplest-way")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:/Users/pk/Documents/Studies/selflearn/gitspark/exercise4/Book.txt")
words = lines.flatMap(normalize_text)

freq = words.countByValue()

#sort the results
sorted_words = collections.OrderedDict(sorted(freq.items()))
for word, fre in sorted_words.items():
    print(word + " : " + str(fre))