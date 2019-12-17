from pyspark import SparkContext, SparkConf

def parse_data(data):
    splitD = data.split(",")
    custId =  splitD[0]
    amount = float(splitD[2])
    return (custId, amount)


conf = SparkConf().setMaster("local").setAppName("TotalAmountSpentByCustomer")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:/Users/pk/Documents/Studies/selflearn/gitspark/assignment01/customer-orders.csv")

# parse and get only required data for analysis (custid, amount)
rdd = lines.map(parse_data).reduceByKey(lambda x, y: x + y)
sortedresult = rdd.map(lambda x: (x[1], x[0])).sortByKey()
totalSpentByCust = sortedresult.collect()

for cust in totalSpentByCust:
    print(""+str(cust[1])+" :  "+ str(cust[0]))

