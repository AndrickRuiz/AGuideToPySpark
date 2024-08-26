#SETUP

# PySpark is the Spark API for Python.
# We use PySpark to initialize the spark context.

import findspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

findspark.init()

#1. SPARK CONTEXT AND SPARK SESSION

#Creating a spark context class
sc = SparkContext()

#Creating a spark session
spark = SparkSession \
    .builder \
    .appName("Python Spark DataFrames basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

#To work with dataframes we just need to verify that
#the spark session instance has been created.

if 'spark' in locals() and isinstance(spark, SparkSession):
    print("SparkSession is active and ready to use.")
else:
    print("SparkSession is not active. Please create a SparkSession.")

#2. RDDs

#We create an RDD here by calling sc.parallelize(), which has integers from 1 to 30.
data = range(1,30)

#Print first element of iterator
print(data[0])
len(data)
xrangeRDD = sc.parallelize(data, 4)

#This will let us know that we created an RDD.
#It shows "PythonRDD[4] at RDD at PythonRDD.scala:53"
xrangeRDD

#A transformation is an operation on an RDD that results in a new RDD.

subRDD = xrangeRDD.map(lambda x: x-1)
filteredRDD = subRDD.filter(lambda x : x<10)

#A transformation returns a result to the driver.
#We now apply the collect() action to get the output from the transformation.

print(filteredRDD.collect())
filteredRDD.count()

#This shows how to create an RDD and cache it. Notice the 10x speed improvement!
import time 

test = sc.parallelize(range(1,50000),4)
test.cache()

t1 = time.time()
#First count will trigger evaluation of count *and* cache
count1 = test.count()
dt1 = time.time() - t1
print("dt1: ", dt1)


t2 = time.time()
#Second count operates on cached data only
count2 = test.count()
dt2 = time.time() - t2
print("dt2: ", dt2)


#3. DATAFRAMES AND SPARKSQL

#Create an example data file "people.json"
df = spark.read.json("people.json").cache()

#Print the dataframe as well as the data schema
df.show()
df.printSchema()

#Register the DataFrame as a SQL temporary view
df.createTempView("people")

#Select and show basic data columns
df.select("name").show()
df.select(df["name"]).show()
spark.sql("SELECT name FROM people").show()

# Perform basic filtering
df.filter(df["age"] > 21).show()
spark.sql("SELECT age, name FROM people WHERE age > 21").show()

# Perfom basic aggregation of data
df.groupBy("age").count().show()
spark.sql("SELECT age, COUNT(age) as count FROM people GROUP BY age").show()

spark.stop() #will stop the spark session