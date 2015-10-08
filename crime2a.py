from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
import datetime
import csv

sc = SparkContext()
sqlContext = SQLContext(sc)

def csvParse(tup):
    line = tup[0];
    reader = csv.reader( [ line ] );
    return list( reader )[ 0 ];

# Load file, remove header
file = sc.textFile("hdfs://wolf.iems.northwestern.edu/user/huser88/crime/Crimes_-_2001_to_present.csv").zipWithIndex().filter( lambda x: x[1] > 0).map(csvParse)

# Create RDD containing block, and number 1
file1 = file.map(lambda x: (x[3], x[17])).filter(lambda x: x[1] not in "").filter(lambda x: int(x[1]) > 2011).map(lambda x: (x[0], int(1)))

# Sum crimes for each block
file2  = file1.reduceByKey(lambda x,y: x+y)

# Make crime count the key, and then sort on it. Get the top 10 sorted values
file3 = file2.map(lambda x: (x[1], x[0]))
file4 = file3.sortByKey(False).take(10)

print file4
