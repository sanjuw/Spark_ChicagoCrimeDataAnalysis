from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.mllib.stat import Statistics
import csv
import datetime
import math

sc = SparkContext()
sqlContext = SQLContext(sc)

# Parse file using CSV parser
def csvParse(tup):
   line = tup[0];
   reader = csv.reader([line]);
   return list( reader )[0];

#Load file and remove header
file = sc.textFile("hdfs://wolf.iems.northwestern.edu/user/huser88/crime/Crimes_-_2001_to_present.csv").zipWithIndex( ).filter(lambda x: x[1] > 0 ).map(csvParse)

# Filter out districts that don't exist
# Convert district to int to combine '012' and '12'
file1 = file.filter(lambda x: x[11] not in ["", "031", "023", "021", "013"]).map(lambda x: (int(x[11]), int(x[17]), int(1)))

# Create separate RDD's for Daly and Emanuel
daly = file1.filter(lambda x: x[1] <= 2011).map(lambda x: (x[0], x[2])).cache()
emanuel  = file1.filter(lambda x: x[1] > 2011).map(lambda x:(x[0], x[2])).cache()

# Find average number of crimes by District for both Daly and Emanuel
d1 = daly.reduceByKey(lambda x,y: (x+y))
e1 = emanuel.reduceByKey(lambda x,y: (x+y))

d1_avg = d1.map(lambda x: (x[0], int(x[1]/11)))
e1_avg = e1.map(lambda x: (x[0], int(x[1]/4)))

# Combine Daly and Emanuel data for comparisons
combined = d1_avg.join(e1_avg) 
combined.saveAsTextFile("hdfs://wolf.iems.northwestern.edu/user/huser88/combined")
# Do a Paired t-test to check how significantly different the two distributions are
# Used the following link as reference : http://www.cliffsnotes.com/math/statistics/univariate-inferential-tests/paired-difference-t-test

diff = combined.map(lambda x: int(x[1][0]) - int(x[1][1]))
n = diff.count()
meandiff = diff.sum()/n

sum_s_sq = diff.map(lambda x: pow((x-meandiff),2)).sum()
std_dev = math.sqrt(sum_s_sq/(n-1))

t = meandiff/std_dev
print t
