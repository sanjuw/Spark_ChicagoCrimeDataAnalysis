from pyspark import SparkConf, SparkContext
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.stat import Statistics
import csv
import numpy as np

sc = SparkContext()

# Load file and remove header
#file = sc.textFile("hdfs://wolf.iems.northwestern.edu/user/huser88/crime/Crimes_-_2001_to_present.csv")

def csvParse( tup ):
   line = tup[ 0 ];
   reader = csv.reader([line]);
   return list(reader)[0];

# Get count of beat,year combination
# key: beat, year. value: 1
file1 = sc.textFile("hdfs://wolf.iems.northwestern.edu/user/huser88/crime/Crimes_-_2001_to_present.csv").zipWithIndex( ).filter( lambda tup: tup[ 1 ] > 0 ).map( csvParse );

beatYrComboCount = file1.map(lambda x: ( (x[10], int(x[17])) , 1)).reduceByKey(lambda x,y: x + y).cache()

# List of beat-yr combos
beatYr = beatYrComboCount.keys().flatMap(lambda x: [(x[0], y) for y in range(2001,2016)])

# Find missing beats and union with non-missing to create full set
missing = beatYr.subtract(beatYrComboCount.keys()).distinct()
allCrimeCnts = beatYrComboCount.union(missing.map(lambda x: (x,0)))

file_cnts = allCrimeCnts.map(lambda x: (x[0][1], (x[0][0], x[1]))).groupByKey().mapValues(lambda x: sorted(list(x),key = lambda x: x[0])).cache()

# List of beats
beats = [element[0] for element in file_cnts.values().first()]
vectorCnts = file_cnts.values().map(lambda x: Vectors.dense([ element[1] for element in x]))

cor = Statistics.corr(vectorCnts, method='pearson')


cor.flags['WRITEABLE'] = True;
np.fill_diagonal(cor, 0.0)

# Get top 10 correlation values from matrix
sorted = cor.argsort(axis=None)
ind = np.unravel_index(sorted[-20::2], cor.shape)

mostCorrBeatPairs = [(beats[i], beats[j]) for i,j in zip(ind[0], ind[1])]

for i,j in mostCorrBeatPairs:
	print i,j


