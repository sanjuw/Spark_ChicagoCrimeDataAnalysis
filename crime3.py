from pyspark import SparkConf, SparkContext
import csv
import numpy as np
import datetime as dt
from pyspark.mllib.tree import RandomForest
from pyspark.mllib.util import MLUtils
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.stat import Statistics
from pyspark.mllib.linalg import Vectors

sc = SparkContext()

# Load file and remove header
def csvParse( tup ):
   line = tup[ 0 ];
   reader = csv.reader([line]);
   return list(reader)[0];

file1 = sc.textFile("hdfs://wolf.iems.northwestern.edu/user/huser88/crime/Crimes_-_2001_to_present.csv").zipWithIndex( ).filter( lambda tup: tup[ 1 ] > 0 ).map( csvParse );

extFile = sc.textFile("hdfs://wolf.iems.northwestern.edu/user/huser88/crime/Chicago_Public_Schools_-_Progress_Report_Cards__2011-2012_.csv").zipWithIndex().filter(lambda tup: tup[1] > 0).map(csvParse);

# RDD with date and beat no.
dateAndBeat = file1.map(lambda x: (dt.datetime.strptime(x[2], '%m/%d/%Y %I:%M:%S %p' ), x[10])).filter(lambda x: x[0]<dt.datetime(2015,5,18,0,0))

# Filter out dates > May 17th, 2015 to keep full weeks 
#predictors = dateAndBeat.map(lambda x: (x[1], x[0].year, x[0].month, x[0].weekday(), x[0].hour))

# Aggregate the crime count over (year, week, beat)
yearWeekBeatCount = dateAndBeat.map(lambda x: ((x[0].year, x[0].isocalendar()[1], x[1]), 1)).reduceByKey(lambda x,y: x+y).cache()

# Take all possible combinations of (year, week, beat)
# Then get list of such combination missing from the current set
# Append these missing combinations in the final set with a count of 0.

# List of all the beats
beatList = dateAndBeat.map(lambda x: x[1]).distinct().sortBy(lambda x: x)

# All year-week combinations in the dataset
allYrWeeks = yearWeekBeatCount.map(lambda x: (x[0][0], x[0][1])).distinct();

# Get the list of all year-week-beat combinations, and see which combinations are missing in our dataset. 
# Set those counts equal to 0
allYrWeekBeatCombo = beatList.cartesian(allYrWeeks).map(lambda x: (x[1][0], x[1][1], x[0]))
missingCombo = allYrWeekBeatCombo.subtract(yearWeekBeatCount.keys()).distinct()
superSet = yearWeekBeatCount.union(missingCombo.map(lambda x: (x,0)))
superSet.persist()


### External Dataset ###
# RDD: (police district, count )
nonmapped_beats = sc.textFile("nonmapped_beats.csv").zipWithIndex().map(csvParse).map(lambda x: (int(x[1]), x[0]))
# Count of the number of schools per district
schoolsDistrictCount = extFile.map(lambda x: (int(x[77]),1)).reduceByKey(lambda x,y: x+y)

# According to online research, I found that each beat is coded in a way that its first two digits are the police district. This was much more accurate than mapping and doing a distinct on the combinations of beat and district. Seems like there are lots of errors there.
# However, there were 18 missing beats that could not be mapped, so had to map them manually. THese are in the nonmapped_beats RDD.
# RDD: (police district, count )
nonmapped_beats = sc.textFile("nonmapped_beats.csv").zipWithIndex().map(csvParse).map(lambda x: (int(x[1]), x[0]))

# RDD: (district (beat, count))
notmapped = nonmapped_beats.map(lambda x:x[1])

beatAndDistr = beatList.subtract(notmapped).map(lambda x: (int(x[0:2]), x))
beatAndDistr2 = beatAndDistr.union(nonmapped_beats)
# RDD: (beat, count)
schoolCount = beatAndDistr2.join(schoolsDistrictCount).map(lambda x: (x[1][0], x[1][1]))

# Join with the base set
# baseData RDD: beat, (week, count)
baseData = superSet.map(lambda x: (x[0][2], (x[0][1], x[1])))
# joinedData RDD: beat, ((week, count), num_schools) => (count, (week, beat, num_schools))
joinedData = baseData.join(schoolCount).map(lambda x: (x[1][0][1], (x[1][0][0], x[0], x[1][1]))).cache()

### Predictive Analysis ###
# (crime count, (week, beat, number of schools))
# Basically, got rid of year, but did not aggregate. 
# This way we have multiple entries (more data points) per week-beat combo. 
#predArray = superSet.map(lambda x: (x[1], (x[0][1], x[0][2])))

# Dictionary to map week to an index
weekDict = dict(zip(range(1,54), range(0,53)))
# List of all the beats
# Dictionary mapping each beat to an index. Useful when converting to LabeledPoint. Otherwise converts to numeric.
beatsDict = dict(beatList.zipWithIndex().map(lambda x: (x[0],x[1])).collect())

# Data points as LabeledPoints
# (crime count, [beat, week])
predArrayLP = joinedData.map(lambda x: LabeledPoint(x[0], [weekDict[x[1][0]], beatsDict[x[1][1]], x[1][2]]))

# Split into training and testing set. 70-30 split.
(train, test) = predArrayLP.randomSplit([0.7, 0.3])

# Feature categories : 
featuresCat = {0: len(beatsDict), 1: 53}
maxBins = max(len(beatsDict),len(weekDict))

model = RandomForest.trainRegressor(train, categoricalFeaturesInfo=featuresCat,
                                    numTrees=10, featureSubsetStrategy="auto",
                                    impurity='variance', maxDepth=5, maxBins=maxBins)


# Evaluate model on test instances and compute test error
predictions = model.predict(test.map(lambda x: x.features))
#rschoolCountBeats = schoolCount.map(lambda x: x[0])
predOutput = predictions.collect()
labelsAndPredictions = test.map(lambda lp: lp.label).zip(predictions)
testMSE = labelsAndPredictions.map(lambda (v, p): (v - p) * (v - p)).sum() / float(test.count())
print('Test Mean Squared Error = ' + str(testMSE))

### Write output to file ###
with open("predictions.txt", 'wb') as f:
    writer = csv.writer(f)
    writer.writerows(predOutput)
