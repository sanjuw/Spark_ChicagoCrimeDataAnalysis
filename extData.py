from pyspark import SparkConf, SparkContext
import csv
import numpy as np
import datetime as dt

sc = SparkContext()

# Load file and remove header
def csvParse( tup ):
   line = tup[ 0 ];
   reader = csv.reader([line]);
   return list(reader)[0];

#extFile = sc.textFile("hdfs://wolf.iems.northwestern.edu/user/huser88/crime/Chicago_Public_Schools_-_Progress_Report_Cards__2013-2012_.csv").zipWithIndex().filter(lambda tup: tup[1] > 0).map(csvParse);

extFile = sc.textFile("Chicago_Public_Schools_-_Progress_Report_Cards__2011-2012_.csv").zipWithIndex().filter(lambda tup: tup[1] > 0).map(csvParse);
nonmapped_beats = sc.textFile("nonmapped_beats.csv").zipWithIndex().map(csvParse).map(lambda x: (int(x[1]), x[0]))

file1 = sc.textFile("Crimes_-_2001_to_present.csv").zipWithIndex( ).filter( lambda tup: tup[ 1 ] > 0 ).map( csvParse );

# RDD: (police district, count )
schoolsDistrictCount = extFile.map(lambda x: (int(x[77]),1)).reduceByKey(lambda x,y: x+y)

dateAndBeat = file1.map(lambda x: (dt.datetime.strptime(x[2], '%m/%d/%Y %I:%M:%S %p' ), x[10])).filter(lambda x: x[0]<dt.datetime(2015,5,18,0,0))

beatList = dateAndBeat.map(lambda x: x[1]).distinct().sortBy(lambda x: x)

# According to online research, I found that each beat is coded in a way that its first two digits are the police district. This was much more accurate than mapping and doing a distinct on the combinations of beat and district. Seems like there are lots of errors there. 
notmapped = beatList.subtract(schoolCountBeats)

beatAndDistr = beatList.subtract(notmapped).map(lambda x: (int(x[0:2]), x))
beatAndDistr2 = beatAndDistr.union(nonmapped_beats)
# 
schoolCount = beatAndDistr2.join(schoolsDistrictCount).map(lambda x: (x[1][0], x[0])).reduceByKey(lambda x,y: x+y)
schoolCountBeats = schoolCount.map(lambda x: x[0])

with open("beatAndDistrict.txt", 'wb') as f:
    writer = csv.writer(f)
    writer.writerows(test)

