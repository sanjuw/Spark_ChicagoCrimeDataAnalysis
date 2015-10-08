from pyspark import SparkContext
import datetime
import csv

sc = SparkContext()
def csvParse(tup):
    line = tup[0];
    reader = csv.reader( [ line ] );
    return list( reader )[ 0 ];

# Load file, remove header
file = sc.textFile("hdfs://wolf.iems.northwestern.edu/user/huser88/crime/Crimes_-_2001_to_present.csv").zipWithIndex().filter( lambda x: x[1] > 0).map(csvParse)

# Filter to keep crimes with arrests
crime_arrests = file.filter(lambda x: x[8]=="true")

# Create RDD with date and count
dates = crime_arrests.map(lambda x: (datetime.datetime.strptime(x[2], '%m/%d/%Y %I:%M:%S %p' ))).cache()

# Monthly Arrests
arrestsMonthly = dates.map(lambda x: x.month).histogram(list(range(1,14)))

# Hourly Arrests
arrestsHourly = dates.map(lambda x: x.hour).histogram(list(range(0,25)))

# Weekly Arrests
arrestsWeekly = dates.map(lambda x: x.weekday()).histogram(list(range(0,8)))

with open('crime4.txt', "w") as out_file:
	out_file.write("Arrests By Month")
	out_file.write(m)

out_file.close()
