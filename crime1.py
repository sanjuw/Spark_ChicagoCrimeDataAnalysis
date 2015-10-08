from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *
import datetime
import csv

sc = SparkContext()
sqlContext = SQLContext(sc)

def csvParse( tup ):
    line = tup[ 0 ];
    reader = csv.reader( [ line ] );
    return list( reader )[ 0 ];



# Load File, remove header, and parse
file = sc.textFile("hdfs://wolf.iems.northwestern.edu/user/huser88/crime/Crimes_-_2001_to_present.csv").zipWithIndex().filter(lambda x: x[ 1 ] > 0 ).map(csvParse)

# Create RDD with year and month
file1 = file.map(lambda x: Row(id=x[0], date=x[2])).cache()

# Prepare for sql queries
headers = "date id"
fields = [StructField(field_name, StringType(), True) for field_name in headers.split()]
schema = StructType(fields)

schema_file = sqlContext.applySchema(file1, schema)
schema_file.registerTempTable("crime1")

# Get monthly average crime rate
crimeByMonth = sqlContext.sql("SELECT substr(date, 0,2), COUNT(id)/COUNT(DISTINCT substr(date,7,4)) AS avgCrimeCnt FROM crime1 GROUP BY substr(date,0,2)")

# Print output to screen
for m in crimeByMonth.collect():
	print m

