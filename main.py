from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext
import math
import datetime

# create string identifier for a GPS Cell (a kilometer-rounded latitude and longitude)
def getCellID(lat, lon):
	return (str(round(lat, 2)) + "-"+str(round(lon, 2)))
	
# map a timestamp to a week day
def mapToDay(val):
	t = datetime.datetime.strptime(val[0],'%Y-%m-%d %H:%M:%S')
	if(t.weekday() == 6):
		return ('Sunday', val[1])
	else:
		return ('Other', val[1])
	
# check if timestamp provided is between 8 and 11 AM
def isInCorrectTimeRange(val):
	t = datetime.datetime.strptime(val,'%Y-%m-%d %H:%M:%S')
	if(t.hour >= 8 and t.hour <= 11):
		return True
	else:
		return False

# check if value is a floating point number
def isfloat(value):
	try:
		float(value)
		return True
	except:
		return False

if __name__ == "__main__":
	if len(sys.argv) != 5:
		print("Usage: job <file> <file> <output> ", file=sys.stderr)
		exit(-1)
		
	sc = SparkContext(appName="TaxiDataAnalysis")

	taxiLines = sc.textFile(sys.argv[1])

	splitted = taxiLines.map(lambda x: x.split(",")) # split each record by comma
	splittedCleaned = splitted.filter(lambda x: len(x) == 17) # remove incomplete records

	dataWithGPS = splittedCleaned.filter(lambda x: isfloat(x[9]) and isfloat(x[8])).filter(lambda x: float(x[9]) != 0 and float(x[8]) != 0 ) # remove samples missing GPS
	data = dataWithGPS.map(    lambda x: (  x[3], getCellID(float(x[9]),float(x[8]))  )    )

	dataFiltered = data.filter(lambda x: isInCorrectTimeRange(x[0]))

	dataKeyed = dataFiltered.map(mapToDay)
	# dataKeyed.countByKey() # check distribution of data (-> fewer trips on sunday morning that during other days of the week, it seems)

	sundayTrips = dataKeyed.filter(lambda x: x[0] == 'Sunday')
	otherTrips = dataKeyed.filter(lambda x: x[0] == 'Other')

	topSundayTrips = sundayTrips.map( lambda x: (x[1],1) ).reduceByKey(lambda a,b : a+b).map(lambda (a,b):(b,a)).top(20)

	topOtherTrips = otherTrips.map( lambda x: (x[1],1) ).reduceByKey(lambda a,b : a+b).map(lambda (a,b):(b,a)).top(20)

	poiLines = sc.textFile(sys.argv[2])
	splittedPOI = poiLines.map(lambda x: x.split("||"))
	splittedCleanedPOI = splittedPOI.filter(lambda x: isfloat(x[0]) and isfloat(x[1])).filter(lambda x: len(x) == 4)
	keyedPOI = splittedCleanedPOI.map(   lambda x: ( getCellID(float(x[0]),float(x[1])), x[2])    )
	keyedPOI.take(1)

	topSundayTripsRDD = sc.parallelize(topSundayTrips).map(lambda (a,b):(b,a))
	finalRes1 = topSundayTripsRDD.leftOuterJoin(keyedPOI).map(lambda (a,b): (a,b[1])).groupByKey().mapValues(list).join(topSundayTripsRDD).mapValues(lambda (a,b): (b,a)).collect() # removes all the nbDropOffs and joins again with (very small - 20 samples) topSundayTrips to add it only once
	sc.parallelize(finalRes1,1).saveAsTextFile(sys.argv[3])
	
	topOtherTripsRDD = sc.parallelize(topOtherTrips).map(lambda (a,b):(b,a))
	finalRes2 = topOtherTripsRDD.leftOuterJoin(keyedPOI).map(lambda (a,b): (a,b[1])).groupByKey().mapValues(list).join(topOtherTripsRDD).mapValues(lambda (a,b): (b,a)).collect() # removes all the nbDropOffs and joins again with (very small - 20 samples) topSundayTrips to add it only once
	sc.parallelize(finalRes2,1).saveAsTextFile(sys.argv[4])
	
	sc.stop()
