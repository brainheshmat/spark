
#spark-submit --master spark://IP:7077 pi.py 1
from pyspark import SparkConf, SparkContext
from operator import add
import sys

## Constants

APP_NAME = " Querying PCMD files"
#filename = "hdfs://ec2-52-207-247-64.compute-1.amazonaws.com:9000/user/15061021.PCMD"
filename = "file:///tmp/mme1/2015-07-07.14.*.-0800.MMEpcmd.geo"
##OTHER FUNCTIONS/CLASSES

conf = SparkConf().setAppName(APP_NAME)
#conf = conf.setMaster("local[*]")
sc   = SparkContext(conf=conf)
## Main functionality
rdd = sc.textFile(filename) 
r = rdd.filter(lambda line: (line.split(';')[8]=="1467"))
r.cache()
m = r.count()
print "m1", m
m2 = r.count()
print "m2", m2

print r.first()

# def main(sc):
# 	rdd = sc.textFile(filename) 
# 	r = rdd.filter.filter(lambda line: (line.split(';')[8]=="1467"))
#    #r.cache()
#    #r.count() 
# 	#out = r.reduceByKey(add).collect()

# if __name__ == "__main__":
#    # Configure Spark
#    conf = SparkConf().setAppName(APP_NAME)
#    #conf = conf.setMaster("local[*]")
#    sc   = SparkContext(conf=conf)
#    #filename = sys.argv[1]
#    # Execute Main functionality
#    # main(sc, filename)
#    main(sc)