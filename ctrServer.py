from pyspark import SparkConf, SparkContext
#sc = SparkContext('local')

from flask import Flask, request
app = Flask(__name__)

#sc = SparkContext('local')
APP_NAME = " Querying PCMD files"
#filename = "hdfs://ec2-52-207-247-64.compute-1.amazonaws.com:9000/user/15061021.PCMD"
filename = "file:///tmp/mme1/2015-07-07.14.*.-0800.MMEpcmd.geo"
##OTHER FUNCTIONS/CLASSES
## Main functionality
conf = SparkConf().setAppName(APP_NAME)
#conf = conf.setMaster("local[*]")
sc   = SparkContext(conf=conf)

@app.route('/accessFunction', methods=['GET'])  #can set first param to '/'

def toyFunction():
	rdd = sc.textFile(filename) 
	r = rdd.filter(lambda line: (line.split(';')[8]=="1467"))
	r.cache()
	m = r.count()
 	#posted_data = sc.parallelize([request.get_data()])
	return m # str(posted_data.collect()[0])

if __name__ == '__main_':
    app.run(port=8080)    #note set to 8080!