import os
import glob
from pyspark import SparkConf, SparkContext

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataRDD:

	def get_data(self, col,id):
		# rdd_array = []
		# for fn in glob.glob(file_path):
		# 	if os.path.isfile(fn):
		# 		print "file://"+fn
		# 		rdd_array.append(self.sc.textFile("file://"+fn))
		# 		#self.data =  self.sc.union(self.sc.textFile("file://"+fn))
		# #file_path = "/data/*.geo"
		# #self.data = self.sc.textFile("file://"+file_path)# self.sc.union([self.sc.textFile("file://"+f) for f in file_path])
		# self.data =  self.sc.union(rdd_array);

		result = self.data.filter(lambda line: (line.split(';')[col]==str(id)))
		#print result.collect()
		#result.cache()
		return result

	def __init__(self, sc, dataset_path):
		#Init the recommendation engine given a Spark context and a dataset path

		logger.info("Starting up the data engine: "+dataset_path)
		file_path = dataset_path #os.path.join(dataset_path)
		self.sc = sc
		raw_RDD = sc.textFile(file_path).repartition(33);
		# for fn in os.listdir(file_path):
		# 	if os.path.isfile(fn):
		#         #print (fn)
		# 		self.data = self.sc.textFile(','.join("file:///"+fn))#sc.textFile(file_path)
				
		self.data = raw_RDD