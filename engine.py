import os
from pyspark import SparkConf, SparkContext

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataRDD:

	def get_data(self,col,id):
		result = self.data.filter(lambda line: (line.split(';')[col]==str(id)))
		#print result.collect()
		result.cache()
		return result

	def __init__(self, sc, dataset_path):
		#Init the recommendation engine given a Spark context and a dataset path

		logger.info("Starting up the data engine: ")
		file_path = dataset_path #os.path.join(dataset_path)
		self.sc = sc
		raw_RDD = self.sc.textFile(file_path)

		self.data = raw_RDD
