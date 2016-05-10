
from flask import Blueprint
main = Blueprint('main', __name__)
 
import json
from engine import DataRDD
import pyspark
 
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
 
from flask import Flask, request

@main.route("/getData/<int:col>/<int:id>", methods=["GET"])
def getDataPCMD(col,id):
    logger.debug("Getting data")
    result = data_engine.get_data(col,id)
    result.persist() #.cache()
    res = result.collect()
    #print res
    return json.dumps(res)

def create_app(spark_context, dataset_path):
	global data_engine 

	data_engine = DataRDD(spark_context, dataset_path)    

	app = Flask(__name__)
	app.register_blueprint(main)
	return app 