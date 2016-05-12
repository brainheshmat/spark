
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
def getData(col,id):
    logger.debug("filtering")
    res = data.filter(lambda line: (line.split(';')[col]==str(id)))
    #print res
    return json.dumps(res.collect())

def getDataPCMD():
    logger.debug("Getting data")
    filepath = "/data/*.geo"
    result = data_engine.get_data()#(col,id)
    result.cache()
    #data = result.collect()
    #print res
    return result #json.dumps(res)

def create_app(spark_context, dataset_path):
    global data_engine
    global data
    data_engine = DataRDD(spark_context, dataset_path)    
    data = getDataPCMD()

    app = Flask(__name__)
    app.register_blueprint(main)
    return app 