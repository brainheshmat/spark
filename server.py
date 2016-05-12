import time, sys, cherrypy, os
from paste.translogger import TransLogger
from app import create_app
from pyspark import SparkContext, SparkConf

def init_spark_context():
    # load spark context
    conf = SparkConf().setAppName("ctr-server")
    conf.set('spark.kryoserializer.buffer', '512mb')
    conf.set('spark.kryoserializer.buffer.max', '512')
    # IMPORTANT: pass aditional Python modules to each worker
    sc = SparkContext(conf=conf, pyFiles=['/home/ec2-user/engine.py', '/home/ec2-user/app.py'])
 
    return sc
 
 
def run_server(app):
 
    # Enable WSGI access logging via Paste
    app_logged = TransLogger(app)

    # Mount the WSGI callable object (app) on the root directory
    cherrypy.tree.graft(app_logged, '/')

    # Set the configuration of the web server
    cherrypy.config.update({
        'engine.autoreload.on': True,
        'log.screen': True,
        'server.socket_port': 5432,
        'server.socket_host': '0.0.0.0'
    })

    # Start the CherryPy WSGI web server
    cherrypy.engine.start()
    cherrypy.engine.block()
 
 
if __name__ == "__main__":
    # Init spark context and load libraries
    sc = init_spark_context()
    hdfs_dataset_path = "hdfs://ip-172-31-41-216.ec2.internal:9000/data/output1.file"#".2015-07-07.14*.-0800.MMEpcmd.geo"#"file:///tmp/mme1/2015-07-07.14.*.-0800.MMEpcmd.geo"#os.path.join('datasets', 'ml-latest')
    local_dataset_path = "file:///data/output1.file"#"file://tmp/mme1/2015-07-07.14.59.-0800.MMEpcmd.geo"
    app = create_app(sc, hdfs_dataset_path)

    # start web server
    run_server(app)
