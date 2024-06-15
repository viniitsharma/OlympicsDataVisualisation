import logging
import logging.config
import os
from Conf.Config import Config
from Utils.Connect.ConnectSpark import Connect


    
def main(mode):
    # Providing path of python to pyspark
    os.environ['PYSPARK_PYTHON'] = Config["PYTHONPATH"]

    # Log environment setup completion with time
    logging.info("Environment setup completed.")

    # Establish Spark session
    spark = Connect().connectSpark()
    logging.info("Spark session established.")
    
