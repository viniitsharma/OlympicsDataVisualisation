
from pyspark.sql import SparkSession # type: ignore
import logging
from Conf.Config import Config
from Utils.CONSTANTS import CONST

class Connect:
    def connectSpark(self):

        # Log Spark session creation start with time
        logging.info("Initalisation spark session")

        # Create SparkSession
        spark = SparkSession.builder \
            .appName(Config["APPNAME"]) \
            .config(Config["CONFIG_HIVEWRH"], Config["CONFIG_HIVEDIR"]) \
            .enableHiveSupport() \
            .getOrCreate()

        # Log Spark session creation completion with time
        logging.info("Initalisation spark session completed")

        return spark
