import logging
import logging.config
import os
from Conf.Config import Config
from Utils.CONSTANTS import CONST
from Utils.Connect.ConnectSpark import Connect
from Utils.DataOperation import DataOperation
from Utils.ETL import ETL
from Utils.Connect.ConnectSpark import Connect



def main(mode):
    # Providing path of python to pyspark
    os.environ['PYSPARK_PYTHON'] = Config["PYTHONPATH"]


    #Cleaning Cache
    DataOperation().pycacheCleanup()

    # Log environment setup completion with time
    logging.debug("Environment setup completed.")

    # Establish Spark session
    spark = Connect().connectSpark()
    logging.debug("Spark session established.")
    

    spark.sql(CONST["USE_DB"])

    if mode == "CleanUp":
        """
        Truncate all the data from the hive tables
        """
        hive_table_list = ["athlete","coach","country","discipline","entries_gender","event","medal","team"]
        spark.sql("select count(*) from athlete").show()
        DataOperation().truncateHive(spark,hive_table_list)
        spark.sql("select * from athlete").show()


    if mode == "ETL" or mode == "New":
        """
        only execute if the mode is ETL or New
        """
        logging.info("Executing ETL process")
        ETL().eTL(spark)

    # Log environment setup completion with time
    logging.info("Environment setup completed.")

    # Establish Spark session
    spark = Connect().connectSpark()
    logging.info("Spark session established.")

    
