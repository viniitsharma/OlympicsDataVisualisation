import logging
import logging.config
import os
from Conf.Config import Config
from Utils.CONSTANTS import CONST
from Utils.Connect.ConnectSpark import Connect
from Utils.DataOperation import DataOperation
from Utils.ETL import ETL
from Main.Transformation import Transformation
from Main.Dashboard import Dashboard
from Utils.UtilsDashboard import UtilsDashboard



    
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



    # Execute based on mode
    elif mode == "Transformation" or mode == "New":
        """
        only execute if the mode is Transformation or New
        """
        Transformation().transformation(spark)
        logging.info("ETL process completed.")

    elif mode == "Dashboard":
        """
        only execute if the mode is Dashboard
        """
        logging.info("Starting Dashboard mode...")
        
        # Fetch the data required for the dashboard from HIVE
        try:

            map_data_pandas,bar_chart_data_pandas,most_gold_data_pandas,most_silver_data_pandas,most_bronze_data_pandas,most_total_medal_data_pandas,plotly_map = Dashboard().dashboard(spark)

            logging.info("Data converted to Pandas DataFrames.")

            dashboard = UtilsDashboard()
            
            # Set input data for the dashboard
            dashboard.set_input_data(
                map_data_pandas,
                bar_chart_data_pandas,
                most_gold_data_pandas,
                most_silver_data_pandas,
                most_bronze_data_pandas,
                most_total_medal_data_pandas,
                plotly_map
            )


            logging.info("Input data set for the dashboard.")

            # Run the server
            dashboard.run_server()
            logging.info("Dashboard server started.")
        except Exception as e:

            logging.error("An error occurred while executing the Dashboard mode: %s", str(e))
            raise

if __name__ == "__main__":
    mode = "Dashboard"
    logging.basicConfig(
        filename=Config["LOGPATH"],
        level=getattr(logging, Config["LOGGINGLEVEL"]),
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logging.info("Mode selected: %s", mode)

    #Cleanup the log file
    with open(Config["LOGPATH"], 'w') as f:
        f.write('')
    main(mode)
