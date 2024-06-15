import os
import logging
from Utils.FileOperations import FileOperation
from Conf.Config import Config

class ETL:
    def eTL(self,spark):
        
        """
        Read the file from the input source and store the file in hdfs storage in csv,parquest and orc format.
        """
        try:
            logging.info("ETL process started.")
            # Directory containing Excel files
            
            directory = Config["INPUT_FILE_DIRECTORY"]
            logging.debug("Data directory: %s", directory)
            # Get a list of all files in the directory
            files = os.listdir(directory)
            for file_name in files:
                path = f"{directory}/{file_name}"

                pandas_df = FileOperation().readExcelAsPandas(path)

                # Log the file being processed
                logging.info("Processing file: %s", path)

                # Convert Pandas DataFrame to Spark DataFrame
                spark_df = spark.createDataFrame(pandas_df)

                #create the file name for different format
                csv_name = file_name.replace(".xlsx",".csv")
                parquet_name = file_name.replace(".xlsx",".parquet")
                orc_name = file_name.replace(".xlsx",".orc")

                # Write Spark DataFrame to HDFS in CSV format
                hdfs_csv_file = f"{Config['HDFS_ETL_DIRECTORY']}/{csv_name}"
                logging.debug("Saving ETL csv file at: %s",hdfs_csv_file)
                spark_df.write.csv(hdfs_csv_file)

                # Write Spark DataFrame to HDFS in Parquet format
                hdfs_parquet_file = f"{Config['HDFS_ETL_DIRECTORY']}/{parquet_name}"
                logging.debug("Saving ETL csv file at: %s",hdfs_parquet_file)
                spark_df.write.parquet(hdfs_parquet_file)
                
                # Write Spark DataFrame to HDFS in ORC format
                hdfs_orc_file = f"{Config['HDFS_ETL_DIRECTORY']}/{orc_name}"
                logging.debug("Saving ETL csv file at: %s",hdfs_orc_file)
                spark_df.write.orc(hdfs_orc_file)

            # Log completion of ETL process with time
            logging.info("ETL process completed.")
        except Exception as e:
            # Log error with time
            logging.error("An error occurred in ETL process: %s", str(e))
            raise

