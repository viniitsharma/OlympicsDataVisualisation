import os
import shutil
import logging

class DataOperation:
    def findNA(self, df, spark):
        # Log start of NA detection process with time
        logging.info("NA detection process started.")
        logging("NA detection process started.")

        union_df = None
        for col in df.columns:
            # Filter rows where the column is null
            na_df = df.filter(df[col].isNull())
            
            # Show NA values in the column
            logging.debug("Count of NA for column '%s': %d", col, na_df.count())
            logging.debug("NA values in column '%s':", col)
            if na_df.count() > 0:
                na_df.show()  # Log NA values

                # Union dataframes with NA values
                if union_df is None:
                    union_df = na_df
                else:
                    union_df = union_df.union(na_df)
        
        # Log final dataframe with NA values
        logging.debug("Final dataframe with NA values:")
        union_df.show()

        # Log completion of NA detection process with time
        logging.info("NA detection process completed.")

    def pycacheCleanup(self):
        """
        cleanup the cache files from previous runs
        """

        # Log start of __pycache__ cleanup process with time
        logging.info("__pycache__ cleanup process started.")

        directory = os.getcwd()

        # Iterate over all directories and files within the specified directory
        for root, dirs, files in os.walk(directory):
            for d in dirs:
                if d == '__pycache__':
                    # Remove the __pycache__ directory
                    shutil.rmtree(os.path.join(root, d))

        # Log completion of __pycache__ cleanup process with time
        logging.info("__pycache__ cleanup process completed.")

    
    def truncateHive(self,spark,table_list):
        for table_name in table_list:
            spark.sql(f"TRUNCATE TABLE {table_name}")
        return
