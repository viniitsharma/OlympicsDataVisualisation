import json
import logging
import pandas as pd # type: ignore

class FileOperation:
    def readJson(self, file_Path):
        logging.info("Reading JSON file")
        # Log start of JSON file reading with time
        logging.debug("Reading JSON file started: %s", file_Path)

        # Read JSON file
        with open(file_Path, 'r') as f:
            data = json.load(f)

        # Log completion of JSON file reading with time
        logging.debug("Reading JSON file completed: %s", file_Path)

        return data
    
    def readExcelAsPandas(self, file_Path):
        # Read Excel file using Pandas
        pandas_df = pd.read_excel(file_Path)
        logging.debug("data: %s", pandas_df.head(2))
        return pandas_df
