import logging
from Utils.FileOperations import FileOperation
import pandas as pd # type: ignore
from pyspark.sql.types import ShortType # type: ignore
from Conf.Config import Config

class Encoding:
    def readEncoding(self, spark):
        # Log start of encoding data read process with time
        logging.info("Reading encoding data started.")

        # Read encoding data from JSON files
        data = FileOperation().readJson(Config["ENCODING_FILE_PATH"])

        # Access dictionaries by their names
        country_encoding = data['CountryEncodings']
        discipline_encoding = data['DisciplineEncoding']
        event_encoding = data['EventEncoding']

        # Convert country encoding dictionary to DataFrame and then to Spark DataFrame
        country_encoding_df = pd.DataFrame(country_encoding)
        spark_country_encoding_df = spark.createDataFrame(country_encoding_df)
        spark_country_encoding_df = spark_country_encoding_df.withColumn("noc_encoded", spark_country_encoding_df["noc_encoded"].cast(ShortType()))
        logging.debug(f"spark_country_encoded{spark_country_encoding_df.show(5)}")

        # Convert discipline encoding dictionary to DataFrame and then to Spark DataFrame
        discipline_encoding_df = pd.DataFrame(discipline_encoding)
        spark_discipline_encoding_df = spark.createDataFrame(discipline_encoding_df)
        spark_discipline_encoding_df = spark_discipline_encoding_df.withColumn("discipline_encoding", spark_discipline_encoding_df["discipline_encoding"].cast(ShortType()))
        logging.debug(f"spark_discipline_encoded{spark_discipline_encoding_df.show(5)}")

        # Convert event encoding dictionary to DataFrame and then to Spark DataFrame
        event_encoding_df = pd.DataFrame(event_encoding)
        spark_event_encoding_df = spark.createDataFrame(event_encoding_df)
        spark_event_encoding_df = spark_event_encoding_df.withColumn("event_encoding", spark_event_encoding_df["event_encoding"].cast(ShortType()))
        logging.debug(f"spark_event_encoded{spark_event_encoding_df.show(5)}")

        # Log end of encoding data read process with time
        logging.info("Reading encoding data completed.")

        # Return Spark DataFrames for country, discipline, and event encodings
        return spark_country_encoding_df, spark_discipline_encoding_df, spark_event_encoding_df
