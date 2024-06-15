from Utils.CONSTANTS import CONST
import logging
from Conf.Config import Config
from Utils.FileOperations import FileOperation

class Dashboard:
    def dashboard(self,spark):
        spark.sql("use tokyo_olympics")
        most_gold_data = spark.sql(CONST["MOST_GOLD"])
        most_silver_data = spark.sql(CONST["MOST_SILVER"])
        most_bronze_data = spark.sql(CONST["MOST_BRONZE"])
        most_total_medal_data = spark.sql(CONST["MOST_TOTAL_MEDAL"])
        bar_chart_data = spark.sql(CONST["BAR_CHART_DATA"])
        map_data = spark.sql(CONST["MAP_DATA"])

        # Log data fetch completion with time
        logging.info("Data fetched from HIVE.")

        # Convert to Pandas DataFrame
        map_data_pandas = map_data.toPandas()
        bar_chart_data_pandas = bar_chart_data.toPandas()
        most_gold_data_pandas = most_gold_data.toPandas()
        most_silver_data_pandas = most_silver_data.toPandas()
        most_bronze_data_pandas = most_bronze_data.toPandas()
        most_total_medal_data_pandas = most_total_medal_data.toPandas()

        # Read encoding data for the dashboard
        data = FileOperation().readJson(Config["ENCODING_PATH"])
        plotly_map = data['CountryPlotly']
        return map_data_pandas,bar_chart_data_pandas,most_gold_data_pandas,most_silver_data_pandas,most_bronze_data_pandas,most_total_medal_data_pandas,plotly_map