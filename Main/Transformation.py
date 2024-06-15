import logging
from Utils.Encoding import Encoding
from Utils.UtilsTransformation import UtilsTransformation

class Transformation:
    def transformation(self, spark):
        """
        Transform and store the data to hive data warehouse
        """
        # Set the default database to 'tokyo_olympics'
        spark.sql("use tokyo_olympics")

        logging.info("Transformation process started.")

        # Insert encoding tables into Hive tables
        spark_country_encoding_df, spark_discipline_encoding_df, spark_event_encoding_df = Encoding().readEncoding(spark)
        spark_country_encoding_df.write.mode("append").format("hive").saveAsTable("country")
        spark_discipline_encoding_df.write.mode("append").format("hive").saveAsTable("discipline")
        spark_event_encoding_df.write.mode("append").format("hive").saveAsTable("event")

        # Perform athlete transformation
        logging.info("Athlete transformation started.")
        athletes_df = UtilsTransformation().athletesTransformation(spark, spark_country_encoding_df, spark_discipline_encoding_df)
        athletes_df.write.mode("append").format("hive").saveAsTable("athlete")
        logging.info("Athlete transformation completed.")

        # Perform coach transformation
        logging.info("Coach transformation started.")
        coach_df = UtilsTransformation().coachTransformation(spark, spark_country_encoding_df, spark_discipline_encoding_df, spark_event_encoding_df)
        coach_df.write.mode("append").format("hive").saveAsTable("coach")
        logging.info("Coach transformation completed.")

        # Perform entries gender transformation
        logging.info("Entries gender transformation started.")
        entriesgender_df = UtilsTransformation().EntriesGenderTransformation(spark, spark_discipline_encoding_df)
        entriesgender_df.write.mode("append").format("hive").saveAsTable("entries_gender")
        logging.info("Entries gender transformation completed.")

        # Perform medal transformation
        logging.info("Medal transformation started.")
        medal_df = UtilsTransformation().MedalsTransformation(spark, spark_country_encoding_df)
        medal_df.write.mode("append").format("hive").saveAsTable("medal")
        logging.info("Medal transformation completed.")

        # Perform team transformation
        logging.info("Team transformation started.")
        team_df = UtilsTransformation().TeamTransformation(spark, spark_country_encoding_df, spark_discipline_encoding_df, spark_event_encoding_df)
        team_df.write.mode("append").format("hive").saveAsTable("team")
        logging.info("Team transformation completed.")

        # Log end of transformation process with time
        logging.info("Transformation process completed.")
