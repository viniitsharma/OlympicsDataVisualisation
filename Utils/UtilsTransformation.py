import logging
from pyspark.sql.functions import when # type: ignore
from Conf.Config import Config

class UtilsTransformation:
    def athletesTransformation(self, spark, spark_country_encoding_df, spark_discipline_encoding_df):
        """
        Tranform the athelete dataframe
        input : spark session, spark_country_encoding_df having country encoded values, spark_discipline_encoding_df having discipline encoded values
        operations : rename a value, join with country_encoding_df, join with discipline_encoding_df, rename the column
        """
        
        logging.info("Athlete transformation started.")

        # Read athlete data from ORC file
        athletes = spark.read.format("orc").option("header", "true").load(f"{Config['HDFS_ETL_DIRECTORY']}/{Config['ATHLETE_HDFS_FILE']}")
        logging.debug(f"Athlete dataframe raw : {athletes.show(5)}")
        # Replace "Côte d'Ivoire" with "Ivory Coast" in the NOC column
        athletes = athletes.withColumn(
            "NOC",
            when(athletes["NOC"] == "Côte d'Ivoire", "Ivory Coast").otherwise(athletes["noc"])
        )

        # Join athlete data with country encoding data based on NOC
        athletes_encoded = athletes.join(
            spark_country_encoding_df,
            athletes["NOC"] == spark_country_encoding_df["noc"],
            "left"
        )
        
        # Join athlete data with discipline encoding data based on Discipline
        athletes_encoded = athletes_encoded.join(
            spark_discipline_encoding_df,
            athletes_encoded["Discipline"] == spark_discipline_encoding_df["discipline"],
            "left"
        )
        
        # Rename the "Name" column to "name"
        athletes_encoded = athletes_encoded.withColumnRenamed("Name", "name")
        
        # Select relevant columns from the joined data
        athletes_df = athletes_encoded.select("name", "noc_encoded", "discipline_encoding")
        logging.debug(f"Athlete dataframe transformed : {athletes_df.show(5)}")
        
        # Log completion of athlete transformation with time
        logging.info("Athlete transformation completed.")
        
        # Return the transformed athlete dataframe
        return athletes_df


    def coachTransformation(self, spark, spark_country_encoding_df, spark_discipline_encoding_df, spark_event_encoding_df):


        logging.info("Coach transformation started.")

        # Read coach data from ORC file
        coach_raw = spark.read.format("orc").option("header", "true").load(f"{Config['HDFS_ETL_DIRECTORY']}/{Config['COACH_HDFS_FILE']}")
        
        # Replace "Côte d'Ivoire" with "Ivory Coast" in the NOC column
        coach_raw = coach_raw.withColumn("NOC", when(coach_raw["NOC"] == "Côte d'Ivoire", "Ivory Coast").otherwise(coach_raw["NOC"]))
        
        # Replace "NaN" with "NA" in the Event column
        coach = coach_raw.na.replace(["NaN"], ["NA"], subset=["Event"])
        
        # Join coach data with country encoding data based on NOC
        coach_encoded = coach.join(spark_country_encoding_df, coach["NOC"] == spark_country_encoding_df["noc"], "left")
        
        # Join coach data with discipline encoding data based on Discipline
        coach_encoded = coach_encoded.join(spark_discipline_encoding_df, coach_encoded["Discipline"] == spark_discipline_encoding_df["discipline"], "left")
        
        # Join coach data with event encoding data based on Event
        coach_encoded = coach_encoded.join(spark_event_encoding_df, coach_encoded["Event"] == spark_event_encoding_df["event"], "left")
        
        # Rename the "Name" column to "name"
        coach_encoded = coach_encoded.withColumnRenamed("Name", "name")
        
        # Select relevant columns from the joined data
        coach_df = coach_encoded.select("name", "noc_encoded", "event_encoding", "discipline_encoding")

        # Log completion of coach transformation with time
        logging.info("Coach transformation completed.")

        # Return the transformed coach dataframe
        return coach_df


    def EntriesGenderTransformation(self, spark, spark_discipline_encoding_df):

        logging.info("Entries gender transformation started.")

        # Read entries gender data from ORC file
        entriesgender_raw = spark.read.format("orc").option("header", "true").load(f"{Config['HDFS_ETL_DIRECTORY']}/{Config['ENTRIES_GENDER_HDFS_FILE']}")
        
        # Replace "NaN" with "NA" in the Discipline column
        entriesgender = entriesgender_raw.na.replace(["NaN"], ["NA"], subset=["Discipline"])
        
        # Join entries gender data with discipline encoding data based on Discipline
        entriesgender_encoded = entriesgender.join(spark_discipline_encoding_df, entriesgender["Discipline"] == spark_discipline_encoding_df["discipline"], "left")
        
        # Rename columns for clarity
        entriesgender_encoded = entriesgender_encoded.withColumnRenamed("Female", "female_count")
        entriesgender_encoded = entriesgender_encoded.withColumnRenamed("Male", "male_count")
        entriesgender_encoded = entriesgender_encoded.withColumnRenamed("Total", "total_count")
        
        # Select relevant columns from the joined data
        entriesgender_df = entriesgender_encoded.select("discipline_encoding", "female_count", "male_count", "total_count")

        # Log completion of entries gender transformation with time
        logging.info("Entries gender transformation completed.")

        # Return the transformed entries gender dataframe
        return entriesgender_df


    def MedalsTransformation(self, spark, spark_country_encoding_df):

        logging.info("Medal transformation started.")

        # Read medals data from ORC file
        medal_raw = spark.read.format("orc").option("header", "true").load(f"{Config['HDFS_ETL_DIRECTORY']}/{Config['MEDALS_HDFS_FILE']}")
        
        # Replace "Côte d'Ivoire" with "Ivory Coast" in the Team/NOC column
        medal_raw = medal_raw.withColumn("Team/NOC", when(medal_raw["Team/NOC"] == "Côte d'Ivoire", "Ivory Coast").otherwise(medal_raw["Team/NOC"]))
        
        # Replace "NaN" with "NA" in the Team/NOC column
        medal = medal_raw.na.replace(["NaN"], ["NA"], subset=["Team/NOC"])
        
        # Join medals data with country encoding data based on Team/NOC
        medal_encoded = medal.join(spark_country_encoding_df, medal["Team/NOC"] == spark_country_encoding_df["noc"], "left")
        
        # Rename columns for clarity
        medal_encoded = medal_encoded.withColumnRenamed("Gold", "gold_count")
        medal_encoded = medal_encoded.withColumnRenamed("Silver", "silver_count")
        medal_encoded = medal_encoded.withColumnRenamed("Bronze", "bronze_count")
        medal_encoded = medal_encoded.withColumnRenamed("Total", "total_count")
        medal_encoded = medal_encoded.withColumnRenamed("Rank by Total", "rank_by_total")
        
        # Select relevant columns from the joined data
        medal_df = medal_encoded.select("noc_encoded", "gold_count", "silver_count", "bronze_count", "total_count", "rank_by_total")

        # Log completion of medals transformation with time
        logging.info("Medal transformation completed.")

        # Return the transformed medals dataframe
        return medal_df


    def TeamTransformation(self, spark, spark_country_encoding_df, spark_discipline_encoding_df, spark_event_encoding_df):

        logging.info("Team transformation started.")

        # Read team data from ORC file
        team_raw = spark.read.format("orc").option("header", "true").load(f"{Config['HDFS_ETL_DIRECTORY']}/{Config['TEAM_HDFS_FILE']}")
        
        # Replace "Côte d'Ivoire" with "Ivory Coast" in the NOC column
        team_raw = team_raw.withColumn("NOC", when(team_raw["NOC"] == "Côte d'Ivoire", "Ivory Coast").otherwise(team_raw["NOC"]))
        
        # Replace "Men's Épée Team" with "Men's Epee Team" in the Event column
        team_raw = team_raw.withColumn("Event", when(team_raw["Event"] == "Men's Épée Team", "Men's Epee Team").otherwise(team_raw["Event"]))
        
        # Replace "Women's Épée Team" with "Women's Epee Team" in the Event column
        team_raw = team_raw.withColumn("Event", when(team_raw["Event"] == "Women's Épée Team", "Women's Epee Team").otherwise(team_raw["Event"]))
        
        # Replace "NaN" with "NA" in the Event column
        team = team_raw.na.replace(["NaN"], ["NA"], subset=["Event"])
        
        # Join team data with event encoding data based on Event
        team_encoded = team.join(spark_event_encoding_df, team["Event"] == spark_event_encoding_df["event"], "left")
        
        # Join team data with country encoding data based on NOC
        team_encoded = team_encoded.join(spark_country_encoding_df, team_encoded["NOC"] == spark_country_encoding_df["noc"], "left")
        
        # Join team data with discipline encoding data based on Discipline
        team_encoded = team_encoded.join(spark_discipline_encoding_df, team_encoded["Discipline"] == spark_discipline_encoding_df["discipline"], "left")
        
        # Select relevant columns from the joined data
        team_df = team_encoded.select("noc_encoded", "event_encoding", "discipline_encoding")

        # Log completion of team transformation with time
        logging.info("Team transformation completed.")

        # Return the transformed team dataframe
        return team_df
