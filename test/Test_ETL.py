import pytest # type: ignore
from pyspark.sql import SparkSession # type: ignore
from Utils.Connect.ConnectSpark import Connect
from Conf.Config import Config


def test_connect_spark_real():

    # Call connectSpark method to get a real Spark session
    spark = Connect().connectSpark()

    # Verify that a SparkSession is created
    assert isinstance(spark, SparkSession)

    # Verify the application name and config
    assert spark.sparkContext.appName == Config["APPNAME"]
    # Here, we assume that the warehouse directory may be prefixed with 'hdfs://localhost:9820'
    expected_hive_dir = 'hdfs://localhost:9820' + Config["CONFIG_HIVEDIR"]
    assert spark.conf.get(Config["CONFIG_HIVEWRH"]) == expected_hive_dir

