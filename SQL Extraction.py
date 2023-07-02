# Databricks notebook source
# import the pandas library to our code, refer to it as pd. what will we use pandas for: data manipulation and analysis
import pandas as pd 
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SQL to DF").getOrCreate();

# COMMAND ----------

jdbcHostname =  spark.conf.get("spark.SQLInst")
jdbcDatabase = spark.conf.get("spark.SQLDB")
jdbcPort = 1433
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
connectionProperties = {
"user" : spark.conf.get("spark.SQLUser"),
"password" : spark.conf.get("spark.SQLPWD"),
"driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

Spdf = spark.read.jdbc(url=jdbcUrl, table="stagingmapping", properties=connectionProperties)
display(Spdf)

# COMMAND ----------


