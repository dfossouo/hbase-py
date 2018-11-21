# hbase-py

## Prerequisites 
# Scala 2.11
# Sbt 1.2.4 
# Java 1.8
# Spark 2.3.0 - from HDP 
# Maven 3.5.4

# PART I - Static Schema 

This code does the following:
  1) Read the same HBase Table from two distinct or identical clusters (the schema of tables is required)
  2) Extract specific time Range
  3) Create two dataframes and compare them to give back lines where the tables are differents (only the columns where we have differences)

Usage:
SPARK_MAJOR_VERSION=2 spark-submit --class com.github.dfossouo.SparkHBase.SparkReadHBaseTable --master yarn --deploy-mode client --driver-memory 2g --executor-memory 3g --executor-cores 2 --num-executors 2 --jars ./target/HBaseCRC-0.0.2-SNAPSHOT.jar /tmp/phoenix-client.jar ./props


# PART II - Dynamic Schema 

In the second part we can discover the schema of the table

This code does the following:
  1) Read the same HBase Table from two distinct or identical clusters (the schema of tables is not required)
  2) Extract specific time Range
  3) Create two dataframes and compare them to give back lines where the tables are differents (only the columns where we have differences)
  
Usage: 
SPARK_MAJOR_VERSION=2 spark-submit --class com.github.dfossouo.SparkHBase.SparkReadHBaseTable_DiscoverSchema --master yarn --deploy-mode client --driver-memory 2g --executor-memory 3g --executor-cores 2 --num-executors 2 --jars ./target/HBaseCRC-0.0.2-SNAPSHOT.jar /tmp/phoenix-client.jar ./props


NB: props is the property file where the cluster properties are defined