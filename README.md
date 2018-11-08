# hbase-py

## Prerequisites 
# Scala 2.11
# Sbt 1.2.4 
# Java 1.8
# Spark 2.3.0 - from HDP 
# Maven 3.5.4

This code does the following:
  1) Read the same HBase Table from two distinct or identical clusters (the schema of tables is required)
  2) Extract specific time Range
  3) Create two dataframes and compare them to give back lines where the tables are differents (only the columns where we have differences)

Usage:
SPARK_MAJOR_VERSION=2 spark-submit --class com.github.dfossouo.SparkHBase.SparkReadHBaseTable --master yarn --deploy-mode client --driver-memory 1g --executor-memory 4g --executor-cores 1 --jars ./target/HBaseCRC-0.0.2-SNAPSHOT.jar /usr/hdp/current/phoenix-client/phoenix-client.jar /tmp/props2
NB: props2 est un fichier qui contient les variables d'entrées du projet

