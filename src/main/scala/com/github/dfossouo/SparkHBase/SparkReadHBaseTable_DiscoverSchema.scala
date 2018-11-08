/*******************************************************************************************************
This code does the following:
  1) Read the same HBase Table from two distinct or identical clusters (without knowing the schema of tables)
  2) Extract specific time Range
  3) Create two dataframes and compare them to give back lines where the tables are differents (only the columns where we have differences)

Usage:
SPARK_MAJOR_VERSION=2 spark-submit --class com.github.dfossouo.SparkHBase.SparkReadHBaseTable_DiscoverSchema --master yarn --deploy-mode client --driver-memory 1g --executor-memory 4g --executor-cores 1 --jars ./target/HBaseCRC-0.0.2-SNAPSHOT.jar /usr/hdp/current/phoenix-client/phoenix-client.jar /tmp/props2
NB: props2 est un fichier qui contient les variables d'entrées du projet
  ********************************************************************************************************/

package com.github.dfossouo.SparkHBase;

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.KeyValue.Type
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableSnapshotInputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
import scala.io.Source.fromFile

import org.apache.spark.sql.{SQLContext, _}
import org.apache.spark.sql.execution.datasources.hbase._
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.sql.functions._


object SparkReadHBaseTable_DiscoverSchema {

  case class hVar(rowkey: Int, colFamily: String, colQualifier: String, colDatetime: Long, colDatetimeStr: String, colType: String, colValue: String)

  def main(args: Array[String]) {

    // Create difference between dataframe function
    import org.apache.spark.sql.DataFrame

    def diff(rowkey: String,key: String,value: String, df1: DataFrame, df2: DataFrame): DataFrame = {
      val fields = df1.columns
      val diffColumnName = "Diff"

      df1.join(df2, (df1(key) === df2(key))&&(df1(rowkey) === df2(rowkey))&&(df1(value) =!= df2(value)), "inner")

    }

    // Get Start time

    val start_time = Calendar.getInstance()
    println("[ *** ] Start Time: " + start_time.getTime().toString)

    // Init properties
    val props = getProps(args(0))

    // Get Start time table Scan
    val tbscan = 1540365233000L
    val start_time_tblscan = tbscan.toString()

    val table = props.get("hbase.table.name").get
    val tablex = props.get("hbase.table.name_x").get

    // Create Spark Application
    val sparkConf = new SparkConf().setAppName("SparkReadHBaseTable")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    println("[ *** ] Creating HBase Configuration cluster 1")
    val hConf = HBaseConfiguration.create()
    hConf.setInt("timeout", 120000)
    hConf.set("hbase.rootdir", "/tmp")
    hConf.set("zookeeper.znode.parent", "/hbase-unsecure")
    hConf.set("hbase.zookeeper.quorum", "hdpcluster-15377-master-0.field.hortonworks.com:2181")

    // Create Connection
    val connection: Connection = ConnectionFactory.createConnection(hConf)

    print("connection created")

    // test scala

    print("[ ****** ] define schema table emp ")

    def customerinfocatalog= s"""{
        "table":{"namespace":"default", "name":"$table"},
        "rowkey":"key",
        "columns":{
        "rowkey":{"cf":"rowkey", "col":"key", "type":"string"},
        "data":{"cf":"demographics", "col":"", "type":"map<string, string>"}
        }
        }""".stripMargin

    print("[ ****** ] define schema table customer_info ")

    def customerinfodebugcatalog= s"""{
        "table":{"namespace":"default", "name":"$tablex"},
        "rowkey":"key",
        "columns":{
        "rowkey":{"cf":"rowkey", "col":"key", "type":"string"},
        "data":{"cf":"demographics", "col":"", "type":"map<string, string>"}
        }
        }""".stripMargin

    print("[ ****** ] Create DataFrame table emp ")

    def withCatalogInfo(customerinfocatalog: String): DataFrame = {
      sqlContext
        .read
        .options(Map(HBaseTableCatalog.tableCatalog->customerinfocatalog,HBaseRelation.RESTRICTIVE -> "HBaseRelation.Restrictive.none",HBaseRelation.MIN_STAMP -> "0", HBaseRelation.MAX_STAMP -> (start_time_tblscan + 100).toString))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .load()
    }

    print("[ ****** ] Create DataFrame table customer_info ")

    def withCatalogInfoDebug(customerinfodebugcatalog: String): DataFrame = {
      sqlContext
        .read
        .options(Map(HBaseTableCatalog.tableCatalog->customerinfodebugcatalog,HBaseRelation.RESTRICTIVE -> "HBaseRelation.Restrictive.none",HBaseRelation.MIN_STAMP -> "0", HBaseRelation.MAX_STAMP -> (start_time_tblscan + 100).toString))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .load()
    }

    print("[ ****** ] declare DataFrame for table customer_info ")

    val df = withCatalogInfo(customerinfocatalog)

    print("Here are the columns " + df.columns.foreach(println))

    df.columns.map(f => print(f))

    print("[ ****** ] declare DataFrame for table customer_info_debug ")

    val df_debug = withCatalogInfoDebug(customerinfodebugcatalog)

    print("[ ****** ] here is the dataframe contain: ")

    df.show(10,false)
    df_debug.show(10,false)

    print("[ *** ] Selective Differences")

    val columns = df.columns

    print("[ *** ] Selective Differences - step1")

    df.select($"rowkey", size($"data").as("count")).show(false)
    df.select($"rowkey", explode($"data")).show(false)
    df_debug.select($"rowkey", explode($"data")).show(false)

    val df1 = df.select($"rowkey", explode($"data"))
    val df2 = df_debug.select($"rowkey", explode($"data"))

    diff("rowkey","key","value",df1,df2).show(false)


    // selectiveDifferences.toString

    print("[ *** ] Selective Differences only diff columns")

    print("[ ************** ] Here is the number of different rows " + diff("rowkey","key","value",df1,df2).count())

    println("[ *** ] Ending HBase Configuration cluster 1")

    connection.close()

  }

  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan);
    Base64.encodeBytes(proto.toByteArray());
  }

  def getArrayProp(props: => HashMap[String, String], prop: => String): Array[String] = {
    return props.getOrElse(prop, "").split(",").filter(x => !x.equals(""))
  }

  def getProps(file: => String): HashMap[String, String] = {
    var props = new HashMap[String, String]
    val lines = fromFile(file).getLines
    lines.foreach(x => if (x contains "=") props.put(x.split("=")(0), if (x.split("=").size > 1) x.split("=")(1) else null))
    props
  }


}
//ZEND
