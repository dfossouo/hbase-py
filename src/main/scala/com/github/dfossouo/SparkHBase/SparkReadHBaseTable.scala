/*******************************************************************************************************
This code does the following:
  1) Read the same HBase Table from two distinct or identical clusters (the schema of tables is required)
  2) Extract specific time Range
  3) Create two dataframes and compare them to give back lines where the tables are differents (only the columns where we have differences)

Usage:
SPARK_MAJOR_VERSION=2 spark-submit --class com.github.dfossouo.SparkHBase.SparkReadHBaseTable --master yarn --deploy-mode client --driver-memory 1g --executor-memory 4g --executor-cores 1 --jars ./target/HBaseCRC-0.0.2-SNAPSHOT.jar /usr/hdp/current/phoenix-client/phoenix-client.jar /tmp/props2
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


object SparkReadHBaseTable {

  case class hVar(rowkey: Int, colFamily: String, colQualifier: String, colDatetime: Long, colDatetimeStr: String, colType: String, colValue: String)

  def main(args: Array[String]) {

    // Create difference between dataframe function

    def diff(key: String, df1: DataFrame, df2: DataFrame): DataFrame = {
      val fields = df1.columns
      val diffColumnName = "Diff"

      df1
        .join(df2, df1(key) === df2(key), "full_outer")
        .withColumn(
          diffColumnName,
          when(df1(key).isNull, "New row in DataFrame 2")
            .otherwise(
              when(df2(key).isNull, "New row in DataFrame 1")
                .otherwise(
                  concat_ws("",
                    fields.map(f => when(df1(f) !== df2(f), s"$f ").otherwise("")):_*
                  )
                )
            )
        )
        .filter(col(diffColumnName) !== "")
        .select(
          fields.map(f =>
            when(df1(key).isNotNull, df1(f)).otherwise(df2(f)).alias(f)
          ) :+ col(diffColumnName):_*
        )
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
    hConf.set("hbase.rootdir", props.getOrElse("hbase.rootdir", "/tmp"))
    hConf.set("zookeeper.znode.parent", props.getOrElse("zookeeper.znode.parent", "/hbase-unsecure"))
    hConf.set("hbase.zookeeper.quorum", props.getOrElse("hbase.zookeeper.quorum", "hdpcluster-15377-master-0.field.hortonworks.com:2181"))

    // Create Connection
    val connection: Connection = ConnectionFactory.createConnection(hConf)

    print("connection created")

    // test scala

    print("[ ****** ] define schema table emp ")

    def customerinfocatalog= s"""{
        "table":{"namespace":"default", "name":"$table"},
        "rowkey":"key",
        "columns":{
        "key":{"cf":"rowkey", "col":"key", "type":"string"},
        "custid":{"cf":"demographics", "col":"custid", "type":"string"},
        "gender":{"cf":"demographics", "col":"gender", "type":"string"},
        "age":{"cf":"demographics", "col":"age", "type":"string"},
        "level":{"cf":"demographics", "col":"level", "type":"string"}
        }
        }""".stripMargin

    print("[ ****** ] define schema table customer_info ")

    def customerinfodebugcatalog= s"""{
        "table":{"namespace":"default", "name":"$tablex"},
        "rowkey":"key",
        "columns":{
        "key":{"cf":"rowkey", "col":"key", "type":"string"},
        "custid":{"cf":"demographics", "col":"custid", "type":"string"},
        "gender":{"cf":"demographics", "col":"gender", "type":"string"},
        "age":{"cf":"demographics", "col":"age", "type":"string"},
        "level":{"cf":"demographics", "col":"level", "type":"string"}
        }
        }""".stripMargin

    print("[ ****** ] Create DataFrame table emp ")

    def withCatalogInfo(customerinfocatalog: String): DataFrame = {
      sqlContext
        .read
        .options(Map(HBaseTableCatalog.tableCatalog->customerinfocatalog, HBaseRelation.MIN_STAMP -> "0", HBaseRelation.MAX_STAMP -> (start_time_tblscan + 100).toString))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .load()
    }

    print("[ ****** ] Create DataFrame table customer_info ")

    def withCatalogInfoDebug(customerinfodebugcatalog: String): DataFrame = {
      sqlContext
        .read
        .options(Map(HBaseTableCatalog.tableCatalog->customerinfodebugcatalog, HBaseRelation.MIN_STAMP -> "0", HBaseRelation.MAX_STAMP -> (start_time_tblscan + 100).toString))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .load()
    }

    print("[ ****** ] declare DataFrame for table customer_info ")

    val df = withCatalogInfo(customerinfocatalog)

    print("[ ****** ] declare DataFrame for table customer_info_debug ")

    val df_debug = withCatalogInfoDebug(customerinfodebugcatalog)

    print("[ ****** ] here is the dataframe contain: ")

    df.show(10)
    df_debug.show(10)

    val columns = df.columns
    val selectiveDifferences = columns.map(col => df.select(col).except(df_debug.select(col)))

    print("[ *** ] Selective Differences")

    val outer_join = df.join(df_debug, df("key") === df_debug("key"), "left_outer")

    outer_join.show(10)

    diff("key", df, df_debug).show(false)
    diff("key", df, df_debug).show(true)

    print("[ **** ] We have : " + outer_join.count() + " differents rows")

    // selectiveDifferences.toString

    print("[ *** ] Selective Differences only diff columns")

    // selectiveDifferences.map(diff => {if(diff.count > 0) diff.show})

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