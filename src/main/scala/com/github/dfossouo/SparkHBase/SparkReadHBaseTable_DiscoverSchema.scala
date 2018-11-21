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
import org.apache.hadoop.hbase.spark.HBaseContext
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
import org.apache.spark.streaming.dstream


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

    def diff_row(rowkey: String,key1: String, key2: String, df1: DataFrame): DataFrame = {
      df1.where(df1.col(key1).isNull or df1.col(key2).isNull)
    }

    // Get Start time

    val start_time = Calendar.getInstance()
    println("[ *** ] Start Time: " + start_time.getTime().toString)

    // Init properties
    val props = getProps(args(0))

    // Get Start time table Scan
    val tbscan = 1540365233000L
    val start_time_tblscan = tbscan.toString()

    val path = props.get("zookeeper.znode.parent").get
    val table = props.get("hbase.table.name").get
    val quorum = props.get("hbase.zookeeper.quorum").get
    val pathx = props.get("zookeeper.znode.parent_x").get
    val tablex = props.get("hbase.table.name_x").get
    val quorumx = props.get("hbase.zookeeper.quorum_x").get

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
    hConf.set("hbase.zookeeper.quorum", "hdpcluster-15377-master-0.field.hortonworks.com,hdpcluster-15377-compute-2.field.hortonworks.com,hdpcluster-15377-worker-1.field.hortonworks.com:2181")

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


    print("[ ****** ] Create DataFrame table emp ")

    val connectionHbase = s"""{
        "hbase.zookeeper.quorum":"$quorum",
        "zookeeper.znode.parent":"$path"
      }
      """

    def withCatalogInfo(customerinfocatalog: String): DataFrame = {
      sqlContext
        .read
        .options(Map(HBaseTableCatalog.tableCatalog->customerinfocatalog,HBaseRelation.RESTRICTIVE -> "HBaseRelation.Restrictive.none",HBaseRelation.MIN_STAMP -> "0", HBaseRelation.MAX_STAMP -> "15416709729711".toString, HBaseRelation.HBASE_CONFIGURATION -> connectionHbase))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .load()
    }

    print("[ ****** ] Create DataFrame table customer_info ")

    print("[ ****** ] declare DataFrame for table customer_info ")

    val df = withCatalogInfo(customerinfocatalog)

    print("Here are the columns " + df.columns.foreach(println))

    df.columns.map(f => print(f))
    df.show(10,false)

    connection.close()

    println("[ *** ] Creating HBase Configuration cluster 2")

    val hConf2 = HBaseConfiguration.create()
    hConf2.setInt("timeout", 120000)
    hConf2.set("zookeeper.znode.parent_x", "/hbase-unsecure")
    hConf2.set("hbase.zookeeper.quorum_x", "c325-node4.field.hortonworks.com,c325-node3.field.hortonworks.com,c325-node2.field.hortonworks.com:2181")

    val hbaseContext2 = new HBaseContext(sc, hConf2, null)

    def customerinfodebugcatalog= s"""{
        "table":{"namespace":"default", "name":"$tablex"},
        "rowkey":"key",
        "columns":{
        "rowkey":{"cf":"rowkey", "col":"key", "type":"string"},
        "data":{"cf":"demographics", "col":"", "type":"map<string, string>"}
        }
        }""".stripMargin

    // Create Connection
    val connection2: Connection = ConnectionFactory.createConnection(hConf2)

    print("connection 2 created")

    val connectionHbase2 = s"""{
        "hbase.zookeeper.quorum":"$quorumx",
        "zookeeper.znode.parent":"$pathx"
      }
      """

    def withCatalogInfoDebug(customerinfodebugcatalog: String): DataFrame = {
      sqlContext
        .read
        .options(Map(HBaseTableCatalog.tableCatalog->customerinfodebugcatalog,HBaseRelation.RESTRICTIVE -> "HBaseRelation.Restrictive.none",HBaseRelation.MIN_STAMP -> "0", HBaseRelation.MAX_STAMP -> "15416709729711".toString, HBaseRelation.HBASE_CONFIGURATION -> connectionHbase2))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .load()
    }

    print("[ ****** ] declare DataFrame for table customer_info_debug ")

    val df_debug = withCatalogInfoDebug(customerinfodebugcatalog)

    print("[ ****** ] here is the dataframe contain: ")

    df_debug.show(10,false)

    connection2.close()

    print("[ *** ] Selective Differences - Get the count of each dataframe")

    val df_count = df.count()
    val df_debug_count = df_debug.count()

    if(df_count.equals(df_debug_count))
      print("[ ******* ] The number of row is the same between cluster 1 and 2 we have : " + df_count + " rows ")
    else
      print("[ ******* ] The number of row is different between dataframe " + "cluster 1 have " + df_count + " rows and cluster 2 have "
        + df_debug_count + " rows" )

    print("[ *** ] Selective Differences - Get the count of each columns by rowkey")

    val counts_df1 = df.select($"rowkey", size($"data").as("count"))

    val counts_df2= df_debug.select($"rowkey", size($"data").as("count"))

    print("[ ********* +++++++ Here start the difference over columns +++++++++++ ********* ]")

    counts_df1.show(false)
    counts_df2.show(false)

    // Section 1 : If you want to display columns per row
    /*    df.select($"rowkey", explode($"data")).show(false)
        df_debug.select($"rowkey", explode($"data")).show(false)
        val df1 = df.select($"rowkey", explode($"data"))
        val df2 = df_debug.select($"rowkey", explode($"data"))
        val df_difference_value = diff("rowkey","key","value",df1,df2)
        df_difference_value.show(false) */

    // Now taking only rowkey and columns


    // Uncomment this if secition 1 if uncommented
    /*    val dfjoin = counts_df1.join(counts_df2,(counts_df1("rowkey")===counts_df2("rowkey"))&&(counts_df1("count") =!= counts_df2("count")),"inner")
        print("[ ****** ] Here is the Inner Join List of rowkey with different column")
        dfjoin.show(false)
        val dfjoin = counts_df1.join(counts_df2,(counts_df1("rowkey")===counts_df2("rowkey"))&&(counts_df1("count") =!= counts_df2("count")),"inner")
        dfjoin.join(df,dfjoin("rowkey")===df("rowkey")).join(df_debug,dfjoin("rowkey")===df_debug("rowkey")).show(false) */

    // Now that we compared the nb of rows between two tables we need to go deep dive and show the row which are differents

    print("Step 1 ---------------------------------------------------------------- ")

    val dfjoin = counts_df1.join(counts_df2,(counts_df1("rowkey")===counts_df2("rowkey"))&&(counts_df1("count") =!= counts_df2("count")),"inner")

    val colNames_1 = Seq("rowkey1", "count1", "rowkey2","count2")
    dfjoin.toDF(colNames_1: _*)

    print("Step 2 ----------------------------------------------------------------")
    // show the different MaType
    // Update columns names
    val colNames_2 = Seq("rowkey1", "count1", "rowkey2","count2")

    dfjoin


    val newdfjoin = dfjoin.toDF(colNames_2: _*)

    val dftemp = newdfjoin.join(df,newdfjoin("rowkey1")===df("rowkey")).select("rowkey1","data").toDF("rowkey1","datatable1")

    print("Step 3 ----------------------------------------------------------------")

    dftemp.join(df_debug,dftemp("rowkey1")===df_debug("rowkey")).select("rowkey1","datatable1","data").toDF("rowkey","datatable1","datatable2").show(false)

    /*   val colNames = Seq("rowkey", "key1", "key2")
       val newDF = dfjoin.toDF(colNames: _*)
      val df_difference_columns = diff_row("rowkey","key1", "key2",newDF)
       df_difference_columns.show(false) */

    connection.close()
    connection2.close()

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