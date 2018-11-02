
/*******************************************************************************************************
This code does the following:
  1) Read an HBase Snapshot, and convert to Spark RDD (snapshot name is defined in props file)
  2) Parse the records / KeyValue (extracting column family, column name, timestamp, value, etc)
  3) Perform general data processing - Filter the data based on rowkey range AND timestamp (timestamp threshold variable defined in props file)
  4) Write the results to HDFS (formatted for HBase BulkLoad, saved as HFileOutputFormat)

Usage:

spark-submit --class com.github.dfossouo.SparkHBase.SparkReadHBaseTable_DiscoverSchema --master yarn --deploy-mode client --driver-memory 2g --executor-memory 4g --executor-cores 1 --jars ./HBaseCRC-0.0.2-SNAPSHOT.jar /usr/hdp/current/phoenix-client/phoenix-client.jar /tmp/props
  ********************************************************************************************************/

package com.github.dfossouo.SparkHBase

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



import org.apache.hadoop.hbase.mapreduce.TableInputFormat


object SparkReadHBaseTable_DiscoverSchema {

  case class hVar(rowkey: Int, colFamily: String, colQualifier: String, colDatetime: Long, colDatetimeStr: String, colType: String, colValue: String)
  case class customer_info(custid: String, gender: String, age: String, level: String)

  case class Customer(rowkey: String)

  object Customer extends Serializable {
    def parseCustomer(result: Result): Customer = {
      val rowkey = Bytes.toString(result.getRow())
      // remove time from rowKey, stats row key is for day
      val p0 = rowkey.split(" ")(0)
      Customer(p0)
    }
  }

  def main(args: Array[String]) {


    // Get Start time

    val start_time = Calendar.getInstance()
    println("[ *** ] Start Time: " + start_time.getTime().toString)

    // Init properties
    val props = getProps(args(0))

    // Get Start time table Scan
    val tbscan_start = 1539350424527L
    val tbscan_end =   1540821806523L
    val start_time_tblscan = tbscan_start.toString()
    val end_time_tblscan = tbscan_end.toString()
    val columnfamily = props.getOrElse("hbase.table.family", "demographics")
    val columnfields = props.getOrElse("hbase.table.fields", "custid,gender,age,level").split(",")
    val tablename = props.get("hbase.table.name")

    // Create Spark Application
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sparkConf = new SparkConf().setAppName("SparkReadHBaseTable_DiscoverSchema")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._

    println("[ *** ] Creating HBase Configuration cluster 1")
    @transient val hConf = HBaseConfiguration.create()
    hConf.setInt("timeout", 120000)
    hConf.set("hbase.rootdir", props.getOrElse("hbase.rootdir", "/tmp"))
    hConf.set("zookeeper.znode.parent", props.getOrElse("zookeeper.znode.parent", "/hbase-unsecure"))
    hConf.set("hbase.zookeeper.quorum", props.getOrElse("hbase.zookeeper.quorum", "hdpcluster-15377-master-0.field.hortonworks.com:2181"))
    hConf.set(TableInputFormat.INPUT_TABLE, props.get("hbase.table.name").get)
    hConf.set(TableInputFormat.SCAN_TIMERANGE_START, start_time_tblscan)
    hConf.set(TableInputFormat.SCAN_TIMERANGE_END, end_time_tblscan)
    hConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    // Create Connection Cluster 1
    val connection: Connection = ConnectionFactory.createConnection(hConf)

    print("connection created")

    print("[ **** ] Print Here is the Navigable Map which contain HBASE RDD - Cluster 1")

    val hBaseRDD = sc.newAPIHadoopRDD(hConf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]).map(kv => (kv._1.get(), navMapToMap(kv._2.getMap))).map(kv => (Bytes.toString(kv._1), rowToStrMap(kv._2))).take(10).foreach(println)

    val hBaseRDD_compare = sc.newAPIHadoopRDD(hConf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

 //   val HBaseDF = hBaseRDD_compare.toDF()
    val HBaseDF_schema = hBaseRDD_compare.first()
    println("Here second column "+ HBaseDF_schema._2.raw())
    println("Here first column " + HBaseDF_schema._1.get())

    val hBaseRDD_compare_brut = sc.newAPIHadoopRDD(hConf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]).map(tuple => tuple._2)

    val res = hBaseRDD_compare.take(10)

    val rs = res(0)._2

    val kv = rs.raw()

    print(" [ Start of KV ROWS ]")

    kv.take(10)

    print(" [ End of KV ROWS ]")

    var init = 0

/*    val customer = for(keyvalue <- kv) {
      println("rowkey:"+ new String(keyvalue.getRow))
      println(" cf:"+new String(keyvalue.getFamily()) + " column:" + new String(keyvalue.getQualifier) + " " + "value:"+new String(keyvalue.getValue()))
    }

*/

//    var SeqCustomer = Seq("demographics")
    var SeqCustomer: Seq[String] = Seq()

    val listcustomer = for(keyvalue <- kv)  SeqCustomer = SeqCustomer :+ new String(keyvalue.getQualifier)

    var finalSeqCustomer = Seq("row")

    val listcustomerx = for(keyvalue <- kv)  finalSeqCustomer = finalSeqCustomer :+ new String(keyvalue.getQualifier)


    val CustomerColumnLength = SeqCustomer.length

    print(SeqCustomer.mkString("[",",","]"))

    print(" first one: " + SeqCustomer(CustomerColumnLength-2).mkString(""))
    print(" second one: " + SeqCustomer(CustomerColumnLength-1).mkString(""))
    print(" last one: " + SeqCustomer.mkString(",").split(",").mkString("\"", "\",\"", "\""))

    print("\"" + SeqCustomer(0).mkString("") + "\"")
    print("\"" + SeqCustomer(1).mkString("") + "\"")
    print("\"" + SeqCustomer(2).mkString("") + "\"")
    print("\"" + SeqCustomer(3).mkString("") + "\"")

    println("Here is the finalSeqCustomer : " + finalSeqCustomer.mkString(",").split(",").mkString("\"", "\",\"", "\""))

    print("\"" + finalSeqCustomer(2).mkString("") + "\"")
    print("\"" + finalSeqCustomer(3).mkString("") + "\"")
    print("\"" + finalSeqCustomer(4).mkString("") + "\"")
    print("\"" + finalSeqCustomer(1).mkString("") + "\"")

    val customer_family_1 = hBaseRDD_compare_brut.map(r => (Bytes.toString(r.getRow),
      Bytes.toString(r.getValue(Bytes.toBytes("\"" +columnfamily + "\""), Bytes.toBytes("\"" + finalSeqCustomer(2).mkString("") + "\""))),
      Bytes.toString(r.getValue(Bytes.toBytes("\"" +columnfamily + "\""), Bytes.toBytes("\"" + finalSeqCustomer(3).mkString("") + "\""))),
      Bytes.toString(r.getValue(Bytes.toBytes("\"" +columnfamily + "\""), Bytes.toBytes("\"" + finalSeqCustomer(4).mkString("") + "\""))),
      Bytes.toString(r.getValue(Bytes.toBytes("\"" +columnfamily + "\""), Bytes.toBytes("\"" + finalSeqCustomer(1).mkString("") + "\""))))
    ).toDF("row","\"" + finalSeqCustomer(2).mkString("") + "\"","\"" + finalSeqCustomer(3).mkString("") + "\"","\"" + finalSeqCustomer(4).mkString("") + "\"","\"" + finalSeqCustomer(1).mkString("") + "\"")

    val customer_family = hBaseRDD_compare_brut.map(r => (Bytes.toString(r.getRow),
      Bytes.toString(r.getValue(Bytes.toBytes("\"" +columnfamily + "\""), Bytes.toBytes("\"" + finalSeqCustomer(2).mkString("") + "\""))),
      Bytes.toString(r.getValue(Bytes.toBytes("\"" +columnfamily + "\""), Bytes.toBytes("\"" + finalSeqCustomer(3).mkString("") + "\""))),
      Bytes.toString(r.getValue(Bytes.toBytes("\"" +columnfamily + "\""), Bytes.toBytes("\"" + finalSeqCustomer(4).mkString("") + "\""))),
      Bytes.toString(r.getValue(Bytes.toBytes("\"" +columnfamily + "\""), Bytes.toBytes("\"" + finalSeqCustomer(1).mkString("") + "\""))))
    ).toDF("row","\"" + finalSeqCustomer(2).mkString("") + "\"","\"" + finalSeqCustomer(3).mkString("") + "\"","\"" + finalSeqCustomer(4).mkString("") + "\"","\"" + finalSeqCustomer(1).mkString("") + "\"")

    customer_family.registerTempTable("customer_family")

    val df = sqlContext.sql("SELECT * FROM customer_family limit 10")

    df.collect.foreach(println)



    println("[ *** ] Creating HBase Configuration cluster 2")
    @transient val hConf2 = HBaseConfiguration.create()
    hConf2.setInt("timeout", 120000)
    hConf2.set("hbase.rootdir", props.getOrElse("hbase.rootdir", "/tmp"))
    hConf2.set("zookeeper.znode.parent", props.getOrElse("zookeeper.znode.parent", "/hbase-unsecure"))
    hConf2.set("hbase.zookeeper.quorum", props.getOrElse("hbase.zookeeper.quorum", "hdpcluster-15377-master-0.field.hortonworks.com:2181"))
    hConf2.set(TableInputFormat.INPUT_TABLE, props.get("hbase.table.name_x").get)
    hConf2.set(TableInputFormat.SCAN_TIMERANGE_START, start_time_tblscan)
    hConf2.set(TableInputFormat.SCAN_TIMERANGE_END, end_time_tblscan)
    hConf2.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    // Create Connection Cluster 2
    val connection2: Connection = ConnectionFactory.createConnection(hConf2)

    print("connection created")


    print("[ **** ] Print Here is the Navigable Map which contain HBASE RDD - Cluster 2")

    val hBaseRDDx = sc.newAPIHadoopRDD(hConf2, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]).map(kv => (kv._1.get(), navMapToMap(kv._2.getMap))).map(kv => (Bytes.toString(kv._1), rowToStrMap(kv._2))).take(10).foreach(println)

    val hBaseRDDx_compare = sc.newAPIHadoopRDD(hConf2, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]).map(tuple => tuple._2)

    val customerx = hBaseRDDx_compare.map(r => (Bytes.toString(r.getRow),
      Bytes.toString(r.getValue(Bytes.toBytes("demographics"), Bytes.toBytes("custid"))),
      Bytes.toString(r.getValue(Bytes.toBytes("demographics"), Bytes.toBytes("gender"))),
      Bytes.toString(r.getValue(Bytes.toBytes("demographics"), Bytes.toBytes("level"))),
      Bytes.toString(r.getValue(Bytes.toBytes("demographics"), Bytes.toBytes("age")))) //
    ).toDF("row", "custid", "gender", "level", "age")

    customerx.registerTempTable("customerx")

    val df2 = sqlContext.sql("SELECT * FROM customerx limit 10")

    df2.collect.foreach(println)

    print("[ *** ] Selective Differences only diff columns")

//    print(" [ ***** ] We have in the leftOuterJoin: " + joins + " Lines")

//    print(" [ ***** ] Here is the result of join " + joins.count()
//     + " [ ******** ]")


    // selectiveDifferences.map(diff => {if(diff.count > 0) diff.show})

    println("[ *** ] Ending HBase Configuration cluster 1")

    connection.close()
    connection2.close()

  }

  // define HBaseRow and Timeseries

  type HBaseRow = java.util.NavigableMap[Array[Byte], java.util.NavigableMap[Array[Byte], java.util.NavigableMap[java.lang.Long, Array[Byte]]]]
  type CFTimeseriesRow = Map[Array[Byte], Map[Array[Byte], Map[Long, Array[Byte]]]]
  type CFTimeseriesRowStr = scala.collection.immutable.Map[String, scala.collection.immutable.Map[String, scala.collection.immutable.Map[Long, String]]]


  def rowToStrMap(navMap: CFTimeseriesRow): CFTimeseriesRowStr = navMap.map(cf =>
    (Bytes.toString(cf._1), cf._2.map(col =>
      (Bytes.toString(col._1), col._2.map(elem => (elem._1, Bytes.toString(elem._2)))))))
  def navMapToMap(navMap: HBaseRow): CFTimeseriesRow =
    navMap.asScala.toMap.map(cf =>
      (cf._1, cf._2.asScala.toMap.map(col =>
        (col._1, col._2.asScala.toMap.map(elem => (elem._1.toLong, elem._2))))))

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

  // Create difference between dataframe function

  def diff(key: String, df1: DataFrame, df2: DataFrame): DataFrame = {
    val fields = df1.schema.fields.map(_.name)
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


}
//ZEND