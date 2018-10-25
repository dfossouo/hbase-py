
/*******************************************************************************************************
This code does the following:
  1) Read an HBase Snapshot, and convert to Spark RDD (snapshot name is defined in props file)
  2) Parse the records / KeyValue (extracting column family, column name, timestamp, value, etc)
  3) Perform general data processing - Filter the data based on rowkey range AND timestamp (timestamp threshold variable defined in props file)
  4) Write the results to HDFS (formatted for HBase BulkLoad, saved as HFileOutputFormat)

Usage:

spark-submit --class com.github.dfossouo.SparkHBase.SparkReadHBaseSnapshot --jars /tmp/SparkHBaseExample-0.0.1-SNAPSHOT.jar /usr/hdp/current/phoenix-client/phoenix-client.jar /tmp/props

********************************************************************************************************/  

package com.github.dfossouo.SparkHBase;

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.avg

import scala.collection.mutable.HashMap
import scala.io.Source.fromFile
import scala.collection.JavaConverters._

import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableSnapshotInputFormat}
import org.apache.hadoop.hbase.util.Base64
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor}
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.KeyValue.Type
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles

import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

import java.text.SimpleDateFormat
import java.util.Arrays
import java.util.Date
import java.util.Calendar
import java.lang.String

import sys.process._


object SparkReadHBaseSnapshot{
 
  case class hVar(rowkey: Int, colFamily: String, colQualifier: String, colDatetime: Long, colDatetimeStr: String, colType: String, colValue: String)
 
  def main(args: Array[String]) {

    val start_time = Calendar.getInstance()
    println("[ *** ] Start Time: " + start_time.getTime().toString)
    
    val props = getProps(args(0))
    val max_versions : Int = props.getOrElse("hbase.snapshot.versions","3").toInt
    
    val sparkConf = new SparkConf().setAppName("SparkReadHBaseSnapshot")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    println("[ *** ] Creating HBase Configuration cluster 1")
    val hConf = HBaseConfiguration.create()
    hConf.set("hbase.rootdir", props.getOrElse("hbase.rootdir", "/tmp"))
    hConf.set("hbase.zookeeper.quorum",  props.getOrElse("hbase.zookeeper.quorum", "hdpcluster-15377-master-0.field.hortonworks.com:2181"))
    hConf.set(TableInputFormat.SCAN, convertScanToString(new Scan().setMaxVersions(max_versions)) )

    val job = Job.getInstance(hConf)

    val path = new Path(props.getOrElse("hbase.snapshot.path", "/user/hbase"))
    val snapName = props.getOrElse("hbase.snapshot.name", "hbase_simulated_50m")

    TableSnapshotInputFormat.setInput(job, snapName, path)

    val hBaseRDD = sc.newAPIHadoopRDD(job.getConfiguration,
      classOf[TableSnapshotInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val record_count_raw = hBaseRDD.count()
    println("[ *** ] Read in SnapShot of table on cluster 1 (" + snapName.toString  + "), which contains " + record_count_raw + " records")

    // Extract the KeyValue element of the tuple
    val keyValue = hBaseRDD.map(x => x._2).map(_.list)

    println("[ *** ] Printing raw SnapShot (10 records) from HBase SnapShot")
    hBaseRDD.map(x => x._1.toString).take(10).foreach(x => println(x))
    hBaseRDD.map(x => x._2.toString).take(10).foreach(x => println(x))
    keyValue.map(x => x.toString).take(10).foreach(x => println(x))

    val df = keyValue.flatMap(x =>  x.asScala.map(cell =>
      hVar(
        Bytes.toInt(CellUtil.cloneRow(cell)),
        Bytes.toStringBinary(CellUtil.cloneFamily(cell)),
        Bytes.toStringBinary(CellUtil.cloneQualifier(cell)),
        cell.getTimestamp,
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS").format(new Date(cell.getTimestamp.toLong)),
        Type.codeToType(cell.getTypeByte).toString,
        Bytes.toStringBinary(CellUtil.cloneValue(cell))
      )
    )
    ).toDF()

    println("[ *** ] Printing parsed SnapShot (10 records) from HBase SnapShot Cluster 1")
    df.show(10, false)

    //Get timestamp (from props) that will be used for filtering
    val datetime_threshold      = props.getOrElse("datetime_threshold", "2018-10-16 11:24:02:001")
    val datetime_threshold_long = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS").parse(datetime_threshold).getTime()
    println("[ *** ] Filtering/Keeping all SnapShot records that are more recent (greater) than the datetime_threshold (set in the props file): " + datetime_threshold.toString)

    println("[ *** ] Filtering Dataframe")
    val df_filtered = df.filter($"colDatetime" >= datetime_threshold_long && $"rowkey".between(80001, 90000))

    println("[ *** ] Filtered dataframe contains cluster 2 " + df_filtered.count() + " records")
    println("[ *** ] Printing filtered HBase SnapShot records (10 records) of cluster 2")
    df_filtered.show(10, false)

    // Second Part Cluster 2

    println("[ *** ] Creating HBase Configuration cluster 2")
    val hConfx = HBaseConfiguration.create()
    hConfx.set("hbase.rootdir", props.getOrElse("hbase.rootdir_x", "/tmp"))
    hConfx.set("hbase.zookeeper.quorum",  props.getOrElse("hbase.zookeeper.quorum_x", "hdpcluster-15377-master-0.field.hortonworks.com:2181"))
    hConfx.set(TableInputFormat.SCAN, convertScanToString(new Scan().setMaxVersions(max_versions)) )


    val jobx = Job.getInstance(hConfx)

    val pathx = new Path(props.getOrElse("hbase.snapshot.path_x", "/user/hbase"))
    val snapNamex = props.getOrElse("hbase.snapshot.name_x", "hbase_simulated_50mx")


    TableSnapshotInputFormat.setInput(jobx, snapNamex, pathx)

    val hBaseRDDx = sc.newAPIHadoopRDD(jobx.getConfiguration,
        classOf[TableSnapshotInputFormat],
        classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
        classOf[org.apache.hadoop.hbase.client.Result])
    
    val record_count_rawx = hBaseRDDx.count()
    println("[ *** ] Read in SnapShot of table on cluster 2 (" + snapNamex.toString  + "), which contains " + record_count_rawx + " records")
 
    // Extract the KeyValue element of the tuple
    val keyValuex = hBaseRDDx.map(x => x._2).map(_.list)

    println("[ *** ] Printing raw SnapShot (10 records) from HBase SnapShot")         
    hBaseRDDx.map(x => x._1.toString).take(10).foreach(x => println(x))
    hBaseRDDx.map(x => x._2.toString).take(10).foreach(x => println(x))
    keyValuex.map(x => x.toString).take(10).foreach(x => println(x))

    val dfx = keyValuex.flatMap(x =>  x.asScala.map(cell =>
        hVar(
            Bytes.toInt(CellUtil.cloneRow(cell)),
            Bytes.toStringBinary(CellUtil.cloneFamily(cell)),
            Bytes.toStringBinary(CellUtil.cloneQualifier(cell)),
            cell.getTimestamp,
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS").format(new Date(cell.getTimestamp.toLong)),
            Type.codeToType(cell.getTypeByte).toString,
            Bytes.toStringBinary(CellUtil.cloneValue(cell))
            )
        )
    ).toDF()

    println("[ *** ] Printing parsed SnapShot (10 records) from HBase SnapShot cluster 2")
    dfx.show(10, false)

    //Get timestamp (from props) that will be used for filtering
    val datetime_threshold_x      = props.getOrElse("datetime_threshold", "2018-10-16 11:24:02:001")
    val datetime_threshold_x_long = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS").parse(datetime_threshold_x).getTime()
    println("[ *** ] Filtering/Keeping all SnapShot records that are more recent (greater) than the datetime_threshold (set in the props file): " + datetime_threshold_x.toString)
    
    println("[ *** ] Filtering Dataframe")
    val df_filtered_x = dfx.filter($"colDatetime" >= datetime_threshold_x_long && $"rowkey".between(80001, 90000))

    println("[ *** ] Filtered dataframe contains cluster 2 " + df_filtered_x.count() + " records")
    println("[ *** ] Printing filtered HBase SnapShot records (10 records) of cluster 2")
    df_filtered_x.show(10, false)

    sc.stop()


  }  


  def convertScanToString(scan : Scan) = {
      val proto = ProtobufUtil.toScan(scan);
      Base64.encodeBytes(proto.toByteArray());
  }


  def getArrayProp(props: => HashMap[String,String], prop: => String): Array[String] = {
    return props.getOrElse(prop, "").split(",").filter(x => !x.equals(""))
  }


  def getProps(file: => String): HashMap[String,String] = {
    var props = new HashMap[String,String]
    val lines = fromFile(file).getLines
    lines.foreach(x => if (x contains "=") props.put(x.split("=")(0), if (x.split("=").size > 1) x.split("=")(1) else null))
    props
  }

}

//ZEND
