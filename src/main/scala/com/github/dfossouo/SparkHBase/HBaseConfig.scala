package main.scala.com.github.dfossouo.SparkHBase

import com.github.dfossouo.SparkHBase.SparkReadHBaseTable_DiscoverSchema.getProps
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat

import scala.collection.mutable.HashMap
import scala.io.Source.fromFile

/**
  * Wrapper for HBaseConfiguration
  */
class HBaseConfig(defaults: Configuration) extends Serializable {
  def get: Configuration = HBaseConfiguration.create(defaults)
}

object HBaseConfig {

  def getProps(file: => String): HashMap[String, String] = {
    var props = new HashMap[String, String]
    val lines = fromFile(file).getLines
    lines.foreach(x => if (x contains "=") props.put(x.split("=")(0), if (x.split("=").size > 1) x.split("=")(1) else null))
    props
  }

  def apply(hConf: Configuration): HBaseConfig = new HBaseConfig(hConf)

  def apply(options: (String, String)*): HBaseConfig = {
    val hConf = HBaseConfiguration.create

    for ((key, value) <- options) { hConf.set(key, value) }

    apply(hConf)
  }



  def apply(hConf: { def rootdir: String; def quorum: String; def start_time_tblscan: String; def end_time_tblscan: String }): HBaseConfig =
    apply("timeout" -> "120000",
      "hbase.rootdir" -> "/tmp",
      "zookeeper.znode.parent" -> "/hbase-unsecure",
      "hbase.zookeeper.quorum" -> "hdpcluster-15377-master-0.field.hortonworks.com:2181",
      TableInputFormat.INPUT_TABLE -> "customer_info",
      TableInputFormat.SCAN_TIMERANGE_START -> hConf.start_time_tblscan,
      TableInputFormat.SCAN_TIMERANGE_END -> hConf.end_time_tblscan,
      "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer")

}
