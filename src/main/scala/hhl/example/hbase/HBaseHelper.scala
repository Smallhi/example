package com.lenovo.persona.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase._
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.RegionSplitter.HexStringSplit
import org.apache.hadoop.mapreduce.Job

import scala.collection.mutable

/**
  * Created by huanghl4 on 2017/7/27
  *
  */

class HBaseHelper {
  var nameSpace = "gucp"

  def createHTable(connection: Connection, tablename: String, columns: Array[String]): Unit = {

    val admin = connection.getAdmin
    val tableName = TableName.valueOf(nameSpace + ":" + tablename)

    if (!admin.tableExists(tableName)) {
      if(!admin.getNamespaceDescriptor(nameSpace).getName.equals(nameSpace))
        admin.createNamespace(NamespaceDescriptor.create(nameSpace).build())
      val tableDescriptor = new HTableDescriptor(tableName)

      if (columns != null) {
        columns.foreach(c => {
          val hcd = new HColumnDescriptor(c.getBytes()) //设置列簇
          hcd.setMaxVersions(1)
          hcd.setCompressionType(Algorithm.GZ) //设定数据存储的压缩类型.默认无压缩(NONE)
          tableDescriptor.addFamily(hcd)
        })
      }
      admin.createTable(tableDescriptor)
    }

  }

  /**
    * Hbase自带了两种pre-split的算法,分别是 HexStringSplit 和  UniformSplit
    * 如果我们的row key是十六进制的字符串作为前缀的,就比较适合用HexStringSplit
    * @param tablename 表名
    * @param regionNum 预分区数量
    * @param columns 列簇数组
    */
  def createHTable(connection: Connection, tablename: String,regionNum: Int, columns: Array[String]): Unit = {

    val hexsplit: HexStringSplit = new HexStringSplit()
    val splitkeys: Array[Array[Byte]] = hexsplit.split(regionNum)

    val admin = connection.getAdmin

    val tableName = TableName.valueOf(nameSpace + ":" + tablename)

    if (!admin.tableExists(tableName)) {

      if(!admin.getNamespaceDescriptor(nameSpace).getName.equals(nameSpace))
        admin.createNamespace(NamespaceDescriptor.create(nameSpace).build())

      val tableDescriptor = new HTableDescriptor(tableName)

      if (columns != null) {
        columns.foreach(c => {
          val hcd = new HColumnDescriptor(c.getBytes()) //设置列簇
          hcd.setMaxVersions(1)
          hcd.setCompressionType(Algorithm.GZ) //设定数据存储的压缩类型.默认无压缩(NONE)
          tableDescriptor.addFamily(hcd)
        })
      }
      admin.createTable(tableDescriptor,splitkeys)
    }

  }


  /**
    * short预分区建表:0X0000~0X7FFF
    * @param connection
    * @param tablename 表名
    * @param regionNum 预分区数量
    * @param columns 列簇数组
    */
  def createHTable(connection: Connection, tablename: String,regionNum: Short, columns: Array[String]): Unit = {

    val admin = connection.getAdmin

    val tableName = TableName.valueOf(nameSpace+ ":" + tablename)

    if (!admin.tableExists(tableName)) {

      if(!admin.getNamespaceDescriptor(nameSpace).getName.equals(nameSpace))
        admin.createNamespace(NamespaceDescriptor.create(nameSpace).build())

      val tableDescriptor = new HTableDescriptor(tableName)

      if (columns != null) {
        columns.foreach(c => {
          val hcd = new HColumnDescriptor(c.getBytes()) //设置列簇
          hcd.setMaxVersions(1)
          hcd.setCompressionType(Algorithm.GZ) //设定数据存储的压缩类型.默认无压缩(NONE)
          tableDescriptor.addFamily(hcd)
        })
      }
      val start = (0x7FFF / regionNum).toShort
      val end = (0x7FFF - start).toShort
      admin.createTable(tableDescriptor,Bytes.toBytes(start),Bytes.toBytes(end),regionNum)
    }

  }

  def deleteHTable(connection: Connection, tn: String): Unit = {
    val tableName = TableName.valueOf(nameSpace + ":" + tn)
    val admin = connection.getAdmin
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName)
      admin.deleteTable(tableName)
    }
  }

  def insertRecord(connection: Connection, tablename: String, family: String, column: String, key: String, value: String): Unit = {
    var table: Table = null
    try {
      val userTable = TableName.valueOf(tablename)
      table = connection.getTable(userTable)
      val p = new Put(key.getBytes)
      p.addColumn(family.getBytes, column.getBytes, value.getBytes())
      table.put(p)
    } finally {
      if (table != null)
        table.close()
    }
  }

  def batchInsertRecords(tablename:String, family:String, column:String, rdd: RDD[(String, String)]): Unit = {
    val conf = this.getConf
    val jobConf = new JobConf(conf)
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tablename)
    jobConf.setOutputFormat(classOf[TableOutputFormat])

    rdd.map(r => {
      val put = new Put(Bytes.toBytes(r._1))
      put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(r._2))
      (new ImmutableBytesWritable, put)
    }).saveAsHadoopDataset(jobConf)
  }

  def getRecord(connection: Connection, tablename: String, family: String, column: String, key: String): String = {
    var table: Table = null
    try {
      val userTable = TableName.valueOf(tablename)
      table = connection.getTable(userTable)
      val g = new Get(key.getBytes())
      val result = table.get(g)
      Bytes.toString(result.getValue(family.getBytes(), column.getBytes()))
    } finally {
      if (table != null)
        table.close()
    }
  }

  def deleteRecord(connection: Connection, tablename: String, family: String, column: String, key: String): Unit = {
    var table: Table = null
    try {
      val userTable = TableName.valueOf(tablename)
      table = connection.getTable(userTable)
      val d = new Delete(key.getBytes())
      d.addColumn(family.getBytes(), column.getBytes())
      table.delete(d)
    } finally {
      if (table != null)
        table.close()
    }
  }

  def scanHTable(connection: Connection, tablename: String, family: String, column: String): mutable.HashMap[String, String] = {
    var table: Table = null
    var scanner: ResultScanner = null
    try {
      val map = new mutable.HashMap[String, String]()
      val userTable = TableName.valueOf(tablename)
      table = connection.getTable(userTable)
      val s = new Scan()
      s.addColumn(family.getBytes(), column.getBytes())
      scanner = table.getScanner(s)
      println("scan...for...")
      var result: Result = scanner.next()
      while (result != null) {
        map.put(Bytes.toString(result.getRow),
          Bytes.toString(result.getValue(family.getBytes(), column.getBytes())))
        result = scanner.next()
      }

      map

    } finally {
      if (table != null)
        table.close()
      scanner.close()
    }
  }

  def getConf: Configuration = {
    val conf: Configuration = HBaseConfiguration.create
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "node81.it.leap.com,node82.it.leap.com")
    conf.set("hbase.client.keyvalue.maxsize", "1048576000") //500M
    conf.set("zookeeper.znode.parent","/hbase-unsecure")
    conf
  }

  def getConnection: Connection = {
    val conf = getConf
    ConnectionFactory.createConnection(conf)

  }

  def execute(action: Connection => Unit): Unit = {

    val connection = getConnection
    try {
      action(connection)
    }
    finally {
      connection.close()
    }
  }

  def saveHBaseByBulkLoad(bulkRdd: RDD[(ImmutableBytesWritable, KeyValue)],bulkFloder:String,tableName:String) = {
    val conf = getConf
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    conf.set("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", "500")
    //val conf = HBaseConfiguration.create();

    val hbTableName = TableName.valueOf(tableName)
    val connection = ConnectionFactory.createConnection(conf)

    val realTable = connection.getTable(hbTableName)
    try {
      lazy val job = Job.getInstance(conf)
      job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
      job.setMapOutputValueClass(classOf[KeyValue])
      //    val regionLocator = connection.getRegionLocator(hbTableName)
      val regionLocator = new HRegionLocator(hbTableName, connection.asInstanceOf[ClusterConnection])
      HFileOutputFormat2.configureIncrementalLoad(job, realTable, regionLocator)

      bulkRdd.saveAsNewAPIHadoopFile(bulkFloder, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], job.getConfiguration())
      // bulk load start
      val loader = new LoadIncrementalHFiles(conf)
      val admin = connection.getAdmin()
      loader.doBulkLoad(new Path(bulkFloder), admin, realTable, regionLocator)
    } catch {
      case e: Exception => e.printStackTrace()
      case t: Throwable => t.printStackTrace() // TODO: handle error
    } finally {
      realTable.close()
      connection.close()
    }

  }
}
