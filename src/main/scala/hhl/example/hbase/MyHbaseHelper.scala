package com.lenovo.persona.hbase

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.PrefixFilter
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scala.collection.JavaConversions._
import scala.collection.mutable


/**
  * Created by menggb1 on 17-8-29.
  */
class MyHbaseHelper extends HBaseHelper{

  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.sss")

  def scaneByPrefixFilter(connection: Connection, tablename: String, startRow: Short, rowPrifix: String): mutable.HashMap[String, String] = {
    val map = new mutable.HashMap[String, String]()
    var table: Table = null
    var resultScan: ResultScanner = null
    println(sdf.format(new Date()) + " start scan")
    try {
      val userTable = TableName.valueOf(tablename)
      table = connection.getTable(userTable)

      val rowkey = Bytes.add(Bytes.toBytes(startRow), Bytes.toBytes("|" + rowPrifix))
      val scan = new Scan(Bytes.toBytes(startRow), new PrefixFilter(rowkey))
      //scan.setCaching(10000 * 2);//设置hbase单次获取到内存的数据条数
      //      scan.setFilter(new PrefixFilter(rowPrifix.getBytes()))
      resultScan = table.getScanner(scan)

      for (result <- resultScan) {
        val cellItertor = result.listCells().iterator()
        while (cellItertor.hasNext){
          val cell = cellItertor.next()
          val columnName = Bytes.toString(CellUtil.cloneQualifier(cell)) //列名
          val value = Bytes.toString(CellUtil.cloneValue(cell)) //列值
              map.put(Bytes.toString(result.getRow),value)
          val rowkey = result.getRow
          println(sdf.format(new Date()) + " scan rowkey=>" + Bytes.toString(rowkey) + "columnName=>" + columnName + ",value=>" + value)
          for(i <- 0 until(rowkey.length)){
            print(rowkey(i) + ",")
          }
          println("==========================")
        }
      }
      println("*************************************************************************************8")
    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }finally {
      if (table != null){
        table.close()
        resultScan.close()
      }
    }
    return map
  }

//  def sortRddByHbaseRegion(rdd:RDD[Array[Byte]], tableName:String):RDD[Array[Byte]]= {
//    val hbase = new HBaseHelper
//    val conf = hbase.getConf
//    val conn = hbase.getConnection
//    val jobConf = new JobConf(conf)
//
//    val regionLocator = conn.getRegionLocator(TableName.valueOf(tableName))
//
//    val startKeys = regionLocator.getStartKeys
//
//    if (startKeys.length == 0) {
//      println("Table " + tableName.toString + " was not found")
//    }
//    val regionSplitPartitioner = new BulkLoadPartitioner(startKeys)
//    val x = rdd.map(x=>(x,x))//.repartitionAndSortWithinPartitions(regionSplitPartitioner)
//
//  }

}
