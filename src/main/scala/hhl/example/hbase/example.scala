package com.lenovo.persona.hbase

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by huanghl4 on 2017/9/22.
  */
object example {


  def main(args: Array[String]): Unit = {

    //全量和增量大批量写入Hbase
    val sparkConf = new SparkConf().setAppName("HBaseBulkDeleteExample ")
    val sc = new SparkContext(sparkConf)


    //根据目标表的region 对rdd 进行分区，然后查询


    val conf = HBaseConfiguration.create()
    val tableName = TableName.valueOf("gucp:USER_GRAPH_TEST")
    conf.set("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily","500")


    val conn = HBaseConnectionCache.getConnection(conf)
    val regionLocator = conn.getRegionLocator(tableName)

    val startKeys = regionLocator.getStartKeys

    if (startKeys.length == 0) {
      println("Table " + tableName.toString + " was not found")
    }

    val regionSplitPartitioner = new BulkLoadPartitioner(startKeys,1)

      //repartitionAndSortWithinPartitions 只支持 KV 排序，应该讲数据写成（rowkey,Map(列名,列值的形式)）
    val rdd = sc.textFile("").map(x=>(x,x))

    rdd.repartitionAndSortWithinPartitions(regionSplitPartitioner)

  }

}
