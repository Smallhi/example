# HbaseETL
## 功能特色
1. Spark 快速大批量得存取 Hbase
2. 支持隐式的RDD 调用

## 性能说明：
在一个 3台 64c,128G 内存上的hbase 集群上测试：
1. BulkLoad 一个40G 文件 4分钟（regions = 50实际时间和region 个数有关）
2. bulkGet 100000000 条数据从1 的表中时间为 1 分钟
3. bulkDelte 10000000 调数据从1 的表中时间为 1 分钟
## 使用方法
 sbt 打包引入到项目中，参照 HbaseSuit 实现
 
## 使用场景

1. Hbase 作为前端数据快速检索的数据库
    - 数据源为hive 表
    - 数据源为关系型数据库
    - 参考DataBaseSuit.scala 实现

例如将hive 表的数据增量写入到Hbase    
   
 ```scala
   def insertOrUpdate = {
     val rdd = spark.read.table("").rdd
     hc.bulkLoadThinRows[Row](rdd,
       tableName,
       r => {
         val rawPK = new StringBuilder
         for(c<- table_PK) rawPK.append(r.getAs[String](c))
         val rk = rowKeyByMD5(rawPK.toString)
         val familyQualifiersValues = new FamiliesQualifiersValues
         val fq = familyQualifierToByte
         for(c<- fq) {
           val family = c._1
           val qualifier = c._2
           val value = Bytes.toBytes(r.getAs[String](c._3))
           familyQualifiersValues += (family, qualifier, value)
         }
         (new ByteArrayWrapper(rk), familyQualifiersValues)
       },
       10)
   }
 ```   

2. Hbase 作为支持支持数据检索、更新的Spark运行数据库
    - bulkLoad 更新
    - bulkGet 查询，Spark SQL Join 解决Hbase 不支持Join 的问题
    - BulkDelete 数据删除
    - 参考 HbaseSuit.scala 实现
    
例如向Hbase 批量导入数据

```scala
def initDate() = {
    // 清空，并重新创建表
    createTable
    // 准备数据，rdd 处理
    import spark.implicits._
    val rdd = spark.sql("select * from hive.graph").map(x => {
      val sid = x.getString(0)
      val id = x.getString(1)
      val idType = x.getString(3)
      (sid, id, idType)
    }).rdd
    // bulk load
    hc.bulkLoadThinRows[(String, String, String)](rdd,
      "lenovo:GRAPH",
      t => {
        val rowKey = rowKeyByMD5(t._2, t._3)
        val familyQualifiersValues = new FamiliesQualifiersValues
        val pk = t._2 + "|" + t._3
        // Hbase 存入两列，一列 PK 存 业务主键，一列 s 存 superid
        val column = List(("pk", pk), ("s", t._1))
        column.foreach(f => {
          val family: Array[Byte] = Bytes.toBytes(columnFamily.head)
          val qualifier = Bytes.toBytes(f._1)
          val value: Array[Byte] = Bytes.toBytes(f._2)
          familyQualifiersValues += (family, qualifier, value)
        })
        (new ByteArrayWrapper(rowKey), familyQualifiersValues)
      },
      10
    )
  }
```    
    

3. 声明
    - 该ETL 工具改造自 hbase-Spark 并对其中BulkLoad 方法重新实现
    - 如有任务问题请联系作者邮箱 huanghl0817@gmail.com