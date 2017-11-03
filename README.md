# HbaseUtils
## 功能
1. Spark 快速大批量得存取 Hbase
2. 支持隐式的RDD 调用

## 性能说明：
在一个 3台 64c,128G 内存上的hbase 集群上测试：
1. BulkLoad 一个40G 文件 4分钟（regions = 50实际时间和region 个数有关）
2. bulkGet 100000000 条数据从1 的表中时间为 1 分钟
3. bulkDelte 10000000 调数据从1 的表中时间未 1 分钟
## 使用方法
 sbt 打包引入到项目中，参照 HbaseSuit 实现
 
## 使用场景

1. Hbase 作为前端数据快速检索的数据库
    - 数据源为hive 表
    - 数据源为关系型数据库
    

2. Hbase 作为支持支持数据检索、更新的Spark运行数据库
    - bulkLoad 更新
    - bulkGet 查询，Spark SQL Join 解决Hbase 不支持Join 的问题
    - BulkDelete 数据删除
    

