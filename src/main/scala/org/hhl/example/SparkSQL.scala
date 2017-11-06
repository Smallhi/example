package org.hhl.example

import org.apache.spark.sql.functions.{collect_list, collect_set}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, parse, render}

import scala.collection.JavaConverters._

/**
  * Created by huanghl4 on 2017/11/6.
  */
object SparkSQL {
  // 获取SparkSession, spark 操作得入口
  val spark = SparkSession.builder()
    .appName(s"${this.getClass.getSimpleName}")
    .enableHiveSupport().getOrCreate()

  // 通过字符串拼接，实现多列聚合
  def multiColumnAggWithConcatStr = {
    // 拼接
    val data = spark.sql("select sid,id,idType,tag from hive.user").as[UserTag].map(x=> (x.sid,x.id + "|" + x.idType,x.tag)).toDF("sid","vid","tag")
    // 或
    //val data = spark.sql("select sid,concat(id,'|',idType),tag from hive.user").map(x=> (x.getString(0),x.getString(1),x.getString(2))
    // 聚合, 聚合函数必须导入org.apache.spark.sql.functions._
    import org.apache.spark.sql.functions._
    val dataAgg = data
      .groupBy("sid")
      .agg(
        collect_set("vid") as "ids",
        collect_list("tag") as "tags"
      ).select("sid","ids","tags").map(x =>{
      val sid = x.getString(0)
      val ids = x.getList[String](1).asScala.toList
      val tag = x.getList[String](2).asScala.toList
      (sid,strToJson(ids),listToJson(tag))
    }).toDF("sid","ids","tags")
    // 数据传输到ElasticSearch
    saveToES(dataAgg)
  }

  //通过Json实现多列聚合
  def multiColumnAggWithJson = {
    val data = spark.sql("select sid,id,idType,tag from hive.user").as[UserTag].map(x=>
      (x.sid,listToJson(List(x.id,x.idType)),x.tag))
    val dataAgg = data
      .groupBy("sid")
      .agg(
        collect_set("vid") as "ids",
        collect_list("tag") as "tags"
      ).select("sid","ids","tags").map(x =>{
      val sid = x.getString(0)
      val ids = x.getList[String](1).asScala.toList
      val tag = x.getList[String](2).asScala.toList
      (sid,strJsonToJson(ids),listToJson(tag))
    }).toDF("sid","ids","tags")
    // 数据传输到ElasticSearch
    saveToES(dataAgg)
  }

  type strList= List[String]
  def strToJson(ids:strList):String = {
    // 构造ids 的Json 结构
     val id = ids.map(x=>{
       val vid = x.split("\\|")
       (vid(0),vid(1))
     }).groupBy(_._2).map(x=>(x._1,x._2.map(_._1)))
    val json = id.map{x =>(
      x._1-> x._2
    )}
    compact(render(json))
  }

  def strJsonToJson(ids:strList):String = {
    // 构造ids 的Json 结构
    val id = ids.map(x=>{
      val vid = jsonToList(x)
      (vid(0),vid(1))
    }).groupBy(_._2).map(x=>(x._1,x._2.map(_._1)))
    val json = id.map{x =>(
      x._1-> x._2
      )}
    compact(render(json))
  }

  def listToJson(l:strList):String = compact(render(l))
  def jsonToList(str:String):strList = {
    implicit val formats = DefaultFormats
    val json = parse(str)
    json.extract[strList]
  }

  def saveToES(df:DataFrame) = {

  }

  case class UserTag(sid:String,id:String,idType:String,tag:String)
}
