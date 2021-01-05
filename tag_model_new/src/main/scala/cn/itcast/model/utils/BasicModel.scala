package cn.itcast.model.utils

import java.util.Properties

import cn.itcast.model.{MetaData, Tag}
import cn.itcast.model.mtag.JobModel.spark
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.DataFrame

class BasicModel {


  /**
   * 获取数据的元数据信息
   * */
  def  getMetaData(fourTag:Tag): MetaData ={
    val config: Config = ConfigFactory.load()
    val metaUrl: String = config.getString("jdbc.meta_data.url")
    val table: String = config.getString("jdbc.meta_data.table")
    val df: DataFrame = spark.read.jdbc(metaUrl, table, new Properties())
    import spark.implicits._
    df.select('tag_id===fourTag.id).as[MetaData].collect().head
  }
  /**
   * 读取标签的数据。获取到4级以及5级标签的数据
   * */
  def  readBasicTag(tagName:String):(Tag,Array[Tag]) ={
    // 读取数据信息？
    // 读取mysql的数据
    val config: Config = ConfigFactory.load()
    // 获取url以及table的信息？
    val url: String = config.getString("jdbc.basic_tag.url")
    val table: String = config.getString("jdbc.basic_tag.table")
    val df: DataFrame = spark.read.jdbc(url, table, new Properties())
    import spark.implicits._
    val fourTag: Tag = df.where('name === tagName).as[Tag].head()
    val fiveTags: Array[Tag] = df.where('pid === fourTag.id).as[Tag].collect()
    (fourTag,fiveTags)
  }
}
