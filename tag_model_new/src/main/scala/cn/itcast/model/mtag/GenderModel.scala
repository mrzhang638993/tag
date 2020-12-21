package cn.itcast.model.mtag

import java.util.Properties

import cn.itcast.model.{MetaData, Tag}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object GenderModel {

  val  config=ConfigFactory.load()
  val spark=SparkSession.builder()
    .appName("gender mode")
    .master("local[6]")
    .getOrCreate()
  val TAG_NAME="性别"

  def main(args: Array[String]): Unit = {
    //  读取mysql中的4级和5级标签数据
    //  通过4级标签处理元数据
    //  处理元数据，处理成为结构化的方式
    //  使用元数据，连接原表的数据，匹配计算标签的数据。
    // 将标签汇总，放入用户画像表数据？
    //  结构赋值操作实现？
    val (fourtag,fivetags)=readBasicTag()
    // 处理元数据数据，获取的是4级标签的数据执行操作实现。
    val data: MetaData = readMetaData(fourtag.id)
    println(data)
  }

  /**
   * 读取源数据信息
   * */
  def  readMetaData(fourtagId:String):MetaData={
      import  spark.implicits._
      import org.apache.spark.sql.functions._
      //  读取元数据信息。元数据表的配置信息，解析元数据，以对象的方式返回数据信息？
      val  url=config.getString("jdbc.meta_data.url")
      val table: String = config.getString("jdbc.meta_data.table")
      val matchColumn=config.getString("jdbc.meta_data.match_column")
      // 读取元数据信息,将配置解析成为样例类对象的数据执行返回操作实现。
      val head: MetaData = spark.read.jdbc(url, table, new Properties())
        .where(col(matchColumn) === fourtagId).as[MetaData].collect().head
      head
  }
  /**
   * 读取basicTag的基础数据。
   * */
  def readBasicTag():(Tag,Array[Tag])={
      // 读取和读取配置文件：application.conf
      // 创建sparkSession数据信息？读取四级标签
      val url: String = config.getString("jdbc.basic_tag.url")
      val table: String = config.getString("jdbc.basic_tag.table")
      // 使用4级标签,对应的读取5级标签的配置操作。
      // 将mysql的数据表读成了frame执行操作的。
      val source: DataFrame = spark.read.jdbc(url, table, new Properties())
      // 读取一个四级标签的数据信息的，只需要一个标签的数据的。通过name执行筛选操作的
      //  使用column对象执行操作
      import spark.implicits._
      //  scala的相等操作.得到4级标签的数据信息
      val fourTag: Tag = source.where('name === TAG_NAME).as[Tag].collect().head
      //  得到多个五级标签的信息？
      val fiveTags: Array[Tag] = source.where('pid === fourTag.id).as[Tag].collect()
      //  得到五级标签的数据信息？
      //  使用元祖模拟返回多个数值信息
    (fourTag,fiveTags)
  }
}
