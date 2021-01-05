package cn.itcast.model.utils

import java.util.Properties

import cn.itcast.model.{CommonMeta, HbaseMeta, HdfsMeta, MetaData, Tag}
import cn.itcast.model.mtag.JobModel.spark
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.DataFrame

class BasicModel {


  /**
   * 创建datasource数据集数据，执行数据集操作实现
   * */
  def getDataSource(metaData:MetaData): (DataFrame, CommonMeta) ={
    if(metaData.isHdfs()){
      // 执行hdfs的配置操作和实现
      val meta: HdfsMeta = metaData.toHdfsMeta()
      val df: DataFrame = ShcUtils.readHdfs(metaData, spark)
      (df,meta.commonMeta)
    }else if(metaData.isHbase()){
      val meta: HbaseMeta = metaData.toHbaseMeta()
      val df: DataFrame = ShcUtils.read(meta.commonMeta.inFields, meta.columnFamily, meta.tableName, spark)
      (df,meta.commonMeta)
    }else{
      (null,null)
    }
  }

  /**
   * 获取数据的元数据信息
   * */
  def  getMetaData(fourTag:Tag): MetaData ={
    val config: Config = ConfigFactory.load()
    val metaUrl: String = config.getString("jdbc.meta_data.url")
    val table: String = config.getString("jdbc.meta_data.table")
    val df: DataFrame = spark.read.jdbc(metaUrl, table, new Properties())
    import spark.implicits._
    df.where('tag_id===fourTag.id).as[MetaData].collect().head
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
