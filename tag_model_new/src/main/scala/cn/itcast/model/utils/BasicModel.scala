package cn.itcast.model.utils

import java.util.Properties

import cn.itcast.model.mtag.GenderModel.HBASE_USER_PROFILE
import cn.itcast.model.{CommonMeta, HbaseMeta, HdfsMeta, MetaData, Tag}
import cn.itcast.model.mtag.JobModel.{TAG_NAME, getDataSource, getMetaData, process, readBasicTag, saveUserProfile, spark}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 构建父类的操作信息？
 * */
trait BasicModel {

  val spark: SparkSession = SparkSession.builder()
    .appName("职业")
    .master("local[6]")
    .getOrCreate()

  /**
   * 获取对应的标签名称信息
   * */
  def tagName():String

  /**
   * 处理方法和操作逻辑
   * */
  def process(df:DataFrame,fiveTags:Array[Tag],outFields:Array[String]):DataFrame

  /**
   * 流程执行的方法
   * */
  def startFlow(): Unit ={
    //  访问mysql的数据库，获取4及标签以及5级标签的数据
    //  采用结构赋值操作
    val (fourTag,fiveTags)=readBasicTag(tagName)
    //  根据4级标签的数据获取对应的元数据信息。
    val meta: MetaData = getMetaData(fourTag)
    //  读取数据，根据规则匹配5级标签,计算得到结果
    val  (df,commonMeta): (DataFrame,CommonMeta) = getDataSource(meta)
    //  计算标签执行数据操作实现。
    val result: DataFrame = process(df, fiveTags, commonMeta.outFields)
    //  输出结果输出到hbase里面。
    saveUserProfile(result,commonMeta)
  }

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

  /**
   * 保存数据到hbase中进行操作实现
   * */
  def saveUserProfile(result:DataFrame,commonMeta: CommonMeta): Unit ={
    ShcUtils.writeHbase(HBASE_USER_PROFILE,commonMeta.outFields,result,"5")
  }
}
