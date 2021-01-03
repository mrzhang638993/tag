package cn.itcast.model.mtag

import java.util.Properties

import cn.itcast.model.utils.ShcUtils
import cn.itcast.model.{CommonMeta, HbaseMeta, HdfsMeta, MetaData, Tag}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{Column, DataFrame, SparkSession}


object GenderModel {

  val config=ConfigFactory.load()
  val spark=SparkSession.builder()
    .appName("gender mode")
    .master("local[6]")
    .getOrCreate()
  val TAG_NAME="性别"
  val HBASE_NAMESPACE="default"
  val HBASE_ROWKEY_FIELD="id"
  val HBASE_COLUMN_DEFAULT_TYPE="string"
  val HBASE_USER_PROFILE="user_profile_new_1"

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
    // 读取元数据信息
    val (df,commonMeta) = createSource(data)
    //  开始计算标签信息。将标签数据和五级标签进行匹配操作。
    val result: DataFrame = process(df, fivetags, commonMeta.outFields)
    // 数据存储到hbase中进行管理和实现操作
    //println(data.toHbaseMeta().commonMeta)
    saveUserProfile(result,commonMeta)
  }

  /**
   * 保存数据到hbase中进行操作实现
   * */
  def saveUserProfile(result:DataFrame,commonMeta: CommonMeta): Unit ={
      ShcUtils.write(HBASE_USER_PROFILE,commonMeta.outFields,result,"5")
  }

  /**
   * 匹配计算标签进行操作。
   * df:元数据集
   * fivetags：用于计算的五级标签的数据
   * outFields：输出的五级标签的字段信息
   * */
  def process(df:DataFrame,fivetags:Array[Tag],outFields:Array[String]):DataFrame ={
       // 过滤规则，匹配规则。用户的gender为1的话，代表的是男，打上50的标签。gender为2的话，代表的是女，对应的规则是51
       import  spark.implicits._
       import org.apache.spark.sql.functions._
       //根据五级标签进行遍历操作
       var  conditions:Column=null
       //  拼接判断条件
       for(tag<-fivetags){
         conditions= if(conditions==null)
            //执行条件判断操作和实现
            when('gender===tag.rule,tag.id)
          else
            conditions.when('gender===tag.rule,tag.id)
       }
       conditions=conditions.as("gender")
       df.select('id,conditions)
  }

  /**
   * 接受源数据对象信息：metaData数据信息
   * */
  def createSource(metaData: MetaData):(DataFrame,CommonMeta)={
      if(metaData.isHbase()){
        val meta: HbaseMeta = metaData.toHbaseMeta()
        val df: DataFrame = ShcUtils.read(meta.commonMeta.inFields, meta.columnFamily, meta.tableName, spark)
        (df,meta.commonMeta)
      }else if(metaData.isHdfs()){
        val meta: HdfsMeta = metaData.toHdfsMeta()
        val df: DataFrame = ShcUtils.readHdfs(metaData, spark)
        (df,meta.commonMeta)
        }else{
        null
      }
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
      //  === 对应的是col对象的方法。
      val fourTag: Tag = source.where('name === TAG_NAME).as[Tag].collect().head
      //  得到多个五级标签的信息？
      val fiveTags: Array[Tag] = source.where('pid === fourTag.id).as[Tag].collect()
      //  得到五级标签的数据信息？
      //  使用元祖模拟返回多个数值信息
    (fourTag,fiveTags)
  }
}
