package cn.itcast.model.mtag

import java.util.Properties
import cn.itcast.model.{HBaseCataLog1, HBaseColumn1, HBaseTable1, HbaseMeta, HdfsMeta, MetaData, Tag}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import scala.collection.mutable

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
    val df: DataFrame = createSource(data)
  }

  /**
   * 接受源数据对象信息：metaData数据信息
   * */
  def createSource(metaData: MetaData):DataFrame={
      if(metaData.isHbase()){
        val meta: HbaseMeta = metaData.toHbaseMeta()
         // 创建catalog对象
         // 处理catalog对象
         // catalog对象转换成为json对象的。
        val hbase1=HBaseTable1("","");
        val rowkey=""
        val columns=mutable.HashMap.empty[String,HBaseColumn1]
        // 需要根据属性名称获取对应的map数据类型和操作逻辑
        //  指定rowkey对应的字段信息？
        columns += HBASE_ROWKEY_FIELD->HBaseColumn1("rowkey",HBASE_ROWKEY_FIELD,HBASE_COLUMN_DEFAULT_TYPE)
        //  根据源数据中的columns执行操作实现？
        for(filed<-meta.commonMeta.inFields){
          columns += filed->HBaseColumn1(meta.columnFamily,filed,HBASE_COLUMN_DEFAULT_TYPE)
        }
        val hbaseCatalog=HBaseCataLog1(hbase1,rowkey,columns.toMap)
        import org.json4s._
        //  序列化的操作实现
        import org.json4s.jackson.Serialization
        //  导入工具方法
        import org.json4s.jackson.Serialization.write
        //导入隐式转换操作的方法和实现逻辑。需要格式转换操作和实现的
        implicit  val formats=Serialization.formats(NoTypeHints)
        //  执行隐式转换的操作实现和逻辑
        val catalogJson: String = write(hbaseCatalog)
        val df: DataFrame = spark.read.option(HBaseTableCatalog.tableCatalog, catalogJson)
          .format("org.apache.spark.sql.execution.datasources.hbase")
          .load()
        df
      }else if(metaData.isHdfs()){
        val meta: HdfsMeta = metaData.toHdfsMeta()
        //  hdfs的连接操作
        val df: DataFrame = spark.read.option("seperator", meta.separator).load(meta.inPath)
        // 输出字段的显示操作
        import org.apache.spark.sql.functions._
        /*var columns=new Array[Column](meta.commonMeta.inFields.size)
        for(field<-meta.commonMeta.inFields){
          columns += col(field)
        }
        df.select(columns)*/
        null;
        // 读取hdfs处理数据
      }else{
         // 读取源数据的信息执行操作。mysql类型的。所有的关系型的数据库
         //  真实的情况下是不会使用mysql执行操作的。
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
      val fourTag: Tag = source.where('name === TAG_NAME).as[Tag].collect().head
      //  得到多个五级标签的信息？
      val fiveTags: Array[Tag] = source.where('pid === fourTag.id).as[Tag].collect()
      //  得到五级标签的数据信息？
      //  使用元祖模拟返回多个数值信息
    (fourTag,fiveTags)
  }
}
