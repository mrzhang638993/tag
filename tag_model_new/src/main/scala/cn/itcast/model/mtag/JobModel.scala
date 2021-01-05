package cn.itcast.model.mtag

import java.util.Properties

import cn.itcast.model.{CommonMeta, HbaseMeta, HdfsMeta, MetaData, Tag}
import cn.itcast.model.mtag.GenderModel.spark
import cn.itcast.model.utils.{BasicModel, ShcUtils}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 职业标签的书写逻辑操作
 * */
object JobModel  extends  BasicModel{

  val  TAG_NAME="职业"
  val spark: SparkSession = SparkSession.builder()
    .appName("职业")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    //  访问mysql的数据库，获取4及标签以及5级标签的数据
    //  采用结构赋值操作
    var (fourTag,fiveTags)=readBasicTag(TAG_NAME)
    //  根据4级标签的数据获取对应的元数据信息。
    val meta: MetaData = getMetaData(fourTag)
    //  读取数据，根据规则匹配5级标签,计算得到结果
    val source: (DataFrame,CommonMeta) = getDataSource(meta)
    //  输出结果输出到hbase里面。
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
       null
     }
  }
}
