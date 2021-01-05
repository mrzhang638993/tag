package cn.itcast.model.mtag

import java.util.Properties

import cn.itcast.model.{CommonMeta, HbaseMeta, HdfsMeta, MetaData, Tag}
import cn.itcast.model.mtag.GenderModel.spark
import cn.itcast.model.utils.{BasicModel, ShcUtils}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

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
    val  (df,commonMeta): (DataFrame,CommonMeta) = getDataSource(meta)
    println(fiveTags)
    //  计算标签执行数据操作实现。
    //  输出结果输出到hbase里面。
  }

  /**
   * 执行数据判断和逻辑操作执行
   * 对应的是匹配性的类型的数据的计算的。根据匹配数据执行逻辑计算操作
   * */
  def   process(df:DataFrame,fiveTags:Array[Tag],outFields:Array[String]): Unit ={
    import  spark.implicits._
    import org.apache.spark.sql.functions._
    // 构建查询条件和操作实现管理体现？
    var conditions:Column=null
    for(tag<-fiveTags){

    }
  }
}
