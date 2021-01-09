package cn.itcast.model.stats

import cn.itcast.model.Tag
import cn.itcast.model.utils.BasicModel
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}
import org.apache.spark.sql.types.{LongType, StringType}

/**
 * 统计消费者的消费周期数据信息
 * */
object BuyCycleModel  extends BasicModel{

  def main(args: Array[String]): Unit = {
      startFlow()
  }
  /**
   * 获取对应的标签名称信息
   **/
  override def tagName(): String = {
     "消费周期"
  }

  /**
   * 处理方法和操作逻辑
   * 将时间戳映射为时间信息？
   **/
  override def process(df: DataFrame, fiveTags: Array[Tag], outFields: Array[String]): DataFrame = {
    //  导入依赖数据信息
     import  spark.implicits._
     import  org.apache.spark.sql.functions._

    df.show()
    // 只能怪数据过滤操作实现
    val cleanDf: Dataset[Row] = df.filter($"finishTime".isNotNull && $"memberid".isNotNull)
    cleanDf.show()
    // 统计用户的消费周期数据信息？
    val frame: DataFrame = cleanDf.select('memberid, 'finishTime.cast(LongType).as("finishTime"))
      .groupBy('memberid)
      .agg(max('finishTime) as "finishTime")
      .select('memberid as "id",datediff(current_timestamp(),'finishTime.cast(StringType)) as "finishTime")
    frame.show()
    //   专户为距离今天多少天的时间信息的
    var  conditions:Column=null
    // 执行打标签的数据操作实现.根据时间范围对应的执行相关的操作处理和实现逻辑体现的。
    for(tag<-fiveTags){
      val start: String = tag.rule.split("-")(0)
      val end: String = tag.rule.split("-")(1)
      conditions=if(conditions==null){
          when('finishTime between(start,end),tag.id)
       }else{
         conditions.when('finishTime between(start,end),tag.id)
       }
    }
    conditions=conditions.as(outFields.head)
    frame.select('id,conditions)
  }
}
