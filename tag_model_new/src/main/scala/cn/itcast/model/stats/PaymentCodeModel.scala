package cn.itcast.model.stats

import cn.itcast.model.Tag
import cn.itcast.model.utils.BasicModel
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}
import org.apache.spark.sql.expressions.Window
/**
 * 计算支付方式
 * */
object PaymentCodeModel extends  BasicModel{

  def main(args: Array[String]): Unit = {
      startFlow()
  }
  /**
   * 获取对应的标签名称信息
   **/
  override def tagName(): String = {
     "支付方式"
  }

  /**
   * 处理方法和操作逻辑
   **/
  override def process(df: DataFrame, fiveTags: Array[Tag], outFields: Array[String]): DataFrame = {
     // 导入依赖
    import spark.implicits._
    import org.apache.spark.sql.functions._
    //  group by的本质：
    val result: DataFrame =
    df.groupBy('memberid, 'paymentcode).
      agg(count('paymentcode) as "count")
        .sort('memberid)
      // 解决方案：1.按照memberid进行分区操作。2.按照count列进行排序操作
      // 最好的实现方法，对应的可以使用窗口函数执行操作。
      //  flink的窗口对应的是时间上的窗口执行的操作的。
    /*val value: Dataset[Row] = result
        .select('memberid, 'paymentcode, row_number() over Window.partitionBy('memberid).orderBy('count desc) as "rn")
      .where('rn === 1)
    value.show()*/
    //  方式之二：使用withcolumn实现操作？多增加一列rn。withcolumns字段进行字段处理实现。
    val value: DataFrame = result.withColumn("rn", row_number() over Window.partitionBy('memberid).orderBy('count desc))
          .where('rn===1)
     // 获取最多的支付方式，打上标签进行处理
    var  conditions:Column=null
    for(tag<-fiveTags){
       // 数据的处理逻辑和实现？
      conditions=if(conditions==null){
        when('paymentcode===tag.rule,tag.id)
      }else{
        conditions.when('paymentcode===tag.rule,tag.id)
      }
    }
    conditions=conditions.as(outFields.head)
    value.select('memberid as "id",conditions)
  }
}
