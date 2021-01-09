package cn.itcast.model.stats

import cn.itcast.model.Tag
import cn.itcast.model.utils.BasicModel
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}
import org.apache.spark.sql.expressions.Window

/**
 * 统计最近一次的支付方式
 * */
object RecentPaymentCodeModel  extends  BasicModel{

  def main(args: Array[String]): Unit = {
       startFlow()
  }
  /**
   * 获取对应的标签名称信息
   **/
  override def tagName(): String = "支付方式"

  /**
   * 处理方法和操作逻辑
   **/
  override def process(df: DataFrame, fiveTags: Array[Tag], outFields: Array[String]): DataFrame = {
       //  导入spark的相关的依赖
    import spark.implicits._
    import org.apache.spark.sql.functions._
      //  找到最近的一次的支付方式.获取时间信息
    val dfClean: Dataset[Row] = df.filter($"paymentcode".isNotNull && length(trim($"paymentcode")) > 0 && $"paytime".=!=("0"))
      .filter($"paymentcode".isInCollection(List("alipay",
      "cod","chinapay")))
    val destValue: Dataset[Row] = dfClean.select('memberid as "id",
        row_number() over Window.partitionBy('memberid).orderBy('paytime.desc) as "rn",
        'paymentcode as "paymentcode"
    ).where('rn === 1)
    // 根据条件执行规则匹配操作
    var conditions:Column=null;
    for(tag<-fiveTags){
      conditions=if(conditions==null){
        when('paymentcode===tag.rule,tag.id)
      }else{
        conditions.when('paymentcode===tag.rule,tag.id)
      }
    }
    conditions=conditions.as(outFields.head)
    val frame: DataFrame = destValue.select('id, conditions)
    frame.show()
    frame
  }
}
