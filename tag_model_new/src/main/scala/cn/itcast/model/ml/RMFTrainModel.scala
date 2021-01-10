package cn.itcast.model.ml

import cn.itcast.model.Tag
import cn.itcast.model.utils.BasicModel
import org.apache.spark.sql.{DataFrame, Dataset, Row}

/**
 * RMFTrainModel:训练模型操作
 * */
object RMFTrainModel extends  BasicModel{

  def main(args: Array[String]): Unit = {
    startFlow()
  }
  /**
   * 获取对应的标签名称信息
   **/
  override def tagName(): String = "客户价值"

  /**
   * 处理方法和操作逻辑
   **/
  override def process(df: DataFrame, fiveTags: Array[Tag], outFields: Array[String]): DataFrame = {
    // 获取rmf的数据信息？
    import spark.implicits._
    df.show()
    val cleanValue: Dataset[Row] = df.filter($"finishtime".isNotNull)
    //cleanValue.show()
    val frame: DataFrame = rmfScore(cleanValue)
    frame.show(20)
    null
  }

  /**
   * 求解RMFScore的数据
   * */
  def  rmfScore(dataFrame: DataFrame):DataFrame={
     // 1.导入依赖
     //  求解得到每一个的RMF的score机制的。
     import  spark.implicits._
     import  org.apache.spark.sql.functions._
     //  求解R指标的数据信息.求解最后一次消费距离今天的时间。
    val rCol=(datediff(date_sub(current_timestamp(),503),from_unixtime(max('finishTime)))) as "r"
    //  消费频次
    val fCol=count('ordersn ) as "f"
    // 计算得到消费的总金额
    val mCol=sum('orderamount) as "m"
    dataFrame.show()
    dataFrame.groupBy('memberid).agg(rCol,fCol,mCol)
  }

  /**
   * 特征处理操作.数据只是包含一列数据。执行模型的训练操作
   * */
  def  assembleFeatures(df:DataFrame): DataFrame ={
      null
  }

  /**
   * 进行模型的训练操作.执行模型的训练操作和实现
   * */
  def  train()={

  }


}
