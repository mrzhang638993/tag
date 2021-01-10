package cn.itcast.model.ml

import cn.itcast.model.Tag
import cn.itcast.model.utils.BasicModel
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.sql.DataFrame

/**
 * 使用预测模型的数据执行模型的预测操作
 * */
object RMFPredictModel  extends  BasicModel{

  def main(args: Array[String]): Unit = {
      startFlow()
  }
  /**
   * 获取对应的标签名称信息
   **/
  override def tagName(): String = {
      "客户价值"
  }

  /**
   * 处理方法和操作逻辑
   **/
  override def process(df: DataFrame, fiveTags: Array[Tag], outFields: Array[String]): DataFrame = {
    // 获取rmf数据
    val frame: DataFrame = RMFTrainModel.rmfScore(df)
    //  获取打分的数据结果
    val result: DataFrame = RMFTrainModel.assembleFeatures(frame)
    // 对结果进行排序操作。
    //  读取模型数据
    val model: KMeansModel = KMeansModel.load(RMFTrainModel.MODEL_PATH)
    //   获取得到预测结果
    val prodicted: DataFrame = model.transform(result)
    //  执行输出结果的排序操作和实现？
    prodicted.show()
    null
  }
}
