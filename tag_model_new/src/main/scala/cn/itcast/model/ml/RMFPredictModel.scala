package cn.itcast.model.ml

import cn.itcast.model.Tag
import cn.itcast.model.utils.BasicModel
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.ml.linalg
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.collection.immutable

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
    import spark.implicits._
    // 获取rmf数据
    val frame: DataFrame = RMFTrainModel.rmfScore(df)
    //  获取打分的数据结果
    val result: DataFrame = RMFTrainModel.assembleFeatures(frame)
    // 对结果进行排序操作。
    val model: KMeansModel = KMeansModel.load(RMFTrainModel.MODEL_PATH)
    //   获取得到预测结果
    val prodicted: DataFrame = model.transform(result)
    val sortedCenters: immutable.IndexedSeq[(Int, Double)] = model.clusterCenters.indices.map(i => (i, model.clusterCenters(i).toArray.sum)).sortBy(_._2).reverse
    //  得到序号的操作，对应的可以得到
    val centerIndex: DataFrame = sortedCenters.indices.map(i => (sortedCenters(i)._1, i + 1)).toDF("predict", "index")
    //  join的时候一个表的数据特别的小的话，会自动的进行join的map端的优化操作的。
    val frame1: DataFrame = prodicted.join(centerIndex, prodicted.col("predict") === centerIndex.col("predict"))
      .select(prodicted.col("id"), centerIndex.col("index") as outFields.head)
    frame1.show()
    frame1
  }
}
