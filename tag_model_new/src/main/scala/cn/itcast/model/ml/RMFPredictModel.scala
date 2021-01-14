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
    // 下面预测的数据是错误的，需要重新编写代码进行预测操作实现？
    // 步骤一：针对于fivetags的数据需要和centerIndex进行关联，得到关联关系管理.需要处理和转换一下rule以及对应的关联关系
    // val ruleTag: DataFrame = centerIndex.join(five, "rule")
    // val perdict: DataFrame = ruleTag.select(predictStr, "tagsId")  得到预测值和tagsId之间的关联关系？
    // 下面得到预测值和映射关系之间的关联map数据信息？
    //val perMap = perdict.map(t => {
    // val pre = t.getAs(predictStr).toString
    //   val tag = t.getAs("tagsId").toString
    //   (pre, tag)
   // }).collect().toMap
    //  根据对应的预测数值获取到对应的tagId的数据信息？
    //var predictUdf=udf((perdict:String)=>{
    //  var tag=perMap(perdict)
    // tag
    //})
    val new_Tag = prodicted.select('memberId as "userId", predictUdf('predict) as "tagsId")
    //  join的时候一个表的数据特别的小的话，会自动的进行join的map端的优化操作的。
    //val frame1: DataFrame = prodicted.join(centerIndex, prodicted.col("predict") === centerIndex.col("predict"))
     // .select(prodicted.col("id"), centerIndex.col("index") as outFields.head)
    //frame1.show()
    //frame1
  }
}
