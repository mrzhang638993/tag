package cn.itcast.model.ml

import cn.itcast.model.{CommonMeta, Tag}
import cn.itcast.model.utils.MultiSourceModel
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.sql.{DataFrame, Dataset, Row}

object ShoppingGenderModel extends  MultiSourceModel{

  def main(args: Array[String]): Unit = {
      startFlow()
  }
  /**
   * 获取对应的标签名称信息
   **/
  override def tagName(): String = {
     "购物性别"
  }

  /**
   * 处理方法和操作逻辑
   **/
  override def process(df: Array[DataFrame], commonMeta: CommonMeta, fiveTags: Array[Tag]): DataFrame = {
      import spark.implicits._
      import org.apache.spark.sql.functions._
      //  获取到两个数据源，一个是goods表的数据，一个是orders的数据的。
      val  goodsSource=df(0)
      val orderSource=df(1)
      //  通过模拟的时候生成对应的label数据信息
     //   女生对应的数据是1，男生对应的数据是0.
      val label = when('ogcolor.equalTo("樱花粉")
        .or('ogcolor.equalTo("白色"))
        .or('ogcolor.equalTo("香槟色"))
        .or('ogcolor.equalTo("香槟金"))
        .or('producttype.equalTo("料理机"))
        .or('producttype.equalTo("挂烫机"))
        .or('producttype.equalTo("吸尘器/除螨仪")), 1)
        .otherwise(0)
        .alias("gender")
    //  处理数据，生成label信息
    val result: DataFrame = goodsSource.select(label, 'ogcolor as "color", 'cordersn as "ordersn", 'producttype as "producttype")
      .join(orderSource, "ordersn")
      .select('memberid as "id", 'color, 'producttype,'gender)
    //  开始进行模型训练操作和是实现逻辑？
    val   indexer=new StringIndexer()
      .setInputCol("color")
      .setOutputCol("color_index")
    val productIndexer=new StringIndexer()
      .setInputCol("producttype")
      .setOutputCol("product_type_index")
    //  将多个预测要素组装成为一个元素向量信息？
    val  feature=new VectorAssembler()
      .setInputCols(Array("color_index","product_type_index"))
      .setOutputCol("features")
    val featureIndex: VectorIndexer = new VectorIndexer().setInputCol("features")
      .setOutputCol("features_index")
    //  创建决策树对象
    val tree = new DecisionTreeClassifier()
      .setFeaturesCol("features_index")
      .setLabelCol("gender")
      .setPredictionCol("predict")
      .setMaxDepth(5)
      //  设置不存度，对应的体现出来的是划分树的不纯度的操作的。基于gini系数计算信息熵
      .setImpurity("gini")
    //  模型的训练和组装操作
    //  决策依据的优先级，决策的第一个要素是indexer，第二个要素是productIndexer，feature,第四个要素是featureIndex，第五个要素是tree
    val pipeline: Pipeline = new Pipeline().setStages(Array(indexer, productIndexer, feature, featureIndex, tree))
    //  得到model的数据信息？
    // 一般的情况下，会将数据集划分为2个部分，一个部分的数据进行计算，一个部分的数据进行预测操作。
    // 将数据集转化为2个部分的数据的
    //  运用解构赋值操作实现
    val Array(trainDf,testDf): Array[Dataset[Row]] = result.randomSplit(Array(0.8, 0.2))
    // 训练和预测操作.得到模型训练的操作结果信息？
    val model: PipelineModel = pipeline.fit(trainDf)
    //  执行预测操作。根据模型的训练结果进行预测操作
    val predict: DataFrame = model.transform(testDf)
    // 计算标签列和预测列的数据的精确度的数据信息？
   /* val evaluator: MulticlassClassificationEvaluator = new MulticlassClassificationEvaluator()
      .setPredictionCol("predict")
      .setLabelCol("gender")
      .setMetricName("accuracy")*/
    //  根据预测后的数据的精确度操作实现的。计算得到的准确率是1.0的占比的。
    //val d: Double = evaluator.evaluate(predict)
    //  根据用户进行分组操作，求解得到男女偏好的占比，超过8成，则认为是对应的标签信息？
    //  男生的数据对应的是0，女生数据对应的是1.计算总数信息，如果1对应的占比超过总数的80%，那么这个对应的gender是女生的，否则的话，对应的是男生的。
    // predict.groupBy('memberid)
    //println(d)
    // 预测完成之后，对应的需要根据预测的数据判断对应的购物性别的比例数据的。
    null;
  }
}
