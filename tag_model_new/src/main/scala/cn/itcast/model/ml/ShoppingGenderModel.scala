package cn.itcast.model.ml

import cn.itcast.model.{MultiSourceModel, Tag}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.sql.DataFrame

object ShoppingGenderModel extends MultiSourceModel {

  def main(args: Array[String]): Unit = {
    startFlow()
  }

  override def tagName(): String = "购物性别"

  override def process(sources: Array[DataFrame], fiveTags: Array[Tag], outFields: Array[String]): DataFrame = {
    import org.apache.spark.sql.functions._
    import spark.implicits._

    // 1. 拿到两个 DataFrame source, 第一个是 goods, 第二个是 orders
    val goodsSource = sources(0)
    val ordersSource = sources(1)

    // 2. 通过模拟的形式生成对应的 Label
    val label = when('ogcolor.equalTo("樱花粉")
      .or('ogcolor.equalTo("白色"))
      .or('ogcolor.equalTo("香槟色"))
      .or('ogcolor.equalTo("香槟金"))
      .or('producttype.equalTo("料理机"))
      .or('producttype.equalTo("挂烫机"))
      .or('producttype.equalTo("吸尘器/除螨仪")), 1)
      .otherwise(0)
      .alias("gender")

    // 3. 处理数据生成 Label
    val stage1 = goodsSource.select('cordersn as "ordersn", 'ogcolor as "color", 'producttype as "productType", label)
      .join(ordersSource, "ordersn")
      .select('memberid as "id", 'color, 'productType, 'gender)

    // 4. 特征处理
    val colorIndexer = new StringIndexer()
      .setInputCol("color")
      .setOutputCol("color_index")

    val productIndexer = new StringIndexer()
      .setInputCol("productType")
      .setOutputCol("product_type_index")

    val featureAssembler = new VectorAssembler()
      .setInputCols(Array("color_index", "product_type_index"))
      .setOutputCol("features")

    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("features_index")

    val decisionTree = new DecisionTreeClassifier()
      .setFeaturesCol("features_index")
      .setLabelCol("gender")
      .setPredictionCol("predict")
      .setMaxDepth(5)
      .setImpurity("gini")

    // 5. 模型训练和组装
    val pipeline = new Pipeline()
      .setStages(Array(colorIndexer, productIndexer, featureAssembler, featureIndexer, decisionTree))

    // 有监督学习的时候, 会把数据集划分为两个部分, 一个作为训练, 一个作为预测
    val Array(trainDF, testDF) = stage1.randomSplit(Array(0.8, 0.2))

    val model = pipeline.fit(trainDF)
    val result = model.transform(testDF)
    result.show()

    // 6. 预测

    // 7. 计算精确率
    val accEvaluator = new MulticlassClassificationEvaluator()
      .setPredictionCol("predict")
      .setLabelCol("gender")
      .setMetricName("accuracy")

    val acc = accEvaluator.evaluate(result)

    println(acc)

    // 8. 标签设置, 按照用户进行分组, 求得男女偏好的占比, 超过8成, 则认为是对应的标签

    // 总结
    // 1. 算法的重要程度, 重要, 未来的发展方向
    // 2. 算法和大家的关系, 掌握到一定程度就可以了
    // 3. 整体流程, 为什么使用Hbase, 运行Spark程序的时候资源使用

    null
  }
}
