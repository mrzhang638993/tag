package cn.itcast.model.ml

import cn.itcast.model.{CommonMeta, Tag}
import cn.itcast.model.utils.MultiSourceModel
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler, VectorIndexer, VectorIndexerModel}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Dataset, Row}

object ShoppingGenderModel extends  MultiSourceModel{

  /**
   * 根据已有标签的数据，加入更多的数据来完善决策树的生成的，最终形成一个最终可靠的预测决策树的模型的。
   * */
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
      //  获取到两个数据源，一个是goods表的数据，一个是orders的数据的。这个只能是原始的数据的。
      //  最终训练得到的数据应该是对于用户的数据进行标注的，所以，对应的用户的数据需要加入到这里面的。
      val  goodsSource=df(0)
      val  orderSource=df(1)
      // 增加定义用户的数据信息，来决定对应的用户信息，这里面假设用户信息是存在的。
      val  userSource=df(2)
      //  通过模拟的时候生成对应的label数据信息
     //   女生对应的数据是1，男生对应的数据是0.通过这些分析出来的数据比较有代表的可以预测出来性别信息。
     // 需要将原始的数据进行数字化的处理操作的。
     //颜色ID应该来源于字典表,这里简化处理
     val color: Column = functions
       .when('ogColor.equalTo("银色"), 1)
       .when('ogColor.equalTo("香槟金色"), 2)
       .when('ogColor.equalTo("黑色"), 3)
       .when('ogColor.equalTo("白色"), 4)
       .when('ogColor.equalTo("梦境极光【卡其金】"), 5)
       .when('ogColor.equalTo("梦境极光【布朗灰】"), 6)
       .when('ogColor.equalTo("粉色"), 7)
       .when('ogColor.equalTo("金属灰"), 8)
       .when('ogColor.equalTo("金色"), 9)
       .when('ogColor.equalTo("乐享金"), 10)
       .when('ogColor.equalTo("布鲁钢"), 11)
       .when('ogColor.equalTo("月光银"), 12)
       .when('ogColor.equalTo("时尚光谱【浅金棕】"), 13)
       .when('ogColor.equalTo("香槟色"), 14)
       .when('ogColor.equalTo("香槟金"), 15)
       .when('ogColor.equalTo("灰色"), 16)
       .when('ogColor.equalTo("樱花粉"), 17)
       .when('ogColor.equalTo("蓝色"), 18)
       .when('ogColor.equalTo("金属银"), 19)
       .when('ogColor.equalTo("玫瑰金"), 20)
       .otherwise(0)
       .alias("color")

    val productType: Column = functions
      .when('productType.equalTo("4K电视"), 9)
      .when('productType.equalTo("Haier/海尔冰箱"), 10)
      .when('productType.equalTo("Haier/海尔冰箱"), 11)
      .when('productType.equalTo("LED电视"), 12)
      .when('productType.equalTo("Leader/统帅冰箱"), 13)
      .when('productType.equalTo("冰吧"), 14)
      .when('productType.equalTo("冷柜"), 15)
      .when('productType.equalTo("净水机"), 16)
      .when('productType.equalTo("前置过滤器"), 17)
      .when('productType.equalTo("取暖电器"), 18)
      .when('productType.equalTo("吸尘器/除螨仪"), 19)
      .when('productType.equalTo("嵌入式厨电"), 20)
      .when('productType.equalTo("微波炉"), 21)
      .when('productType.equalTo("挂烫机"), 22)
      .when('productType.equalTo("料理机"), 23)
      .when('productType.equalTo("智能电视"), 24)
      .when('productType.equalTo("波轮洗衣机"), 25)
      .when('productType.equalTo("滤芯"), 26)
      .when('productType.equalTo("烟灶套系"), 27)
      .when('productType.equalTo("烤箱"), 28)
      .when('productType.equalTo("燃气灶"), 29)
      .when('productType.equalTo("燃气热水器"), 30)
      .when('productType.equalTo("电水壶/热水瓶"), 31)
      .when('productType.equalTo("电热水器"), 32)
      .when('productType.equalTo("电磁炉"), 33)
      .when('productType.equalTo("电风扇"), 34)
      .when('productType.equalTo("电饭煲"), 35)
      .when('productType.equalTo("破壁机"), 36)
      .when('productType.equalTo("空气净化器"), 37)
      .otherwise(0)
      .alias("productType")
    //   这些数据是已经标记的数据的，已经标记来可以识别性别的标记信息的。
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
    //  决策树对应的是有监督的学习，所有需要增加标签信息，可以和后面预测的数据进行比对的。
    /*val   indexer=new StringIndexer()
      .setInputCol("color")
      .setOutputCol("color_index")*/
    //  增加产品类型的标签信息
    /*val productIndexer=new StringIndexer()
      .setInputCol("producttype")
      .setOutputCol("product_type_index")*/
    // 创建原有的标签的元数据信息
    val stringIndexModel=new StringIndexer()
      .setInputCol("gender")
      .setOutputCol("label")
      .fit(result)
    //  将多个预测要素组装成为一个元素向量信息？组合成为需要预测的向量信息，是模型真正需要输入的数据的
    //  将两个维度的数据分装形成对应的维度数据信息？使用color以及producttype字段进行向量的生成。
    val  vectorAssembler =new VectorAssembler()
      .setInputCols(Array("color","producttype"))
      .setOutputCol("features")
    val featuresDf: DataFrame = vectorAssembler.transform(result)
    //  对特征进行索引，后续的查找的过程会更加的高效的.提高决策树或随机森林等ML方法的分类效果
    ////对特征进行索引,大于3个不同的值的特征被视为连续特征
    //    //VectorIndexer是对数据集特征向量中的类别(离散值)特征(index categorical features categorical features)进行编号。
    //    //它能够自动判断那些特征是离散值型的特征，并对他们进行编号，具体做法是通过设置一个maxCategories，
    //    //特征向量中某一个特征不重复取值个数小于maxCategories，则被重新编号为0～K（K<=maxCategories-1）。
    //    //某一个特征不重复取值个数大于maxCategories，则该特征视为连续值，不会重新编号（不会发生任何改变）
    //  其中maxCategories的数据是怎么计算出来的。存在什么比较好的规律处理对应的理论和操作方法？
    val featureIndex: VectorIndexerModel = new VectorIndexer().setInputCol("features")
      .setOutputCol("features_index")
      .setMaxCategories(3)
      .fit(featuresDf)
    //  创建决策树对象
    val tree = new DecisionTreeClassifier()
      .setFeaturesCol("features_index")
      .setPredictionCol("predict") //Gini不纯度
      .setMaxDepth(5) //树的最大深度
      .setMaxBins(5) //离散化连续特征的最大划分数
      //  设置不存度，对应的体现出来的是划分树的不纯度的操作的。基于gini系数计算信息熵
      .setImpurity("gini")
    //  设置label的还原操作实现
    val labelConverter: IndexToString = new IndexToString()
      .setInputCol("label")
      .setOutputCol("labelConverted")
      .setLabels(stringIndexModel.labels)
    //  模型的训练和组装操作
    //  决策依据的优先级，决策的第一个要素是indexer，第二个要素是productIndexer，feature,第四个要素是featureIndex，第五个要素是tree
    val pipeline: Pipeline = new Pipeline().setStages(Array(stringIndexModel, vectorAssembler, featureIndex, tree,labelConverter))
    //  得到model的数据信息？
    // 一般的情况下，会将数据集划分为2个部分，一个部分的数据进行计算，一个部分的数据进行预测操作。
    // 将数据集转化为2个部分的数据的
    //  运用解构赋值操作实现
    val Array(trainDf,testDf): Array[Dataset[Row]] = result.randomSplit(Array(0.8, 0.2))
    // 训练和预测操作.得到模型训练的操作结果信息？
    val model: PipelineModel = pipeline.fit(trainDf)
    //  执行预测操作。根据模型的训练结果进行预测操作
    val predict: DataFrame = model.transform(testDf)
    val predictTestDF = model.transform(trainDf)
    evaluateAUC(predict,predictTestDF)
    // 计算标签列和预测列的数据的精确度的数据信息？
   /* val evaluator: MulticlassClassificationEvaluator = new MulticlassClassificationEvaluator()
      .setPredictionCol("predict")
      .setLabelCol("gender")
      .setMetricName("accuracy")
    //  根据预测后的数据的精确度操作实现的。计算得到的准确率是1.0的占比的。
    val d: Double = evaluator.evaluate(predict)*/
    //  根据用户进行分组操作，求解得到男女偏好的占比，超过8成，则认为是对应的标签信息？
    //  男生的数据对应的是0，女生数据对应的是1.计算总数信息，如果1对应的占比超过总数的80%，那么这个对应的gender是女生的，否则的话，对应的是男生的。
    // predict.groupBy('memberid)
    //println(d)
    // 预测完成之后，对应的需要根据预测的数据判断对应的购物性别的比例数据的。
    //  查看决策树模型
   val treeClassificationModel = model.stages(3).asInstanceOf[DecisionTreeClassificationModel]
    println("Learned classification tree model:\n" + treeClassificationModel.toDebugString)
    val union_predict = predictTestDF.union(predictTestDF)
    var male=when(col("prediction")===0,1).otherwise(0).as("male")
    var female= when(col("prediction")===1,1).otherwise(0).as("female")
    var totalCountSex=count('userId) cast  DoubleType as "total"
    var maleSum=sum('male) cast DoubleType as "maleSum"
    var femaleSum=sum('female) cast DoubleType as "femaleSum"
    val destResult = union_predict.select('userId, male,female)
      .groupBy('userId)
      .agg(totalCountSex , maleSum , femaleSum  )
    val five_Map = fiveTags.toSeq.toDF("tagsId", "name", "rule", "pid").map(row => {
      (row.getAs("rule").toString, row.getAs("tagsId").toString)
    }).collect().toMap
    //预测规则B:计算每个用户近半年内所有订单中的男性商品超过60%则认定该用户为男，或近半年内所有订单中的女性品超过60%则认定该用户为女
    var getGender=udf((total:Double,mSum:Double,fsum:Double)=>{
      val mRatio = mSum/total
      val fRatio = fsum/total
      if (mRatio>=0.8){
        five_Map("0")
      }
      if(fRatio>=0.8){
        five_Map("1")
      }
      five_Map("1")
    })
    val new_Tag = destResult.select('userID, getGender('total, 'maleSum, 'femaleSum) as "tagsId")
    new_Tag
    null;
  }

  /**
   * @param predictTestDF
   * @param predictTrainDF
   */
  def evaluateAUC(predictTrainDF: DataFrame,predictTestDF: DataFrame): Unit = {
    // 1. ACC
    val accEvaluator = new MulticlassClassificationEvaluator()
      .setPredictionCol("prediction")
      .setLabelCol("label")
      .setMetricName("accuracy") //精准度

    val trainAcc: Double = accEvaluator.evaluate(predictTrainDF)
    val testAcc: Double = accEvaluator.evaluate(predictTestDF)
    println(s"训练集上的 ACC 是 : $trainAcc")
    println(s"测试集上的 ACC 是 : $testAcc")
    //训练集上的 ACC 是 : 0.7512278050623347
    //测试集上的 ACC 是 : 0.7660406885758998
    // 2. AUC
    val trainRdd: RDD[(Double, Double)] = predictTrainDF.select("label", "prediction").rdd
      .map(row => (row.getAs[Double](0), row.getAs[Double](1)))
    val testRdd: RDD[(Double, Double)] = predictTestDF.select("label", "prediction").rdd
      .map(row => (row.getAs[Double](0), row.getAs[Double](1)))
    val trainAUC: Double = new BinaryClassificationMetrics(trainRdd).areaUnderROC()
    val testAUC: Double = new BinaryClassificationMetrics(testRdd).areaUnderROC()
    println(s"训练集上的 AUC 是 : $trainAUC")
    println(s"测试集上的 AUC 是 : $testAUC")
    //训练集上的 AUC 是 : 0.6591635864480606
    //测试集上的 AUC 是 : 0.7046995800897444
  }
}
