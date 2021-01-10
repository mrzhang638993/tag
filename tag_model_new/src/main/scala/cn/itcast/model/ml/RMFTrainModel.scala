package cn.itcast.model.ml

import cn.itcast.model.Tag
import cn.itcast.model.utils.BasicModel
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}

/**
 * RMFTrainModel:训练模型操作.训练模型保存数据到hdfs中
 * */
object RMFTrainModel extends  BasicModel{
  val MODEL_PATH="/models/rfm/kmeans"
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
    val cleanValue: Dataset[Row] = df.filter($"finishtime".isNotNull)
    //cleanValue.show()
    val frame: DataFrame = rmfScore(cleanValue)
    val result: DataFrame = assembleFeatures(frame)
    //聚类操作算法实现？进行模型的训练操作
    train(result,fiveTags.length)
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
    val rCol=(datediff(date_sub(current_timestamp(),503),from_unixtime(max('finishtime)))) as "r"
    //  消费频次
    val fCol=count('ordersn ) as "f"
    // 计算得到消费的总金额
    val mCol=sum('orderamount) as "m"
    val rfm: DataFrame = dataFrame.groupBy('memberid).agg(rCol, fCol, mCol)
    // 指定打分的规则操作
    // 2. 为 RFM 打分
    // R: 1-3天=5分，4-6天=4分，7-9天=3分，10-15天=2分，大于16天=1分
    // F: ≥200=5分，150-199=4分，100-149=3分，50-99=2分，1-49=1分
    // M: ≥20w=5分，10-19w=4分，5-9w=3分，1-4w=2分，<1w=1分
    val rScore = when('r >= 1 and 'r <= 3, 5)
      .when('r >= 4 and 'r <= 6, 4)
      .when('r >= 7 and 'r <= 9, 3)
      .when('r >= 10 and 'r <= 15, 2)
      .when('r >= 16, 1)
      .as("r_score")

    val fScore: Column = when('f >= 200, 5)
      .when(('f >= 150) && ('f <= 199), 4)
      .when((col("f") >= 100) && (col("f") <= 149), 3)
      .when((col("f") >= 50) && (col("f") <= 99), 2)
      .when((col("f") >= 1) && (col("f") <= 49), 1)
      .as("f_score")

    val mScore: Column = when(col("m") >= 200000, 5)
      .when(col("m").between(100000, 199999), 4)
      .when(col("m").between(50000, 99999), 3)
      .when(col("m").between(10000, 49999), 2)
      .when(col("m") <= 9999, 1)
      .as("m_score")

    val scores = rfm.select('memberid as "id", rScore, fScore, mScore)
    scores
  }

  /**
   * 特征处理操作.数据只是包含一列数据。执行模型的训练操作
   * */
  def  assembleFeatures(df:DataFrame): DataFrame ={
    val assembled = new VectorAssembler()
      .setInputCols(Array("r_score", "f_score", "m_score"))
      .setOutputCol("features")
      //  处理不可用的数值，对于null的数值直接丢掉数据
      .setHandleInvalid("skip")
      .transform(df)
    assembled
  }

  /**
   * 进行模型的训练操作.执行模型的训练操作和实现
   * */
  def  train(dataFrame: DataFrame,k:Int)={
     // 执行无监督的训练操作
    val classifier=new KMeans()
      .setK(k)
      .setSeed(10)
      //  最大的迭代次数
      .setMaxIter(10)
      //  预测的字段
      .setFeaturesCol("features")
       //  输出的字段
      .setPredictionCol("predict")

    // 执行训练操作
    //  fit对应的是一个训练的过程.产生模型，存储模型数据到hdfs中的
    classifier.fit(dataFrame).save(MODEL_PATH)
  }
}
