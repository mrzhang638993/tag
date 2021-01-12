package cn.itcast.model.ml

import cn.itcast.model.Tag
import cn.itcast.model.utils.BasicModel
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.types.DoubleType

import scala.collection.immutable

/**
 * 对应的也是需要执行聚类操作实现的？
 * */
object PSMModel  extends  BasicModel{

  /**
   * 价格敏感度计算，需要进行关注和理解的。
   * */
  def main(args: Array[String]): Unit = {
      startFlow()
  }
  /**
   * 获取对应的标签名称信息
   **/
  override def tagName(): String = {
     "促销敏感度"
  }

  /**
   * 处理方法和操作逻辑
   **/
  override def process(df: DataFrame, fiveTags: Array[Tag], outFields: Array[String]): DataFrame = {
     import spark.implicits._
     import org.apache.spark.sql.functions._
      // 计算不分组字段
      //   应收金额=订单金额+优惠金额
      val  receiveableAmount=(('orderamount.cast(DoubleType)+'couponcodevalue.cast(DoubleType))) as "receiveableAmount"
      //   优惠金额
      val discountAmount=('couponcodevalue.cast(DoubleType)) as "discountAmount"
      //   实收金额
      val particalAmount=('orderamount.cast(DoubleType))  as "particalAmount"
      //  计算是否优惠的字段.等于0对应的是没有优惠的等于0，否则对应的是有优惠的，对应的是1的。
      val state=when(discountAmount.isNull || discountAmount===0.0d,0)
         .when(discountAmount.isNotNull && discountAmount=!= 0.0d,1)
         .as("state")
      //  得到对应的stage1的数据信息
      val  stage1=df.select('memberid as "id",receiveableAmount,discountAmount,particalAmount,state)
     //  计算分组字段
      //  计算优惠的订单数信息
      //  id|receiveableAmount|discountAmount|particalAmount|state
      val  discountCount=sum('state) as "discountCount"
      //  计算订单总数
      val  orderCount=count('state) as "orderCount"
      //   计算优惠总额
      val  totalDiscount=sum('discountAmount) as "totalDiscount"
      //  计算应收总额
      val   totalReceiveAmount=sum('receiveableAmount) as "totalReceiveAmount"
      val stage2: DataFrame = stage1.groupBy('id).agg(discountCount, orderCount, totalDiscount, totalReceiveAmount)
     //   计算集成字段信息
     //  平均优惠金额
     val avgDiscountAmount=('totalDiscount/'discountCount) as "avgDiscountAmount"
     // 平均 每单应收
     val avgReceivableAmount=('totalReceiveAmount/'orderCount) as "avgReceivableAmount"
     // 优惠订单占比
     val  discountPercent=('discountCount/'orderCount) as "discountPercent"
     // 平均优惠金额占比
     val avgDiscountPercent=(avgDiscountAmount/avgReceivableAmount) as "avgDiscountPercent"
     // 优惠金额占比
     val discountAmountPercent=('totalDiscount/'totalReceiveAmount) as "discountAmountPercent"
     val stage3: DataFrame = stage2.select('id, avgDiscountAmount, avgReceivableAmount, discountPercent, avgDiscountPercent, discountAmountPercent)
     //  计算psmScore指标
     // PSM Score = 优惠订单占比 + (平均优惠金额 / 平均每单应收) + 优惠金额占比
     val psmScore=('avgDiscountPercent+'avgDiscountAmount/'avgReceivableAmount+'discountAmountPercent) as "psmScore"
    val stage4: DataFrame = stage3.select('id, psmScore)
    // 肘部法则，确定k的值。
    val  vectorAssembler=new VectorAssembler()
      .setInputCols(Array("psmScore"))
      .setOutputCol("features")
      .setHandleInvalid("skip")
      .transform(stage4)
    //  确定k，需要对k执行操作处理实现？多次计算k，获取得到损失最小的k执行操作的
    //val kArr=Array(2,3,4,5,6,7,8)
    //  得到不同的key下面的cost操作
    /*val keyCosts: Array[(Int, Double)] = kArr.map(k => {
      val kmeans = new KMeans()
        .setK(k)
        .setMaxIter(10)
        .setPredictionCol("predict")
        .setFeaturesCol("features")
      //  执行数据的训练操作，得到model对象
      val model: KMeansModel = kmeans.fit(vectorAssembler)
      //  执行损失的预测。确定肘部预测操作
      //  得到第一次的损失数值
      val cost: Double = model.computeCost(vectorAssembler)
      (k, cost)
    })*/
    // 绘制图形，发现k出于4-7之间是处于肘部的。是合理的。
    // 结合业务逻辑，对应的取值，k是可以获取到5的数据的。
    //  使用k=5执行数据计算操作。
    //  排序操作的代码输出。需要结合标签对数据进行输出操作实现。
    val kmeans = new KMeans()
      .setK(5)
      .setMaxIter(10)
      .setPredictionCol("predict")
      .setFeaturesCol("features")
    //  执行数据的训练操作，得到model对象
    val model: KMeansModel = kmeans.fit(vectorAssembler)
    // 排序操作实现？怎么排序操作实现的？
    // 通过聚类操作实现分类操作实现。聚类将多个散点数据归类起来进行操作实现管理。
    null
  }
}
