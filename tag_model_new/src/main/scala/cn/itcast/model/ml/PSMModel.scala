package cn.itcast.model.ml

import cn.itcast.model.Tag
import cn.itcast.model.utils.BasicModel
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.types.DoubleType

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
    stage4.show()
    null
  }
}
