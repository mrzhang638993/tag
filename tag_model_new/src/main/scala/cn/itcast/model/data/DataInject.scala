package cn.itcast.model.data

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataInject {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName("WeiBoAccount-Verified")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("hive.exec.dynamic.partition", true)
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.debug.maxToStringFields","100")
      .config("hadoop.home.dir", "/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()
    val sql=
      """
        |insert overwrite table itcast_dw.fact_orders
        |select
        |orderId            ,
        |orderNo            ,
        |shopId             ,
        |userId             ,
        |orderStatus        ,
        |goodsMoney         ,
        |deliverType        ,
        |deliverMoney       ,
        |totalMoney         ,
        |realTotalMoney     ,
        |payType            ,
        |isPay              ,
        |areaId             ,
        |userAddressId      ,
        |areaIdPath         ,
        |userName           ,
        |userAddress        ,
        |userPhone          ,
        |orderScore         ,
        |isInvoice          ,
        |invoiceClient      ,
        |orderRemarks       ,
        |orderSrc           ,
        |needPay            ,
        |payRand            ,
        |orderType          ,
        |isRefund           ,
        |isAppraise         ,
        |cancelReason       ,
        |rejectReason       ,
        |rejectOtherReason  ,
        |isClosed           ,
        |goodsSearchKeys    ,
        |orderunique        ,
        |receiveTime        ,
        |deliveryTime       ,
        |tradeNo            ,
        |dataFlag           ,
        |createTime         ,
        |settlementId       ,
        |commissionFee      ,
        |scoreMoney         ,
        |useScore           ,
        |orderCode          ,
        |extraJson          ,
        |orderCodeTargetId  ,
        |noticeDeliver      ,
        |invoiceJson        ,
        |lockCashMoney      ,
        |payTime            ,
        |isBatch            ,
        |totalPayFee        ,
        |modifiedTime       ,
        |date_format(modifiedTime,'yyyy-MM-dd') as dw_start_date,
        |'9999-12-31' as dw_end_date,
        |date_format(createtime,'yyyyMMdd')
        |from itcast_ods.itcast_orders where dt="20190909"
        |""".stripMargin
    val frame: DataFrame = spark.sql(sql)
    frame.show()
  }
}
