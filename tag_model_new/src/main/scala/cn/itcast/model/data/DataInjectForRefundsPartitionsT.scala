package cn.itcast.model.data

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataInjectForRefundsPartitionsT {

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
        |insert overwrite table itcast_dw.tmp_fact_orders
        |select
        |dw.orderId            ,
        |dw.orderNo            ,
        |dw.shopId             ,
        |dw.userId             ,
        |dw.orderStatus        ,
        |dw.goodsMoney         ,
        |dw.deliverType        ,
        |dw.deliverMoney       ,
        |dw.totalMoney         ,
        |dw.realTotalMoney     ,
        |dw.payType            ,
        |dw.isPay              ,
        |dw.areaId             ,
        |dw.userAddressId      ,
        |dw.areaIdPath         ,
        |dw.userName           ,
        |dw.userAddress        ,
        |dw.userPhone          ,
        |dw.orderScore         ,
        |dw.isInvoice          ,
        |dw.invoiceClient      ,
        |dw.orderRemarks       ,
        |dw.orderSrc           ,
        |dw.needPay            ,
        |dw.payRand            ,
        |dw.orderType          ,
        |dw.isRefund           ,
        |dw.isAppraise         ,
        |dw.cancelReason       ,
        |dw.rejectReason       ,
        |dw.rejectOtherReason  ,
        |dw.isClosed           ,
        |dw.goodsSearchKeys    ,
        |dw.orderunique        ,
        |dw.receiveTime        ,
        |dw.deliveryTime       ,
        |dw.tradeNo            ,
        |dw.dataFlag           ,
        |dw.createTime         ,
        |dw.settlementId       ,
        |dw.commissionFee      ,
        |dw.scoreMoney         ,
        |dw.useScore           ,
        |dw.orderCode          ,
        |dw.extraJson          ,
        |dw.orderCodeTargetId  ,
        |dw.noticeDeliver      ,
        |dw.invoiceJson        ,
        |dw.lockCashMoney      ,
        |dw.payTime            ,
        |dw.isBatch            ,
        |dw.totalPayFee        ,
        |dw.modifiedTime ,
        |dw.dw_start_date,
        |case when ods.orderid is not null and dw.dw_end_date ='9999-12-31'
        |then '2019-09-10'
        |else dw.dw_end_date
        |end as dw_end_date,
        |date_format(dw.createtime,'yyyyMMdd')
        |from
        |(select * from itcast_dw.fact_orders  where dt > '20190801')  dw left join
        |(select * from itcast_ods.itcast_orders where dt ='20190911') ods
        |on dw.orderid=ods.orderid
        |union all
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
        |from itcast_ods.itcast_orders where dt="20190911"
        |""".stripMargin
    val frame: DataFrame = spark.sql(sql)
    frame.show()
  }
}
