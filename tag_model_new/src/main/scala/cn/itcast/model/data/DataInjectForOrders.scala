package cn.itcast.model.data

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataInjectForOrders {

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
        insert overwrite table itcast_dw.fact_order_refunds
        |select
        |id,
        |orderId,
        |goodsId,
        |refundTo,
        |refundReson,
        |refundOtherReson,
        |backMoney,
        |refundTradeNo,
        |refundRemark,
        |refundTime,
        |shopRejectReason,
        |refundStatus,
        |createTime,
        |modifiedTime,
        |date_format(modifiedTime,'yyyy-MM-dd') as dw_start_date,
        |'9999-12-31' as dw_end_date,
        |--此次数据分区按照订单退款的创建时间
        |date_format(createTime,'yyyyMMdd')
        |from itcast_ods.itcast_order_refunds where dt="20190909"
        |""".stripMargin
    val frame: DataFrame = spark.sql(sql)
    frame.show()
  }
}
