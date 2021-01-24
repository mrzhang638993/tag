package cn.itcast.model.data

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataInjectForRefundsAll {

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
        insert overwrite table itcast_dw.tmp_fact_order_refunds
        |select
        |dw.id,
        |dw.orderId,
        |dw.goodsId,
        |dw.refundTo,
        |dw.refundReson,
        |dw.refundOtherReson,
        |dw.backMoney,
        |dw.refundTradeNo,
        |dw.refundRemark,
        |dw.refundTime,
        |dw.shopRejectReason,
        |dw.refundStatus,
        |dw.createTime,
        |dw.modifiedTime,
        |dw.dw_start_date,
        |case when ods.id is not null and dw.dw_end_date ='9999-12-31'
        |then '2019-09-09'
        |else dw.dw_end_date
        |end as dw_end_date,
        |date_format(dw.createTime,'yyyyMMdd')
        |from  itcast_dw.fact_order_refunds  dw
        |left join (select * from itcast_ods.itcast_order_refunds where dt="20190910") ods
        |on dw.id =ods.id
        |union all
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
        |date_format(createTime,'yyyyMMdd')
        |from itcast_ods.itcast_order_refunds where dt="20190910"
        |""".stripMargin
    val frame: DataFrame = spark.sql(sql)
    frame.show()
  }
}
