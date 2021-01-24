package cn.itcast.model.data

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataInjectForDw {

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
        |* from
        |itcast_dw.tmp_fact_orders
        |""".stripMargin
    val frame: DataFrame = spark.sql(sql)
    frame.show()
  }
}
