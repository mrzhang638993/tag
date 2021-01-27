package cn.itcast.model.data

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * 进行点击流数据的清洗操作和实现
 * */
object ETLApp {

  /**
   * 点击流模型的清洗操作实现
   * */
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("etlApp")
      .master("local[6]")
      .enableHiveSupport()
      .getOrCreate()
    //  读取hdfs中的数据.读取的是文本文件的信息
    val value: Dataset[String] = spark.read.textFile("hdfs://hadoop01:8020/flume/tailout/2021-01-26/events-.1611668088629")
    value.show()
  }
}
