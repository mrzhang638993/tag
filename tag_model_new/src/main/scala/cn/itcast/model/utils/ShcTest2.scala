package cn.itcast.model.utils

import org.apache.spark.sql.{DataFrame, DataFrameReader, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog


object ShcTest2 {

  // 1.读取hbase的表数据
  // 1.1 配置hbase的表结构数据.SHC的配置是使用json的方式实现操作的。
  // 1.2 使用spark连接hbase执行操作
  // rowkey  指定的是hbase的rowkey的字段。
  //  shc读取的是sources下面的配置数据的。默认会读取sources下面的配置的。
  // 2.写入数据到hbase中
  def main(args: Array[String]): Unit = {
    //  指定的是hbase的表数据的，需要保证的是hbase的表是存在的。
     val catalog=s"""{
        |   "table":{
        |        "namespace":"default","name":"tbl_users"
        |     },
        |     "rowkey":"id",
        |	 "columns":{
        |	    "id":{"cf":"rowkey","col":"id","type":"string"},
        |		"userName":{"cf":"default","col":"username","type":"string"}
        |	 }
        |}""".stripMargin
    //  获取spark的session操作实现
    val spark: SparkSession = SparkSession
      .builder().master("local[6]")
      .appName("shc test")
      .getOrCreate()
    // 执行操作实现？spark 连接hbase
    val ds: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalog)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()

    val testCatalog=s"""{
                   |   "table":{
                   |        "namespace":"default","name":"tbl_users1"
                   |     },
                   |     "rowkey":"id",
                   |	 "columns":{
                   |	    "id":{"cf":"rowkey","col":"id","type":"string"},
                   |		"userName":{"cf":"default","col":"username","type":"string"}
                   |	 }
                   |}""".stripMargin
    ds.write.option(HBaseTableCatalog.tableCatalog,testCatalog)
      //  指定预分区的个数信息？
      .option(HBaseTableCatalog.newTable,"5")
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
  }
}
