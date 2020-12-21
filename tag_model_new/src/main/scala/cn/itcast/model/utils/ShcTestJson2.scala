package cn.itcast.model.utils

import cn.itcast.model.{HBaseCataLog1, HBaseColumn1, HBaseTable1}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog

import scala.collection.mutable

/**
 * 使用对象的方式操作属性而不是之前的字符串执行的操作的。
 * 真正实际操作的时候是使用对象的方式来操作数据的。
 * */
object ShcTestJson2 {

  def main(args: Array[String]): Unit = {
      //  创建对象和类对象信息？
      //  处理catalog的数据信息？
      //  处理完成之后，转换成为json对象执行操作的。
      val  table=HBaseTable1("default","tbl_users1")
      val  rowkey="id"
      val mapResult=mutable.HashMap.empty[String,HBaseColumn1]
     //  rowkey的column不能是不同的列族的，必须是完整的列族的
    //  指定id对应的是rowkey字段的数据。
      mapResult += "rowkey"->HBaseColumn1("rowkey","id","string")
      mapResult += "username"->HBaseColumn1("default","username","string")
      val  catalog=HBaseCataLog1(table,rowkey,mapResult.toMap)
    //  转换成为json的格式的。需要使用的是json4s的操作的
    import org.json4s._
    //  序列化的操作实现
    import org.json4s.jackson.Serialization
    //  导入工具方法
    import org.json4s.jackson.Serialization.write
    //导入隐式转换操作的方法和实现逻辑。需要格式转换操作和实现的
    implicit  val formats=Serialization.formats(NoTypeHints)
    //  执行隐式转换的操作实现和逻辑
    val catalogJson: String = write(catalog)
    println(catalogJson)
    //读取数据
    val spark: SparkSession = SparkSession.builder()
      .master("local[6]")
      .appName("create object")
      .getOrCreate()
    //读取数据
    val ds: DataFrame = spark.read
      .option(HBaseTableCatalog.tableCatalog, catalogJson)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    ds.show()
  }
}



