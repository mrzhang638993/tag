package cn.itcast.model.utils

import cn.itcast.model.{HBaseCataLog1, HBaseColumn1, HBaseTable1, HdfsMeta, MetaData}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable

/**
 * hbase的工具类信息
 * */
object ShcUtils {

  val TAG_NAME="性别"
  val HBASE_NAMESPACE="default"
  val HBASE_ROWKEY_FIELD="id"
  val HBASE_COLUMN_DEFAULT_TYPE="string"

    /**
     *  hbase的数据读取操作和实现。给定读取参数，读取hbase中的数据信息。
     * */
    def  read(inFields: Array[String],columnFamily:String,tableName:String,spark:SparkSession): DataFrame ={
      val columns=mutable.HashMap.empty[String,HBaseColumn1]
      val rowkey=HBASE_ROWKEY_FIELD
      //  指定rowkey对应的字段信息？
      val hbase1=HBaseTable1(HBASE_NAMESPACE,tableName);
      columns += HBASE_ROWKEY_FIELD->HBaseColumn1("rowkey",HBASE_ROWKEY_FIELD,HBASE_COLUMN_DEFAULT_TYPE)
      // 数据字段不完整，需要的是完整的数据字段操作。
      for(filed<-inFields){
        columns += filed->HBaseColumn1(columnFamily,filed,HBASE_COLUMN_DEFAULT_TYPE)
      }
      val hbaseCatalog=HBaseCataLog1(hbase1,rowkey,columns.toMap)
      val catalogJson: String = objectToJson(hbaseCatalog)
      val df: DataFrame = spark.read.option(HBaseTableCatalog.tableCatalog, catalogJson)
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .load()
      df
    }

  /**
   * 读取hdfs中的文件信息
   * */
  def readHdfs(metaData:MetaData,spark:SparkSession): DataFrame = {
    val meta: HdfsMeta = metaData.toHdfsMeta()
    import org.apache.spark.sql.functions._
    val fields: Array[String] = meta.commonMeta.inFields
    var fieldIn: mutable.ArrayBuilder[StructField] = mutable.ArrayBuilder.make[StructField]
    if (fields != null && fields.size > 0) {
      for (field <- fields) {
        fieldIn += StructField(field, StringType)
      }
    }
    val structType: StructType = StructType(fieldIn.result())
    val outFileds: Array[String] = meta.commonMeta.outFields
    if (outFileds != null && outFileds.size > 0) {
      var fieldOut: mutable.ArrayBuilder[Column] = mutable.ArrayBuilder.make[Column]
      for (field <- outFileds) {
        fieldOut += col(field)
      }
      val df: DataFrame = spark.read
        .option("seperator", meta.separator)
        .schema(structType)
        .load(meta.inPath)
        .select(fieldOut.result(): _*)
      df
    }else{
      null
    }
  }

  /**
   * 对象转化为json的处理方式和方法
   * */
   def objectToJson(hbaseCatalog:HBaseCataLog1): String ={
     import org.json4s._
     //  序列化的操作实现
     import org.json4s.jackson.Serialization
     //  导入工具方法
     import org.json4s.jackson.Serialization.write
     //导入隐式转换操作的方法和实现逻辑。需要格式转换操作和实现的
     implicit  val formats=Serialization.formats(NoTypeHints)
     //  执行隐式转换的操作实现和逻辑
     val catalogJson: String = write(hbaseCatalog)
     catalogJson
   }

    def write(): Unit ={

    }
}
