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
    val value: Dataset[String] = spark.read.textFile("hdfs://hadoop01:8020/flume/tailout/2021-01-26/events-.1611668088629").cache()
    // 下面执行数据清洗操作，得到符合要求的dataStream数据信息
    //  执行数据的过滤操作和实现。得到对应的需要过滤完成的数据信息。
    val clickDataStream:DataStream[ClickDataLog]=transformData(value)
    val destCleanValue:DataStream[ClickDataLog]=clickDataStream.filter(it->it.valid===1)
    // 执行数据的写入操作。将清洗之后的数据写入到对应的hdfs文件中进行管理操作的。数据写入到对应的hdfs文件中进行操作
    destCleanValue.write().mode(SaveMode.Overwrite).save("")
  }
  
  
  /**
   *  数据的标记操作，标记数据是有效的还是无效的数据信息
  */
  /***def tagDataValid()*/
  
  /**
  *  进行数据的转换操作，得到对应的数据的bean对象的对象信息。同时将对应的数据信息增加valid的数据标签信息。
  */
  def transformData(originData:Dataset[String]):DataStream[ClickDataLog]:Unit={
     //  处理对应的数据信息，完成数据信息的转换操作和实现
  }
  
  /**
   *  清洗数据得到想要的dataStream的数据信息？
  */
  /**def  getDestDataStream(originData:Dataset[String]):DataStream[String]={*/
     /***/
  /**}*/
}


/**
*   创建对应的点击流的数据模型信息。将对应的String类型的数据信息转化成为对应的bean类型的数据信息
*/
case Class  ClickDataLog()
