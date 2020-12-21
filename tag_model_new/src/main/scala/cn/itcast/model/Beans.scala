package cn.itcast.model

import org.apache.commons.lang3.StringUtils

/**
 * 所有的标签对应的样例类对象。
 * */
case class  Tag(id:String,name:String,rule:String,pid:String)

/**
 * 元数据的样例类信息
 * 支持的原始数据库的操作的。支持mysql,hdfs文件，hbase文件数据
 * 针对这些类型进行单独处理操作实现。所以需要区分开不同类型的元数据信息。
 * */
case class MetaData(in_type:String, driver:String, url:String,
                     username:String, password:String, db_table:String,
                     in_path:String, sperator:String, in_fields:String,
                     cond_fields:String, out_fields:String, out_path:String,
                     zk_hosts:String, zk_port:String, hbase_table:String,
                     family:String, select_field_names:String, where_field_names:String,
                     where_field_values:String
                   ){

  /**
   * 定义实现简单的义务逻辑实现
   * */
  def isHbase():Boolean={
      in_type.toLowerCase()=="hbase"
  }

  /**
   * 定义是否是mysql的数据类型
   * */
  def isRDBMS(): Boolean ={
    in_type.toLowerCase=="mysql"||in_type.toLowerCase=="postgresql"||
    in_type.toLowerCase=="oracle"||in_type.toLowerCase=="rdbms"
  }

  /***
   *  定义是否是hdfs的数据类型信息
   */
  def isHdfs():Boolean={
    in_type.toLowerCase()=="hdfs"
  }

  /**
   * 将元数据转换为对应类型的元数据对象。转化成为hbase的metadata
   * */
  def  toHbaseMeta():HbaseMeta={
    if(!isHbase()){
       //  抛异常对应的是最为严谨的操作实现
      null
    }
    if(StringUtils.isEmpty(in_fields)||StringUtils.isEmpty(out_fields)){
      // 抛异常对应的是最为严谨的操作实现
      null
    }
    val inFields: Array[String] = in_fields.split(",")
    val outFields: Array[String] = out_fields.split(",")
    HbaseMeta(CommonMeta(in_type,inFields,outFields),hbase_table,family)
  }

  /**
   * 将数据转化为mysql的MySqlMeta的原始数据执行操作
   * */
  def toMySqlMeta():MySqlMeta={
      if(!isRDBMS()){
        null
      }
    if(StringUtils.isEmpty(in_fields)||StringUtils.isEmpty(out_fields)){
      // 抛异常对应的是最为严谨的操作实现
      null
    }
     //  判断转换操作
    val inFields: Array[String] = in_fields.split(",")
    val outFields: Array[String] = out_fields.split(",")
    MySqlMeta(CommonMeta(in_type,inFields,outFields),driver,url,username,password,db_table)
  }

  /**
   * 将原始数据转化成为hdfs的原始数据执行操作实现.HdfsMeta
   * */
  def toHdfsMeta():HdfsMeta={
    if(!isHdfs()){
      null
    }
    if(StringUtils.isEmpty(in_fields)||StringUtils.isEmpty(out_fields)){
      // 抛异常对应的是最为严谨的操作实现
      null
    }
    //  判断转换操作
    val inFields: Array[String] = in_fields.split(",")
    val outFields: Array[String] = out_fields.split(",")
    HdfsMeta(CommonMeta(in_type,inFields,outFields),in_path,sperator)
  }
}

/**
 * 下面是通用的数据类型
 * */
case class CommonMeta(inType:String,inFields:Array[String],outFields:Array[String])

/**
 * mysql的元数据信息
 * */
case class MySqlMeta(commonMeta: CommonMeta,driver:String,url:String,userName:String,password:String,tableName:String)

/**
 * hbase的元数据信息
 * */
case class HbaseMeta(commonMeta: CommonMeta,tableName:String,columnFamily:String)

/**
 * hdfs的元数据信息
 * */
case class HdfsMeta(commonMeta: CommonMeta,inPath:String,separator:String)
