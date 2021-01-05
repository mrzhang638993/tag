package cn.itcast.model.mtag

import java.util.Properties

import cn.itcast.model.{CommonMeta, HbaseMeta, HdfsMeta, MetaData, Tag}
import cn.itcast.model.mtag.GenderModel.spark
import cn.itcast.model.utils.{BasicModel, ShcUtils}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

/**
 * 职业标签的书写逻辑操作
 * */
object JobModel  extends  BasicModel{

  val  TAG_NAME="职业"

  /**
   * 执行数据判断和逻辑操作执行
   * 对应的是匹配性的类型的数据的计算的。根据匹配数据执行逻辑计算操作
   * */
  def   process(df:DataFrame,fiveTags:Array[Tag],outFields:Array[String]): DataFrame ={
    import  spark.implicits._
    import org.apache.spark.sql.functions._
    // 构建查询条件和操作实现管理体现？
    var conditions:Column=null
    //  需要处理对应的规则匹配的逻辑和实现处理
    for(tag<-fiveTags){
      conditions=if(conditions==null){
         when('job===tag.rule,tag.id)
      }else{
        conditions.when('job===tag.rule,tag.id)
      }
    }
    // 列名称字段的对应操作
    conditions=conditions.as(outFields.head)
    //  执行dataFrame的操作筛选
    df.select('id,conditions)
  }

  /**
   * 获取对应的标签名称信息
   **/
  override def tagName(): String = {
    TAG_NAME
  }
}
