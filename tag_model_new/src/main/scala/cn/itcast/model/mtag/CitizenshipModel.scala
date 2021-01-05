package cn.itcast.model.mtag

import cn.itcast.model.Tag
import cn.itcast.model.utils.BasicModel
import org.apache.spark.sql.{Column, DataFrame}

object CitizenshipModel extends  BasicModel{

  var CITIZENSHIP_MODEL="国籍"

  def main(args: Array[String]): Unit = {
     startFlow()
  }
  /**
   * 获取对应的标签名称信息
   **/
  override def tagName(): String = {
    CITIZENSHIP_MODEL
  }

  /**
   * 处理方法和操作逻辑
   **/
  override def process(df: DataFrame, fiveTags: Array[Tag], outFields: Array[String]): DataFrame = {
      //  导入spark对应的隐式转换
    import spark.implicits._
    import org.apache.spark.sql.functions._
      //  执行匹配计算，生成条件列
    var conditions:Column=null
    for(tag<-fiveTags){
      conditions=if(conditions==null){
               when('nationality===tag.rule,tag.id)
            }else{
         conditions.when('nationality===tag.rule,tag.id)
       }
    }
    conditions=conditions.as(outFields.head)
    //  在source上面执行筛选，执行条件。
    df.select('id,conditions)
  }
}
