package cn.itcast.model.stats

import cn.itcast.model.Tag
import cn.itcast.model.utils.BasicModel
import org.apache.spark.sql.{Column, DataFrame}

/**
 * 统计型的标签信息
 * */
object GenerationModel  extends  BasicModel{

  def main(args: Array[String]): Unit = {
      startFlow()
  }
  /**
   * 获取对应的标签名称信息
   **/
  override def tagName(): String = {
    "年龄段"
  }

  /**
   * 处理方法和操作逻辑
   * 输入的字段是id,birthday
   * 输出的字段是id,tag_generation
   **/
  override def process(df: DataFrame, fiveTags: Array[Tag], outFields: Array[String]): DataFrame = {
     // 导入依赖
    import spark.implicits._
    import org.apache.spark.sql.functions._
    //  执行规则计算
    //  根据标签对应的年龄段的数据执行计算操作。19500101-19591231之间对应的是50年的年龄段的用户的。
    // 将conditions作为列字段进行处理操作和实现管理。
    var conditions:Column=null;
    df.show(10)
    for(tag<-fiveTags){
       //  获取开始时间和结束时间的时间信息？
       val start: String = tag.rule.split("-")(0)
       val end: String = tag.rule.split("-")(1)
       //  需要处理和拼装开始时间和结束时间的时间信息？
       val start_year: String = start.substring(0, 4)
       val start_month: String = start.substring(4, 6)
       val start_day: String = start.substring(6, 8)
       val end_year: String = end.substring(0, 4)
       val end_month: String = end.substring(4, 6)
       val end_day: String = end.substring(6, 8)
       conditions=if(conditions==null){
         when('birthday between(start_year+start_month+start_day,end_year+end_month+end_day),tag.id)
       }else{
          conditions.when('birthday between(start_year+start_month+start_day,end_year+end_month+end_day),tag.id)
       }
    }
    conditions=conditions.as(outFields.head)
    // 存储数据
    df.select('id,conditions)
  }
}
