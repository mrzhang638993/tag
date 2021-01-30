package cn.itcast.model.data

import java.text.SimpleDateFormat
import java.util.{Date, UUID}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * 进行点击流数据的清洗操作和实现
 * */
object ETLApp {

  /**
   * 点击流模型的清洗操作实现
   * 不曾定义人任何的数据集，
   * 对应的是不存在任何的schema信息的，数据的流转是多种dataFrame的操作实现的。
   * */
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("etlApp")
      .master("local[6]")
      .getOrCreate()
    //  读取hdfs中的数据.读取的是文本文件的信息
    val value: Dataset[String] = spark.read.textFile("hdfs://hadoop01:8020/flume/tailout/2021-01-26/events-.1611668088629").cache()
    import  spark.implicits._
    //  转换成为对象信息的。
    val touchHitLog: JavaRDD[TouchHitLog] = value.map(it => {
      val log: TouchHitLog = stringToTouchHitLog(it)
      log
    }).filter(it => it != null).toJavaRDD
    //过滤出来静态的资源路径信息。
    initlizePages()
    //过滤静态资源路径信息,得到符合要求的rdd的操作信息
    //分布式的环境下面，对应的数据操作是需要进行共享操作的，可以节省对应的代码量信息的？需要采用广播的方式实现操作的
    val meansRdd: RDD[TouchHitLog] = touchHitLog.rdd.filter(it => {
      pages.contains(it.url) match {
        case true => false
        case false => true
      }
    })
    // 缺少了在页面停留的时长信息的？需要增加页面的停留时长的信息的。
    //分组,对于组内的rdd执行排序操作，增加增加uuid的信息？缺少了schema的信息
    //  groupby的数据倾斜操作？
    val uuidSessionBean: RDD[PageViewsBeanCase1] = meansRdd.groupBy(it => it.uid).flatMap(it => {
      val values: Iterable[TouchHitLog] = it._2
      //  list的信息。进行排序操作和实现.根据请求的顺序降序排列操作
      val list: List[TouchHitLog] = values.toList.sortBy(_.requestTime)
      //  计算每一个list中的数据，然后执行数据的更新的操作和实现管理
      val value1: List[PageViewsBeanCase1] = addUuidInfo(list)
      value1
    }).filter(it=>it!=null)
    // pageview数据模型的数据保存操作
    if(uuidSessionBean!=null){
       //执行数据的写入操作实现
      uuidSessionBean.toDS().write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/itcast_ods.db/click_pageviews/dt=20191101/")
    }
    //下面是vists模型的编程操作和实现
    if(uuidSessionBean!=null){
      // 根据session的数据进行判断，获取session中的第一个步骤和最后的一个步骤
      val visitModel: RDD[VisitBeanCase1] = uuidSessionBean.groupBy(it => it.session).map(it => {
        val values: Iterable[PageViewsBeanCase1] = it._2
        val cases: List[PageViewsBeanCase1] = values.toList.sortBy(it => it.requestTime)
        //  获取排序之后的最小的时间对应的操作，以及排序之后最大的时间对应的排序的操作
        getVisitModelBean(cases)
      })
      // 保存visit模型的数据到hdfs中
      visitModel.toDS().write.mode("overwrite")
        .parquet("/user/hive/warehouse/itcast_ods.db/click_stream_visit/dt=20191101/")
    }
    spark.stop()
  }

  //我们准备一个静态资源的集合
  // 用来存储网站url分类数据
  val pages = new mutable.HashSet[String]()
  //
  def initlizePages(): Unit = {
    pages.add("/about")
    pages.add("/black-ip-list/")
    pages.add("/cassandra-clustor/")
    pages.add("/finance-rhive-repurchase/")
    pages.add("/hadoop-family-roadmap/")
    pages.add("/hadoop-hive-intro/")
    pages.add("/hadoop-zookeeper-intro/")
    pages.add("/hadoop-mahout-roadmap/")
  }


  /**
   * 对应的获取到相关的bean的class信息
   * uid      :String,
   * session			: String,
   * remote_addr	: String,
   * inTime				: String,
   * outTime			: String,
   * inPage			  : String,
   * outPage			: String,
   * referal			: String,
   * pageVisits		: Int
   * */
  def  getVisitModelBean(cases: List[PageViewsBeanCase1]): VisitBeanCase1 ={
    val head: PageViewsBeanCase1 = cases.head
    val last: PageViewsBeanCase1 = cases.last
    VisitBeanCase1(head.uid,head.session,head.ip,
      head.requestTime,last.requestTime,head.url,
      last.url,head.requestUrl,cases.size)
  }
  /**
   * 获取得到对应的bean信息的内容和实现操作
   * 给对应的数据记录增加uuid信息。
   * */
  def addUuidInfo(list: List[TouchHitLog]):List[PageViewsBeanCase1]={
    //前后之间的时间间隔超过了30分钟，对应的需要重新生成对应的uuid信息。任意的两个之间的时间间隔超过了30分钟，重新生成一个uuid信息
    val list1 = new ListBuffer[PageViewsBeanCase1]()
    var uuid: String = UUID.randomUUID().toString
    var currentLog:TouchHitLog=null
    //  处理
    val size: Int = list.size
    if(size==0){
      val log: TouchHitLog = list(0)
      val beanCase: PageViewsBeanCase1 = createPageViewsBeanCase(uuid, log)
      list1 += beanCase
    }else{
      for(num<- 0 until list.size-1) {
        val log: TouchHitLog = list(num)
        if(currentLog==null){
          currentLog=log
          val beanCase: PageViewsBeanCase1 = createPageViewsBeanCase(uuid, log)
          list1 += beanCase
        }else{
          // 相邻的两个元素的时间间隔超过了30分钟的话，对应的执行操作处理的
          val timeDiff: Long = getTimeDiff(currentLog, log)
          if(timeDiff>=30*60*1000){
            uuid=UUID.randomUUID().toString
            val beanCase: PageViewsBeanCase1 = createPageViewsBeanCase(uuid, log)
            list1 += beanCase
          }else{
            val beanCase: PageViewsBeanCase1 = createPageViewsBeanCase(uuid, log)
            list1 += beanCase
          }
          currentLog=log
        }
      }
    }
    if(list1.size==0){
      null
    }else{
      list1.toList
    }
  }

  /**
   * 获取之前的数据信息和对应的后续的信息
   * */
  def getTimeDiff(priv:TouchHitLog,next:TouchHitLog):Long={
    val privTime: String = priv.requestTime
    val nextTime: String = next.requestTime
    //  当前时间和之后的时间信息？
    val format=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val privData: Date = format.parse(privTime)
    val nextData: Date = format.parse(nextTime)
    nextData.getTime-privData.getTime
  }

  /**
   * 处理对应的weblogBean的注册信息
   * */
  def createPageViewsBeanCase(uuid:String,log: TouchHitLog)={
    val beanCase: PageViewsBeanCase1 = PageViewsBeanCase1(
      uuid, log.uid, log.ip,
      log.info, log.requestTime, log.method,
      log.url, log.protocol, log.responseCode,
      log.dataSize, log.requestUrl, log.chrome
    )
    beanCase
  }
  /**
   * 字符串转化成为bean对象
   * */
  def stringToTouchHitLog(content:String):TouchHitLog={
    //  得到的数据的字段信息
    val nonEmptyStr: String = content.replaceAll("\"", "")
    val fields: Array[String] = nonEmptyStr.split(" ")
    fields.size match {
      case 0=>null
      case 1=>null
      case _=> TouchHitLog(fields(0),fields(1),fields(2)+" "+fields(3),fields(4)+" "+fields(5),fields(6),fields(7),fields(8),fields(9),fields(10),fields(11),fields(12))
    }
  }
}

/**
 * 可以重写对应的apply的操作方法实现更多的数据处理操作
 * */
case class  TouchHitLog(
                         uid:String,
                         ip:String,
                         info:String,
                         requestTime:String,
                         method:String,
                         url:String,
                         protocol:String,
                         responseCode:String,
                         dataSize:String,
                         requestUrl:String,
                         chrome:String
                       )

/**
 * 获取对应的case-class的信息操作
 * */
case class PageViewsBeanCase1 (session: String,
                             uid:String,
                             ip:String,
                             info:String,
                             requestTime:String,
                             method:String,
                             url:String,
                             protocol:String,
                             responseCode:String,
                             dataSize:String,
                             requestUrl:String,
                             chrome:String)

/**
 * 获取visit模型对应的数据信息
 * */
case class VisitBeanCase1(
                          uid      :String,
                          session			: String,
                          remote_addr	: String,
                          inTime				: String,
                          outTime			: String,
                          inPage			  : String,
                          outPage			: String,
                          referal			: String,
                          pageVisits		: Int )



