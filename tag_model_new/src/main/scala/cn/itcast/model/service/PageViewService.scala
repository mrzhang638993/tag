package cn.itcast.model.service

import java.util
import java.util.UUID

import cn.itcast.model.data.{PageViewsBeanCase, WebLogBean}
import cn.itcast.model.utils.DateUtil
import org.apache.spark.RangePartitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.CollectionAccumulator

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class PageViewService {

}

object PageViewService {


  def savePageViewToHdfs(filterStaticWeblogRdd: RDD[WebLogBean]) = {
    /*
    1：把key由原来uid变为uid+timeLocal字段
    2：使用rangepartitioner进行均匀分区
     */
    //key:uid+timeLocal ,value:weblogbean
    val uidTimeRdd: RDD[(String, WebLogBean)] = filterStaticWeblogRdd.map(bean => (bean.guid + "&" + bean.time_local, bean))
    //为了保证key全局有序使用sortbykey进行排序
    val sortedUidTimeRdd: RDD[(String, WebLogBean)] = uidTimeRdd.sortByKey()
    //使用rangepartitioner均匀分区
    val rangeRdd: RDD[(String, WebLogBean)] = sortedUidTimeRdd.partitionBy(new RangePartitioner(100, sortedUidTimeRdd))
    //使用累加器收集每个分区的首尾记录
    val spark: SparkSession = SparkSession.getDefaultSession.get
    //使用集合类型累加器收集分区的首尾数据
    val headTailList: CollectionAccumulator[(String, String)] = spark.sparkContext.collectionAccumulator[(String, String)]("headTailList")
    //对每个分区使用mappartitionwithindex算子进行sessionid和步长信息的生成
    val questionSessionRdd: RDD[(WebLogBean, String, Int, Long)] = generateSessionid(rangeRdd, headTailList)
    //累加器中有数据必须触发计算,使用累加器一定要注意重复计算的问题
    //对rdd数据进行cache防止重复计算
    questionSessionRdd.cache()
    questionSessionRdd.count()
    val headTailListValue: util.List[(String, String)] = headTailList.value

    //保存数据
    //questionSessionRdd.saveAsTextFile("/questionSessionRdd")
    //根据累加器中的边界数据判断哪些分区的边界存在问题
    //方便在累加器数据中获取指定分区的数据，我们把累加器数据结构调整为map类型：key:index+"&first/last",value: 记录的数据
    import collection.JavaConverters._
    val buffer: mutable.Buffer[(String, String)] = headTailListValue.asScala
    //转为一个可变map，方便更新其中的数据
    val map: mutable.HashMap[String, String] = mutable.HashMap(buffer.toMap.toSeq: _*) //map装有原来累加器中的数据
    //根据首尾数据判断边界问题得到需要修复的正确数据
    val correctMap: mutable.HashMap[String, String] = processBoundaryMap(map)
    //广播正确的map数据到每个executor
    val questionBroadCast: Broadcast[mutable.HashMap[String, String]] = spark.sparkContext.broadcast(correctMap)
    //经过修复过后的正确的rdd数据，（uidtime,sessionid,step,staylong）
    val correctRdd: RDD[(WebLogBean, String, Int, Long)] =
      repairBoundarySession(questionSessionRdd.filter(it=>it!=null), questionBroadCast)
    val pageviewRdd: RDD[PageViewsBeanCase] = correctRdd.map(
      t => {
        PageViewsBeanCase(
          t._2, t._1.remote_addr, t._1.time_local, t._1.request, t._3, t._4.toString,
          t._1.http_referer, t._1.http_user_agent, t._1.body_bytes_sent, t._1.status, t._1.guid
        )
      }
    )
    //  不存在对应的writemode操作的,需要进行关注操作
    pageviewRdd.saveAsTextFile("/pageviewrddtxt")
  }


  /**
   * 开始修复rdd数据执行修复操作。
   * */
  //修复我们rdd边界处的数据
  def repairBoundarySession(uidTimeSessionStepLongRdd: RDD[(WebLogBean, String, Int, Long)],
                            questionBroadCast: Broadcast[mutable.HashMap[String, String]]) = {
    //key:index/first/last,value:last-->timediff,first-->correctsessionid+correctstep+quesitonsessionid
    val questionMap: mutable.HashMap[String, String] = questionBroadCast.value
    val correctRdd: RDD[(WebLogBean, String, Int, Long)] = uidTimeSessionStepLongRdd.mapPartitionsWithIndex(
      (index, iter) => {
        //uid&time,sessionid,step,staylong
        var orginList = iter.toList
        val firstLine: String = questionMap.getOrElse(index + "&first", "")
        val lastLine: String = questionMap.getOrElse(index + "&last", "")
        //  对应的第一行的数据需要进行修复操作
        if (lastLine != "") {
          //当前这个分区最后一条数据他的停留时长需要修改
          val buffer: mutable.Buffer[(WebLogBean, String, Int, Long)] = orginList.toBuffer
          val lastTuple: (WebLogBean, String, Int, Long) = buffer.remove(buffer.size - 1) //只修改停留时长
          //  修复最后一行的stayLong信息的。步长信息不需要进行修改吗？
          buffer += ((lastTuple._1, lastTuple._2, lastTuple._3, lastLine.toLong))
          orginList = buffer.toList
        }

        /**
         * 对应的表示的是第一行的sessionid需要进行修改的。对应的后续的所有的和第一行的sessionId数据相等的全部都是需要进行修改的。
         * firstLine中存储了对应的初始的步长的信息的。默认的是对应的是1的。在correctMap中需要对应的进行修改操作的。
         * */
        if (firstLine != "") {
          //分区第一条数据有问题，则需要修改：按照错误的sessionid找到所有需要修改的数据，改正sessionid和step
          val firstArr: Array[String] = firstLine.split("&")
          val tuples: List[(WebLogBean, String, Int, Long)] = orginList.map {
            t => {
              if (t._2.equals(firstArr(2))) {
                //错误的sessionid,修改为正确的sessionid和步长
                //等于初始的步长加上当前的步长信息的。
                //  sessionId,step,stayLong信息,保存这些数据即可操作的。
                (t._1, firstArr(0), firstArr(1).toInt + t._3.toInt, t._4)
              } else {
                t
              }
            }
          }
          orginList = tuples
        }
        orginList.iterator
      }
    )
    correctRdd

  }

  /**
   * 得到修正后的分区首尾的map信息。
   * */
  //根据首尾数据找到有问题边界数据，以及修改的正确数据
  def processBoundaryMap(map: mutable.HashMap[String, String]) = {
    //定义一个map接收有问题分区需要修改的正确数据：key:index+"&first/last", value需要修改的正确数据
    val correctMap: mutable.HashMap[String, String] = new mutable.HashMap[String, String]()
    //遍历首尾记录map找到有问题的边界
    for (num <- 1 until (map.size / 2)) {
      //保证等于分区数据
      //获取num分区对应的首尾记录
      val numFirstMsg: String = map.get(num + "&first").get //uid+time+sessionid
      val numLastMsg: String = map.get(num + "&last").get //uid+time+sessionid+step+partition.size(分区数量)
      //获取到上一个分区的最后一条数据
      val lastPartLastMsg: String = map.get((num - 1) + "&last").get
      //判断当前分区与上个分区是否存在边界问题
      val numLastArr: Array[String] = numLastMsg.split("&")
      val lastPartLastArr: Array[String] = lastPartLastMsg.split("&")
      val numFirstArr: Array[String] = numFirstMsg.split("&")
      //判断是否同个用户,对应的是相同的用户信息
      if (lastPartLastArr(0).equals(numFirstArr(0))) {
        //判断时间差
        val timediff = DateUtil.getTimeDiff(lastPartLastArr(1), numFirstArr(1))
        //  同样的会话之中的。
        if (timediff < 30 * 60 * 1000) {
          //说明当前分区第一条数据与上个分区最后一条属于同个会话
          //上个分区记录需要修改的正确的停留时长数据
          //修改上一个分区的last信息进行操作管理实现？修改上一个时间中的stayLong信息的。
          correctMap.put((num - 1) + "&last", timediff.toString)
          //当前分区的第一条数据（有可能是多条数据严谨来说应该是当前分区的第一个session的数据）需要修改的数据
          //sessionid:与上个分区最后一条数据的sessionid保持一致,step:应该是上个分区最后一条记录的step+1
          //  #  初始的记录，currentTuple._1对应的是uid&time的信息
          //headTailList.add((index + "&first", currentTuple._1 + "&" + sessionid))
          //# 尾部数据的记录操作的
          //headTailList.add((index + "&last", currentTuple._1 + "&" + sessionid + "&" + step + "&" + list.size))
          // 最多存在5个元素的数据需要进行处理操作的，不会存在超过5个以上的数据需要进行处理的。
          //  需要参照上一个分区的信息来修改本分区的信息的。至于上一个分区的步长信息将在后面进行数据的整体的修复操作的
          if (lastPartLastArr.size > 5) {
            //正确的sessionid+正确的step+错误的sessionid
            // 需要修改一下前面的last的数据信息的。修改的是上一个分区最尾部的数据的数据信息的
            // 保存的数据是step$stay_long$uid
            correctMap.put(num + "&first", lastPartLastArr(lastPartLastArr.size - 2) + "&"
              + lastPartLastArr(lastPartLastArr.size - 1) + "&" + numFirstArr(2))
          } else {
            correctMap.put(num + "&first", lastPartLastArr(2) + "&" + lastPartLastArr(3) + "&" + numFirstArr(2))
          }
          //判断当前整个分区是否属于同个会话，属于同个会话则更新map中当前分区对应的最后一条数据的sessionid和ste数据
          //  分区内第一个元素的sessionId和分区内的最后一个元素的sessionId相同的话，需要整个分区的sessionId进行修改操作。
          if (numFirstArr(2).equals(numLastArr(2))) {
            //说明是同个会话，存在了会话穿透多个分区的现象
            //更新最后一条数据的step和sessionid信息
            //numlastMsg +正确的sessionid（上个分区的最后一条数据的sessionid）+正确的步长step(上个分区最后一条数据的步长+
            // 当前分区的数量)
            if (lastPartLastArr.size > 5) {
              map.put(num + "&last", numLastMsg + "&" + lastPartLastArr(lastPartLastArr.size - 2) + "&" +
                (lastPartLastArr(lastPartLastArr.size - 1).toInt + numLastArr(4).toInt))
            } else {
              //uid+time+sessionid+step+partition.size(分区数量)
              map.put(num + "&last", numLastMsg + "&" + lastPartLastArr(2) + "&" + (lastPartLastArr(3).toInt + numLastArr(4).toInt))
            }
          }
        }
      }
    }
    correctMap
  }

  //对均匀分区的rdd生成sessionid，使用累加器收集每个分区的首尾数据
  def generateSessionid(rangeRdd: RDD[(String, WebLogBean)],
                        headTailList: CollectionAccumulator[(String, String)]) = {
    //使用mappartitionwithindex算子
    val sessionidStepPageRdd: RDD[(WebLogBean, String, Int, Long)] = rangeRdd.mapPartitionsWithIndex {
      (index, iter) => {
        //iter-->list,list集合中依然是按照key有序分布的
        val list: List[(String, WebLogBean)] = iter.toList
        //准备一个list集合接收每条记录生成的sessionid的信息:数据内容：weblogbean,sessionid,step,pagestaylong
        val resultTupleList: ListBuffer[(WebLogBean, String, Int, Long)] = new ListBuffer[(WebLogBean, String, Int, Long)]()
        //准备sessionid，step，pagestaylong
        var sessionid = UUID.randomUUID().toString
        var step = 1
        var pagestaylong: Long = 60000
        //遍历list集合进行两两比较判断是否是同个用户以及时间是否小于30分钟
        import scala.util.control.Breaks._
        if(list.size==0){
           null
        }

        for (num <- 0 until (list.size)) {
          //取出当前遍历的数据
          val currentTuple: (String, WebLogBean) = list(num)
           //累加器收集第一条数据
          //把数据装入累加器中:key:分区编号+"&"+first/last,value:uid+time,sessionid
          //判断只有一条数据的情况
          if (list.size == 1) {
            //当前分区只有一条数据,不需要生成pageviewbeancase类型的数据,
            //添加数据到resulttuplelist中
            resultTupleList += ((currentTuple._2, sessionid, step, pagestaylong))
            //重新生成sessionid
            sessionid = UUID.randomUUID().toString
            headTailList.add((index + "&first", currentTuple._1 + "&" + sessionid))
            headTailList.add((index + "&last", currentTuple._1 + "&" + sessionid + "&" + step + "&" + list.size))
            //中止循环
            //break()
          }else if(num==0){
            //  编号为1的话
            resultTupleList += ((currentTuple._2, sessionid, step, pagestaylong))
            //重新生成sessionid
            sessionid = UUID.randomUUID().toString
            headTailList.add((index + "&first", currentTuple._1 + "&" + sessionid))
          } else {
            //判断不止有一条数据的情况
            //第一条数据我们跳过从第二条开始遍历，
            //实现第一条数据continue的效果
              //从第二条开始判断
              //获取到上一条的数据然后两两比较   //生成sessionid需要uid和time字段即可生成
              // 存在问题的处理逻辑:第一个元素的话，对应的前面的元素是不存在的？num=0的数据对应的是不需要进行处理的。
              val lastTuple: (String, WebLogBean) = list(num - 1)
              val currentUidTime: String = currentTuple._1
              val lastUidTime: String = lastTuple._1
              //取出uid和time
              //uid+"&"+time_local
              val currentUidTimeArr: Array[String] = currentUidTime.split("&")
              val lastUidTimeArr: Array[String] = lastUidTime.split("&")
              //计算时间差
              val timeDiff = DateUtil.getTimeDiff(lastUidTimeArr(1), currentUidTimeArr(1))
              //是不是同个用户
              if (lastUidTimeArr(0).equals(currentUidTimeArr(0)) && timeDiff < 30 * 60 * 1000) {
                //说明两条记录是同个session，保存上一条数据：sessionid,step,timediff
                resultTupleList += ((lastTuple._2, sessionid, step, timeDiff))
                //sessionid和step如何处置，sessionid无需重新生成，step必须要加1
                step += 1
              } else {
                //说明两条记录是不同的会话。需要重新的生成uuid以及对应的步长信息。
                //sessionid,step
                sessionid = UUID.randomUUID().toString
                //step重置
                step = 1
                //重新操作session信息
                resultTupleList += ((lastTuple._2, sessionid, step, pagestaylong))
              }
              //考虑最后一条数据的输出问题，存在一个问题只有一条数据的话，起始的数据对应的就是最终的数据信息。
              if (num == list.size - 1) {
                //需要保存最后一条数据
                resultTupleList += ((currentTuple._2, sessionid, step, pagestaylong))
                //使用累加器收集最后一条数据,key:index+"&"last/first,value:uid+time+sessionid+step+partition.size
                headTailList.add((index + "&last", currentTuple._1 + "&" + sessionid + "&" + step + "&" + list.size))
                //sessionid
                sessionid = UUID.randomUUID().toString
              }
            }
        }
        resultTupleList.toIterator
      }
    }
    sessionidStepPageRdd
  }
}
