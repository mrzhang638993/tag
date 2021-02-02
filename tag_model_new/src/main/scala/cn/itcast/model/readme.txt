标签只给需要打标签的数据打上标签的，不需要的数据首先执行过滤操作排除数据即可执行操作的。
数据集的数据太杂乱了，导致很多的错误出现，存在无法运行的情况的。
topN的方式适合处理组内的第n个元素的。
所有的数据的finishTime对应的都是空的数据信息的。
步骤：
1.处理数据，得到数据集；
2.使用学习型的算法，从数据中找到数据的规律。
机器学习处理的问题：1.回归；2.分类。
规律对应的称之为模型。
机器学习：
1.监督学习：监督找到数据的规律。数据规律已经存在，需要算法找出规律进行验证操作；使用标签验证学习的过程。
2.无监督学习：zeppelin
spark 可视化的工具操作实现：
机器学习：机器学习的算法对应的是归纳法，学习数据找到数据的规律。
学习型的算法：

机器学习是一个非常难的问题，机器学习是一个学术问题。不是一个工程问题。对数学的基础要求特别的高。
机器学习难以入门。不是一个很难以入门的学科的。
机器学习掌握比较深的人，不是工程是的。而是数据科学家。不要将重点放在方法和理论研究上。不要将尽力放在理论研究上执行的。
更重要的是了解数据如何的处理。如何进行特征工程。数据决定了模型的高度，算法只是让你逼近这个高度。
专业的机器学习而言，80%的时间是处理数据集的，20%的时间是选择算法的。所以，学习的核心应该放在数据的处理上的。
Linux的操作：
软件的安装目录，一般的安装在usr目录或者是opt目录的。
zeppelin：多用途的比较本工具。
1.数据分析的协作功能；
2.作为bi使用：可视化分析。
3.机器学习中的数据探索。
1.学习特征的规律，使用算法生成模型；
2.针对新的生成的模型，输入数据，产生预测结果。
很难通过算法和特征准确的预测哪一个算法的效果更好的，需要进行多次测试的。
大部分的机器学习的时候不是在选择哪一个算法更加的优秀的，大部分的时间更多的是在处理数据的
数据的优劣决定了模型的高度的。算法只是将这个高度不断的推进到理论值的。
大数据开发的关键的问题是数据的处理操作的。
ETL常见的操作：
1.空置的处理：null，NAN(数值型的数据)，"NA"(最为常见的),异常值(容易让模型走向变异),
/**
*处理数据循环排序的问题和对应的操作逻辑实现
*/
%spark
import org.apache.spark.sql.functions._
for(c<-source.columns){
    source.groupBy(col(c)).agg(first(col(c)) as c,countDistinct(col(c)) as s"${c}_count").show
}
数据空值的分析步骤：
1.使用zeppelin判断判断空值的信息
for(c<-source.columns){
    source.groupBy(col(c)).agg(first(col(c)) as c,countDistinct(col(c)) as s"${c}_count").show
}
查询每一列的数据的空值的范围和对应的信息。
2.空值比例不高的话，可以使用na进行数据的填充操作和实现管理的。空值比例特别高的话，对应的只是需要select选择的时候抛弃掉对应的列数据即可。
3.特征的处理：将特征转化为数值，计算机就可以进行识别操作的。
import org.apache.spark.sql.functions._
import spark.implicits._
def  to_level(q:String):Int={
    q  match{
        case "Ex"=>1
        case "Gd"=>2
        case "TA"=>3
        case "Fa"=>4
        case _=>0
    }
}
val udf_to_level=udf(to_level _)
source.select(udf_to_level('ExterQual)).show
4.向数据集中增加新的数据列信息.
import org.apache.spark.sql.functions._
import spark.implicits._
source.select(('TotalBsmtSF+col("1stFlrSF")+col("2ndFlrSF")) as "TotalSF").show
source.withColumn('')
5.将字符串类型的特征转化为数值型的特征信息？
编码方式：OneHot编码方式
将一列转化成为多个列，将单个的数值转化为一个向量。更加符合科学的计算以及数学的计算
%spark
import org.apache.spark.sql.functions._
import spark.implicits._
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.feature.StringIndexer
val cols = Array(
  "BsmtFinType1", "MasVnrType", "Foundation",
  "HouseStyle", "Functional", "BsmtExposure",
  "GarageFinish", "Street", "ExterQual",
  "PavedDrive", "ExterCond", "KitchenQual",
  "HeatingQC", "BsmtQual", "FireplaceQu",
  "GarageQual", "PoolQC"
)
var indexDf:DataFrame=null
for(co<-cols){
    val indexer=new StringIndexer().setInputCol(co).setOutputCol(s"${co}_indexer")
    if(indexDf==null){
       indexDf= indexer.fit(source).transform(source)
    }else{
        indexDf=indexer.fit(indexDf).transform(indexDf)
    }
}
val indexCols=cols.map(col=>s"${col}_indexer").map(c=>col(c))
indexDf.select(indexCols:_*).show
6.Onehot编码操作实现
%spark
import org.apache.spark.sql.functions._
import spark.implicits._
import org.apache.spark.ml.feature.OneHotEncoderEstimator
import org.apache.spark.sql.DataFrame
val oneHotEncoder=new OneHotEncoderEstimator()
     .setInputCols(cols.map(col=>s"${col}_indexer"))
     .setOutputCols(cols.map(col=>s"${col}_onehot"))
// fit  负责找到规律  transform负责转换操作
val oneHotDf=oneHotEncoder.fit(indexDf).transform(indexDf)
oneHotDf.select((cols.map(col=>s"${col}_onehot").map(c=>col(c))):_*).show
7. 将onehot的编码处理成为VectorAssembler的向量信息。
%spark
//  对于很多的机器学习而言，输入的数据只能是一列数据的，不能是多列数据。所以,需要将多列数据进行合并操作实现
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions._
import spark.implicits._
val vector=new VectorAssembler()
      .setInputCols(cols.map(col=>s"${col}_onehot"))
      .setOutputCol("features")
//  执行向量转换操作
val vectorDf=vector.transform(oneHotDf)
vectorDf.select('features).show
8.使用决策树进行预测操作
%spark
//  选择算法： 对应的选择的是决策树信息
//  创建回归算法工具，regressor
//  label 列必须是数值列信息
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.sql.types.DoubleType
//  随机森林，底层对应的也是随机数信息。
val regressor=new RandomForestRegressor()
  .setFeaturesCol("features")
  .setLabelCol("SalePrice")
  .setPredictionCol("prediction")
  .setMaxDepth(5)
  .setImpurity("variance")
val df=vectorDf.select('features,'SalePrice.cast(DoubleType))
//  df  训练模型，通过算法学习规律，生成model
//  通过model 使用transform  进行模型的预测操作
regressor.fit(df).transform(df).select ('SalePrice,'prediction).show


挖掘性的标签：RMF很多的公司都会执行的操作。需要进行关注和理解操作实现
RFM：通过三个维度评价评价用户对于我们的价值度的高低。
1.最后一次的消费的时间距离今天的时间:R
2.购买的评率：F
3.消费的金额：M
描述的事实：RMF维度可以组合成为多个维度的指标的。
1.首先计算RMF的数值，然后执行操作
尽量少的进行模型的训练操作，尽可能多的时间执行模型的预测操作。模型训练的操作可以一周进行一次，一个月进行一次，或者是一年进行一次。
RFM规则的生成对应的是通过打分的机制实现的。
RFE:活跃度标签信息。这个也是需要进行关注操作和实现的。对应的体现出来的是RFE的标签信息。
需要明白RFM以及RFE的活动规则信息。执行数据的过滤操作实现。

// 针对的是单个的用户执行的统计操作的。反应的是单个用户维度的数据信息的？
PSM：价格敏感度模型。模型标签psm价格敏感度标签信息。
PSM Score = 优惠订单占比 + (平均优惠金额 / 平均每单应收) + 优惠金额占比
优惠订单占比：需要计算，对应的是优惠订单/没有优惠订单的数量
            优惠订单=优惠的订单数量/总的订单数量
            未优惠的订单=未优惠的订单数量/总的订单数量
平均优惠金额：需要计算
            总优惠金额/优惠单数
平均每单应收：需要计算
            总应收/总单数
优惠金额占比：需要计算
PSM以及RMF的操作逻辑需要进行确认一下，需要明确的理解逻辑和对应的特性数据的。


聚类操作对应的是一个无监督的算法的。处理的时候并不需要知道数据集对应的标签的，属于无监督的算法处理的。
决策树:解决的是一个分类的问题的。具备了非常经济的算法决策度的操作的。
      决策树对应的是一个逻辑思考的过程，将对应的数据的条件表达成为树的形式进行逻辑操作。
      决策树是怎么得到决策树的形成的。
      决策树的形成对应的是算法轮的形成的。

熵：评价系统的混乱程度。系统越灵活，对应的熵值越大。
信息熵：一句话，表达的意思越多，对应的信息熵的数据就会越大。信息越单一的话，信息熵的值越低的。
      信息熵的公式可以通过相关的公式得到的。决策树：整体得到系统最低的熵值，得到最低的熵而形成的决策树，对应的可以称之为决策树的概念操作的。


通过用户找到对应的购买的商品的信息，从而推断出来对应的用户的性别信息？
修改原则：尽量少的修改现有的代码，而不是不断的扩展对应的代码。增加新的代码进行操作的
    重构的代码是比较的高的。

项目的理解：
1.算法的重要程度:重要。机器学习的算法在商业公司使用的越来越多，成功的也越来越多的。是很关键的。
是一个未来发展的方向的。
2.算法和大家的关系：很难在很短的时间内，先作为一个工程师。算法掌握到一定程度就可以了。算法工程师想对于传统的开发工程师工资会高20%~30%的级别的。
3.复习的重点，整体的流程，
1）调度的过程
2）spark中的资源为什么使用hbase进行操作。

A. Logistic回归可用于预测事件发生概率的大小
B. Logistic回归的目标函数是最小化后验概率
C. SVM的目标的结构风险最小化
D. SVM可以有效避免模型过拟合
答案：B，Logit回归本质上是一种根据样本对权值进行极大似然估计的方法，而后验概率正比于先验概率和似然函数的乘积。
logit仅仅是最大化似然函数，并没有最大化后验概率，更谈不上最小化后验概率。
A错误 Logit回归的输出就是样本属于正类别的几率，可以计算出概率，
正确C. SVM的目标是找到使得训练数据尽可能分开且分类间隔最大的超平面，应该属于结构风险最小化.
 D. SVM可以通过正则化系数控制模型的复杂度，避免过拟合。

准确率=命中的男性用户数量/所有预测男性数量
召回率=命中的男性用户数量/所有男性数量，反映了被正确判定的正例占总的正例的比重。
值得注意的是，决策树的深度不要过深，以防止过拟合的问题
决策树模型的算法还存在很多的问题需要解决的。对应的需要继续探讨和研究这些算法对应的特点和相关的信息的。
当前的算法包括如下的两个步骤的：
1.kmeans的无监督的聚类算法;
2.决策树的监督聚类算法。
决策树的深度如何决定的：
val tree = new DecisionTreeClassifier()
      .setFeaturesCol("features_index")
      .setPredictionCol("predict") //Gini不纯度
      .setMaxDepth(5) //树的最大深度
      .setMaxBins(5) //离散化连续特征的最大划分数
spark实际的操作过程中，主要操作的对象是rdd的数据结构以及对应的dataFrame和相关的dataset的数据结构的。
所有的操作主要的是使用的是rdd的api，以及对应的dataframe的api的数据以及相关的dataFrame的api的数据的，所以，实际的操作的过程中
还是使用spark对应的这三种的数据结构进行操作管理和实现的。
spark的输入和输出对应的只能是rdd,dataset以及dataframe的数据结构的。
###求解的逻辑操作信息？
需要主要以下pageView的数据获取到对应的staylong的信息，获取页面的停留时长的信息的。
数据倾斜的原因在于对应的某一个或者是多个的key的数据明显的偏多，导致部分的task处理的时间特别的长，从而影响整体的执行的效率的。
spark默认采用的是hash分区操作的。可以采用rangePartition处理数据不纯的问题的。
根据key的范围对应的进行划分操作的。
spark对应的pageviews模型里面的staylong数据类型以及对应的step步长的计算逻辑和实现原理如下：
1.staylong时间的计算对应的是
(当前的记录数据开始时间-之前的记录数据信息开始时间)
2.步长step信息：初始的计算步长是1，相同会话内的步长是不断的增加的。
3.使用rangePartition操作的话,对应的同样的key会分布到不同的分区数据中的,需要处理由于这种数据区分导致的数据的不连续的问题的
需要收集不同分区的首尾数据,维护全局的数据的完整性的。处理同样的一个key导致的数据划分到不同的分区信息中的操作的
维护数据的完整性和连续性的。
需要找到存在问题的边界数据进行维护操作实现。原始的数据已经是排好序的数据的，存在如下的问题的。
#########################################################################################
将所有分区的首尾的元素信息广播出去，然后根据分区的首尾信息处理自身分区的信息，进行分区内数据的更新操作实现。
倾斜纠偏的代码需要重新的梳理一下进行操作的。

累加器对应的收集全局的变量的，在多个job之间进行数据的共享操作实现。每一个job对应的会操作数据的，需要对共享的数据进行缓存的。
推荐对于action操作执行cache操作缓存操作的。
上述解决问题的步骤相当的繁琐，并且数据操作存在很大的逻辑问题，是否是可以提供更加好的逻辑操作服务和实现方式的。操作存在很大的问题，需要进行处理实现。不推荐使用这样的聚合操作方式。
下面是实际的使用过程中，可以采取的代码的聚合操作方式的。实际的解决倾斜问题的代码需要进一步的操作实现。
离线分析实现t+1的方式实现数据的收集操作的。需要对应的调度操作的。
实时计算模型的话，对应的是不需要相关的调度操作的。实时运行的话，是一直在运行的操作的。

宽表可以理解为对应的明细表的数据信息的。

问题：怎么将数仓的sql语句转换成为调度操作的。使得数仓的建立和分析可以采用调度的任务执行操作的
这样的话，对应的调度的任务就可以创建起来的。后续的可以得到分析结果从而更好的处理相关的数据信息的。
这种批处理的操作需要的是工作流来进行任务的调度的。数据之间的依赖关系需要相关的调度操作的。
模块之间存在大量的依赖的关系，存在大量的重复性的操作的，可以使用工作流调度系统来完成执行的操作和实现的。
需要的是工作流的调度系统执行调度操作的。需要执行的是任务的调度系统执行的操作的。使用定时的任务调度可以使用任务的调度系统执行调度的。
使用azkaban执行任务流的调度操作的。oozie的执行调度需要大量的xml的。存在问题的。使用job文件的方式是向任务的调度的。

使用azkaban执行任务的定时调度操作。使用shell的方式来实现调度操作的。
实际的业务的开发过程中，执行的操作对应的是使用shell的方式实现相关的调度操作执行的业务逻辑的。使用shell脚本的方式实现相关的脚本的执行操作的。

对于复杂的数据预处理的操作，需要使用到spark来完成更加复杂的数据的预处理和清洗的过程的。简单的数据的清洗的话，直接使用对应的kettle，sqoop,flume等的操作工具就可以了。
对于jar文件的话，需要使用spark-submit执行任务调度即可实现对应的操作实现的。
使用zakaban调度的话，对应的shell的日志信息需要进行整合操作的，对应的日志数据的信息需要整合起来的。具体的调度的信息日志是无法在进行整合操作的
需要进行整合操作的。否则的话，在大规模集群的环境下面对应的收集相关的日志信息的话，是存在问题的。

离线数仓的调度操作：需要的是在shell的基础之上进行相关的数据调度操作和管理的。



















