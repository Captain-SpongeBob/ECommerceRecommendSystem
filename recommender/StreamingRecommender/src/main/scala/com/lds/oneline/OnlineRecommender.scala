package com.lds.oneline

import com.mongodb.casbah.Imports.{MongoClient, MongoClientURI, MongoDBObject}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

import scala.collection.JavaConversions._
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

// 连接助手对象 建立redis、MongoDB的连接
object ConnHelper extends Serializable{
  lazy val jedis:Jedis = new Jedis("192.168.200.101")
  lazy val mongoClient:MongoClient = MongoClient(MongoClientURI("mongodb://localhost:27017/recommender"))
}
//MongoDb配置
case class MongConfig(uri:String,db:String)
// 标准推荐
case class Recommendation(productId:Int, score:Double)
// 用户的推荐
case class UserRecs(userId:Int, recs:Seq[Recommendation])
//商品的相似度
case class ProductRecs(productId:Int, recs:Seq[Recommendation])

object OnlineRecommender {
  val MAX_USER_RATINGS_NUM = 20
  val MAX_SIM_PRODUCTS_NUM = 20
  val MONGODB_STREAM_RECS_COLLECTION = "StreamRecs"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_PRODUCT_RECS_COLLECTION = "ProductRecs"

  /**
   * 获取用户当前最近的M次商品评分
   * @param num  评分的个数
   * @param userId  谁的评分
   * @return
   */
  def getUserRecentlyRating(num: Int, userId: Int, jedis: Jedis): Array[(Int,Double)] = {
    //从redis得用户评分队列中取出num个评分数据，list键名为uid:USERID(例如 USERID为1的，uid:1)，值为 PRODUCTID:SCORE
    jedis.lrange("userId:"+userId.toString, 0, num)
      .map{
      item =>
      val attr = item.split("\\:")
      (attr(0).trim.toInt, attr(1).trim.toDouble)
    }
    .toArray

  }

  /**
   * 获取当前商品K个相似的商品  也就是获取候选列表
   *
   * @param num         相似商品的数量
   * @param productId   当前商品的ID
   * @param userId      当前的评分用户
   * @param simProducts 商品相似度矩阵的广播变量值
   * @param mongConfig  MongoDB的配置
   * @return
   */
  def getTopSimProducts(num: Int,
                        productId: Int,
                        userId: Int,
                        simProducts: scala.collection.Map[Int,scala.collection.immutable.Map[Int,Double]])
                       (implicit mongConfig: MongConfig): Array[Int] = {
    /**
     * 1. 从相似性矩阵中获取当前商品相似的前K个商品
     * 2. 获取用户当前的N个评分过的商品
     * 3. 去除步骤1中与步骤2重复的商品
     */
    //从广播变量的商品相似度矩阵中获取当前商品所有的相似商品 Map[Int, Map[productId, Int]] --> Map[productId, Int] --> Array[Int, Double]
    val allSimProducts :Array[(Int, Double)] = simProducts(productId).toArray
    // 连接MongoDB 获取用户已经评分过得商品，
    val ratingExist = ConnHelper
      .mongoClient(mongConfig.db)(MONGODB_RATING_COLLECTION)
      .find(MongoDBObject("userId" -> userId))
      .toArray
      .map{
      item => item.get("productId").toString.toInt
    }

    //从所有相似商品中过滤掉已经评分过得商品，并降序输出，这里x是 allSimProducts里的，   Map[productId, score]
    allSimProducts.filter(x => !ratingExist.contains(x._1)).sortWith(_._2 > _._2).take(num).map(x =>  x._1)
  }

  /**
   * 计算待选商品的推荐分数
   * @param simProducts            商品相似度矩阵
   * @param userRecentlyRatings  用户最近的k次评分
   * @param candidateProducts         当前商品最相似的K个商品
   * @return
   */
  def computeProductScores(simProducts: scala.collection.Map[Int,scala.collection.immutable.Map[Int,Double]],
                           userRecentlyRatings:Array[(Int,Double)],
                           candidateProducts: Array[Int]): Array[(Int,Double)] = {
    //定义一个长度可变的数组ArrayBuffer，用于保存每一个待选商品和最近评分的每一个商品的权重得分（productid，score）
    val score = scala.collection.mutable.ArrayBuffer[(Int,Double)]()
    //用于保存每一个商品的增强因子数
    val increMap = scala.collection.mutable.HashMap[Int,Int]()
    //用于保存每一个商品的减弱因子数
    val decreMap = scala.collection.mutable.HashMap[Int,Int]()

    for (candidateProduct <- candidateProducts; userRecentlyRating <- userRecentlyRatings){
      val simScore = getProductsSimScore(simProducts,userRecentlyRating._1,candidateProduct)
      if(simScore > 0.6){
        score += ((candidateProduct, simScore * userRecentlyRating._2 ))
        if(userRecentlyRating._2 > 3){
          increMap(candidateProduct) = increMap.getOrDefault(candidateProduct,0) + 1
        }else{
          decreMap(candidateProduct) = decreMap.getOrDefault(candidateProduct,0) + 1
        }
      }
    }

    score.groupBy(_._1).map{
      case (productId,scoreList) =>
      (productId,scoreList.map(_._2).sum / scoreList.length
        + log(increMap.getOrDefault(productId, 1)) - log(decreMap.getOrDefault(productId, 1)))
    }.toArray.sortWith(_._2>_._2)
  }

  /**
   * 获取当个商品之间的相似度
   * @param simProducts       商品相似度矩阵
   * @param recentlyRatingProduct 用户已经评分的商品
   * @param candidateProduct     单个候选商品
   * @return
   */
    //其实就是去相似性矩阵里看之前离线计算的结果
  def getProductsSimScore(simProducts :scala.collection.Map[Int,scala.collection.immutable.Map[Int,Double]],
                          recentlyRatingProduct: Int,
                          candidateProduct: Int): Double = {
    simProducts.get(candidateProduct) match {
      case Some(sim) => sim.get(recentlyRatingProduct) match {
        case Some(score) => score
        case None => 0.0
      }
      case None => 0.0
    }
  }
  //取10的对数
  def log(m:Int):Double ={
    math.log(m) / math.log(10)
  }
  /**
   * 将数据保存到MongoDB    userId -> 1,  recs -> 22:4.5|45:3.8
   * @param streamRecs  流式的推荐结果
   * @param mongConfig  MongoDB的配置
   */
  def saveRecsToMongoDB(userId:Int,streamRecs:Array[(Int,Double)])(implicit mongConfig: MongConfig): Unit ={
    //到StreamRecs的连接
    val streaRecsCollection = ConnHelper.mongoClient(mongConfig.db)(MONGODB_STREAM_RECS_COLLECTION)

    streaRecsCollection.findAndRemove(MongoDBObject("userId" -> userId))
    streaRecsCollection.insert(MongoDBObject("userId" -> userId, "recs" ->
      streamRecs.map( x => MongoDBObject("productId"->x._1,"score"->x._2)) ))
  }

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender",
      "kafka.topic" -> "recommender"
    )
    //创建一个SparkConf配置
    val sparkConf = new SparkConf().setAppName("StreamingRecommender").setMaster(config("spark.cores"))
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext,Seconds(2))
    implicit val mongConfig = MongConfig(config("mongo.uri"),config("mongo.db"))
    import spark.implicits._

    // 广播商品相似度矩阵
    //装换成为 Map[Int, Map[Int,Double]]
    val simProductsMatrix = spark
      .read
      .option("uri",config("mongo.uri"))
      .option("collection",MONGODB_PRODUCT_RECS_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRecs]
      .rdd
      .map{
        item => (item.productId,item.recs.map(x => (x.productId,x.score)).toMap)
      }
      .collectAsMap()
    //广播出去
    val simProductsMatrixBroadCast:Broadcast[collection.Map[Int, Map[Int, Double]]] = spark.sparkContext.broadcast(simProductsMatrix)

    //创建到Kafka的连接
    val kafkaParam = Map(
      "bootstrap.servers" -> "hadoop1:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recommender",
      "auto.offset.reset" -> "latest"
    )
    val kafkaStream :InputDStream[ConsumerRecord[String, String]]= KafkaUtils.createDirectStream[String,String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Array(config("kafka.topic")),kafkaParam))

    // UID|MID|SCORE|TIMESTAMP
    // 产生评分流
    val ratingStream :DStream[(Int, Int, Double, Int)]= kafkaStream.map{
      case msg =>
        var attr = msg.value().split("\\|")
        (attr(0).toInt,attr(1).toInt,attr(2).toDouble,attr(3).toInt)
    }
    //TODO  核心实时推荐算法
    ratingStream.foreachRDD{
      rdd => rdd.map {case (userId,productId,score,timestemp) =>
        println(">>>>>>>>>>>>>>>>")

        //1.从redis中取出当前用户最近的M次商品评分，保存成一个数组Array[(productId,score)]
        val userRecentlyRatings:Array[(Int, Double)] = getUserRecentlyRating(MAX_USER_RATINGS_NUM,userId,ConnHelper.jedis)
        //2.从相似度矩阵中获取商品P的最相似的K个商品，作为备选列表,Array[productId]
        val candidateProducts:Array[Int] = getTopSimProducts(MAX_SIM_PRODUCTS_NUM,productId,userId,simProductsMatrixBroadCast.value)
        //3.计算每个待选商品的推荐优先级，得到当前用户得实时推荐列表，保存成Array[(productId,score)]
        val streamRecs:Array[(Int, Double)] = computeProductScores(simProductsMatrixBroadCast.value,userRecentlyRatings,candidateProducts)
        //4.将数据保存到MongoDB
        saveRecsToMongoDB(userId,streamRecs)
      }
    }

    //启动Streaming程序
    ssc.start()
    System.out.println("streaming start")
    ssc.awaitTermination()
  }
}
