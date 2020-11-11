package com.lds.offline

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jblas.DoubleMatrix

case class ProductRating(userId: Int, productId: Int, score: Double, timestamp: Int)
case class MongoConfig(uri:String, db:String)
// 标准推荐对象，productId,score
case class Recommendation(productId: Int, score:Double)
// 用户推荐列表
case class UserRecs(userId: Int, recs: Seq[Recommendation])
// 商品相似度（商品推荐）
case class ProductRecs(productId: Int, recs: Seq[Recommendation])

object OfflineRecommender {
  def consinSim(product1: DoubleMatrix, product2: DoubleMatrix):Double = {
    product1.dot(product2) / ( product1.norm2()  * product2.norm2() )
  }

  // 定义常量
  val MONGODB_RATING_COLLECTION = "Rating"
  // 推荐表的名称
  val USER_RECS = "UserRecs"
  val PRODUCT_RECS = "ProductRecs"
  //推荐列表有多长
  val USER_MAX_RECOMMENDATION = 20

  def main(args: Array[String]): Unit = {
    //定义配置
    val config = Map(
      "spark.core" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )

    //创建SparkSession
    val sparkConf = new SparkConf().setMaster(config("spark.core")).setAppName("OfflineRecommender")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))
    import spark.implicits._

    val ratingDS = spark
      .read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRating]
      .map(rating => (rating.userId, rating.productId, rating.score))
      .cache()

    //提取出所有商品和用户数据集
    val userDS = ratingDS.map(_._1).distinct()//取出第一列
    val productDS = ratingDS.map(_._2).distinct()//第二列

    //TODO:核心计算过程
    //TODO 1. 训练隐语义模型
    //创建训练集
    val trainData:RDD[Rating]  = ratingDS.map(x => Rating(x._1, x._2, x._3)).rdd
    // rank 是模型中隐语义因子的个数, iterations 是迭代的次数, lambda 是ALS的正则化参
    val (rank,iterations,lambda) = (50, 5, 0.01)
    // 调用ALS算法训练隐语义模型
    val model:MatrixFactorizationModel = ALS.train(trainData,rank,iterations,lambda)

    //TODO 2. 获得预测评分矩阵，得到用户的推荐列表
    //用userRDD和productRDD做一个笛卡尔积，得到空的userProducts
    val userProducts = userDS.rdd.cartesian(productDS.rdd)
    // model已训练好，把id传进去就可以得到预测评分列表RDD[Rating] (userId,productId,rating)
    val preRatings:RDD[Rating] = model.predict(userProducts)

    val userRecs:DataFrame  = preRatings
      .filter(_.rating > 0)
      .map(rating => (rating.user, (rating.product, rating.rating)))
      .groupByKey()//按userId分组
      .map {
        case (userId, recs) => UserRecs(userId, recs.toList.sortWith(_._2 > _._2)
          .take(USER_MAX_RECOMMENDATION).map(x => Recommendation(x._1, x._2)))
      }
      .toDF()

    userRecs.write
      .option("uri",mongoConfig.uri)
      .option("collection",USER_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    //TODO 3.利用商品的特征向量，计算商品的相似度矩阵
    val productFeatures = model.productFeatures.map{
      case (productId,features) => (productId, new DoubleMatrix(features))
    }

    val productRecs = productFeatures.cartesian(productFeatures)
      .filter{
        case (a,b) => a._1 != b._1//过滤掉自己和自己计算的向量
      }
      .map{
        case  (a,b) =>
        val simScore = this.consinSim(a._2,b._2) // 求余弦相似度
        (a._1,(b._1,simScore))
      }
      .filter(_._2._2 > 0.6)
      .groupByKey()
      .map{
        case (productId,items) =>
        ProductRecs(productId,items.toList.map(x => Recommendation(x._1,x._2)))
      }
      .toDF()

    productRecs
      .write
      .option("uri", mongoConfig.uri)
      .option("collection",PRODUCT_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // 关闭spark
    spark.stop()


  }
}
