package com.lds.statistics
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Rating(userId: Int, productId: Int, score: Double, timestamp: Int)

case class MongoConfig(uri:String, db:String)

object StatisticsReconnender {
  //MongoDB中的表名
  val MONGODB_RATING_COLLECTION = "Rating"
  //统计表的名称
  val RATE_MORE_PRODUCTS = "RateMoreProducts"
  val RATE_MORE_RECENTLY_PRODUCTS = "RateMoreRecentlyProducts"
  val AVERAGE_PRODUCTS = "AverageProducts"
  //入口方法
  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )
    //创建SparkConf配置
    val sparkConf = new SparkConf().setAppName("StatisticsRecommender").setMaster(config("spark.cores"))
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
    // 声明一个隐式的配置对象
//    implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))
    //加入隐式转换
    import spark.implicits._

//    加载数据
    val ratingDF = spark
      .read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()
    //创建一张名为ratings的临时表
    ratingDF.createOrReplaceTempView("ratings")
    //TODO: 用Spark SQL 做不同的统计推荐结果

    //1.统计所有历史数据中每个商品的评分数
    //数据结构 -》  productId,count
    val rateMoreProductsDF: DataFrame = spark.sql("select productId,count(productId) as count from ratings group by productId")
    storeDFinMongoDB(rateMoreProductsDF,RATE_MORE_PRODUCTS)(mongoConfig)

    //2.统计以月为单位拟每个商品的评分数    //数据结构 -》 productId,count,time
    //创建一个日期格式化工具
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    //注册一个UDF，用于将timestamp转换为年月格式，
    spark.udf.register("changeDate", (x: Int) => simpleDateFormat.format(new Date(x * 1000L)).toInt)
    //将原来的Ratings数据集中的时间转换为年月格式
    val ratingsOfYearMonth = spark.sql("select productId,score,changeDate(timestamp) as yearmonth from ratings")
    //将新的数据集注册为一张表
    ratingsOfYearMonth.createOrReplaceTempView("ratingOfMonth")
    //按年月和商品ID进行分组，统计同一个月内同一商品的评价总次数,再按月份和评价总次数降序
    val rateMoreRecentlyProducts = spark.sql("select productId,count(productId) as count,yearmonth " +
      "from ratingOfMonth group by yearmonth,productId order by yearmonth desc,count desc")
    storeDFinMongoDB(rateMoreRecentlyProducts,RATE_MORE_RECENTLY_PRODUCTS)(mongoConfig)

    //3统计每个商品的评均评分
    val averageProductsDF = spark.sql("select productId,avg(score) as avg from ratings group by productId")
    storeDFinMongoDB(averageProductsDF,AVERAGE_PRODUCTS)(mongoConfig)

    spark.stop()

  }

  //统一的存储函数
  def storeDFinMongoDB(df:DataFrame,collection_name:String)(implicit mongoConfig: MongoConfig): Unit = {
    df.write
      .option("uri",mongoConfig.uri)
      .option("collection",collection_name)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

  }
}
