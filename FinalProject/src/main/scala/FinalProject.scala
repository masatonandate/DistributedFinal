import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql._
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._


object FinalProject {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    // Setup Spark session for interaction with Dataset API
    val spark = SparkSession.builder
      .master("local")
      .appName("FinalProject")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val df = spark.read.option("header", "true").option("inferSchema", "true").csv("adult.csv")
    df.show()
    df.printSchema()
    val cols = df.columns
    // How to read a column in spark df
    df.rdd.map(row => List(row.getAs[Int]("age"))).take(10).foreach(println)

    val test = spark.sparkContext.parallelize(List(0.1, 0.2, 0.3))
    val decisionTree = DecisionTree(sc = spark.sparkContext)
    print(decisionTree._entropy(test))
    
    //    val conf = new SparkConf().setAppName("FinalProject")
    //      .setMaster("local[4]")
    //    val sc = new SparkContext(conf)
    

  }

}