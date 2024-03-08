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
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") .config("spark.kryoserializer.buffer", "1024k") .config("spark.kryoserializer.buffer.max", "1024m") //.config("spark.kryo.registrationRequired", "true")
  .getOrCreate()


    val df = spark.read.option("header", "true").option("inferSchema", "true").csv("adult.csv")
    df.show()
    df.printSchema()
    val cols = df.columns
    // How to read a column in spark df
    df.rdd.map(row => List(row.getAs[Int]("age"))).take(10).foreach(println)
    val test = df.rdd
    val decisionTree = DecisionTree()//sc = spark.sparkContext)

    // AFAIK SPLIT WORKS
    //print(decisionTree._partition_entropy(decisionTree._split(test, " workclass").map(x => x.map(y => y.getAs[String](" income")))))//.take(1).foreach(println)
   val bestTestSplit = decisionTree._find_best_split(test)
    bestTestSplit._1.collect().foreach(println)
    println(bestTestSplit._2)
    println(bestTestSplit._3)
    //val testEnt = spark.sparkContext.parallelize(List(List("1", "2", "1"), List("1", "2", "1", "2")))
   // print(decisionTree._data_entropy(testEnt))
    //print(decisionTree._partition_entropy(testEnt))
    
    //    val conf = new SparkConf().setAppName("FinalProject")
    //      .setMaster("local[4]")
    //    val sc = new SparkContext(conf)
    

  }

}