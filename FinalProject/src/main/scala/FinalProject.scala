import org.apache.spark.SparkContext._

import scala.io._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.collection._
import scala.collection.immutable.List

// rowNumber,age,workclass,fnlwgt,education,education-num,marital-status,occupation,relationship,race,sex,capital-gain,capital-loss,hours-per-week,native-country,income

object FinalProject {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
        val conf = new SparkConf().setAppName("FinalProject")
          .setMaster("local[4]")
        val sc = new SparkContext(conf)
  
    // Read in the CSV, key=rowNumber, val=Adult object (which is row)
    val data = sc.textFile("adult.csv")
      .map(_.split(","))
      .map(x => (x(0).toInt, Adult(x(1).toInt, x(2), x(3).toInt, x(4), x(5).toInt, x(6), x(7), x(8), x(9), x(10), x(11).toInt, x(12).toInt, x(13).toInt, x(14), x(15))))
    val features = List("age","workclass","fnlwgt","education","educationNum",
      "maritalStatus","occupation","relationship","race","sex","capitalGain",
      "capitalLoss","hoursPerWeek","nativeCountry","income")
    val decisionTree = DecisionTree(columnLabels = features.filter(_ != "income"))

   val bestTestSplit = decisionTree._find_best_split(data.map({case (rowNum, adult) => adult}))
    bestTestSplit._1.collect().foreach(println)
    println(bestTestSplit._2)
    println(bestTestSplit._3)

  }
}