import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._

object NewDriver {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    // Setup Spark session
    val conf = new SparkConf().setAppName("NameOfApp")
      .setMaster("local[4]")
    val sc = new SparkContext(conf)

    val text = sc.textFile("adult.csv")
    val header = text.first()
    val values = text.filter(line => line != header)
      .map(line => line.split(","))

    //Splitting Training and Testing (80/20)
    val training = values.sample(withReplacement=false, fraction=0.8)
    val testing = values.subtract(training)
    //getting the answers
    val testAnswers = testing.map(row => (row(0), row(row.length - 1)))

    // (featureName, featureIdx)
    val featureNames = Array(("workclass", 2), ("education", 4), ("marital-status", 6), ("race", 9), ("native-country", 14))

    //Build tree on train Data
    val decisionTree = NewDecisionTree(maxDepth = 5)
    val parentNode = decisionTree.create_tree(training, 0, featureNames, null)

    //Feed Test Data into Tree and get Results
    val testOutput = testing.map(x => (x(0), decisionTree.evaluate(x, parentNode)))

    //computing F-Score
    val joinedAnsOut = testAnswers.join(testOutput)
//    joinedAnsOut.collect().foreach(println)
    val tp = joinedAnsOut
      .filter({case (rownum, (actual, predicted)) => actual == predicted && actual == ">50k"}).collect().length
    val fp = joinedAnsOut
      .filter({case (rownum, (actual, predicted)) => actual != predicted && predicted == ">50k"}).collect().length
    val fn = joinedAnsOut
      .filter({case (rownum, (actual, predicted)) => actual != predicted && predicted != ">50k"}).collect().length

    val precision = tp.toDouble / (tp + fp)
    val recall = tp.toDouble / (tp + fn)

    val fscore = 2 * (precision * recall) / (precision + recall)
    println(fscore, precision, recall, tp, fp, fn)

//    val fn = testAnswers.subtract(testOutput)
//    println(fn.collect().length)



    //decisionTree.evaluate()
    //decisionTree.recursive_print(decisionTree.create_tree(values, 0,featureNames, null))
    //Evaluate F1 Score

  }
}