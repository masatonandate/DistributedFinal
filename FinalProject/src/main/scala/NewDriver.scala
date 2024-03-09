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
    val values = text.filter(line => line != header).map(line => line.split(","))

    //Splitting Training and Testing (80/20)
    val training = values.sample(withReplacement=false, fraction=0.8)
    val testing = values.subtract(training)

    // (featureName, featureIdx)
    val featureNames = Array(("workclass", 1), ("education", 3), ("marital-status", 5))

    //Build tree on train Data
    val decisionTree = NewDecisionTree()
    decisionTree.create_tree(training, 0,featureNames, null)

    //Feed Test Data into Tree and get Results
    //decisionTree.evaluate()
    decisionTree.recursive_print(decisionTree.create_tree(values, 0,featureNames, null))
    //Evaluate F1 Score

  }
}