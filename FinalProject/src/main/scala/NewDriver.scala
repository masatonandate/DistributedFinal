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
    // Get features minus the label (income) and add index to pull from rdds
    //val featureNames = header.split(",").dropRight(1).zipWithIndex
    val featureNames = Array(("workclass", 1), ("education", 3), ("marital-status", 5))
    //val featureNames = Array(("native-country", 13))
    //getTestSplit(values, featureNames).foreach(x => x.collect().foreach(println))
    //val testEntropy = List("1", "1", "0", "0")
    //print(entropy(testEntropy))
    val decisionTree = NewDecisionTree()
    //val testBestSplit = decisionTree.getBestSplit(values, featureNames)
    //println(testBestSplit._1)
    //println(testBestSplit._2._2)
    //testBestSplit._2._1.collect.foreach({case (str, lst) => println(str);lst.foreach(x => println(x(x.length-1)))})

    decisionTree.recursive_print(decisionTree.create_tree(values, 0,featureNames, null))
    

  }
  //Iterates over each feature still available, finds weighted entropy of all partitions, finds smallest disorder for a feature,
  // Returns data splits mapped to features, feature, optionally probabilities for information gain
//  def getBestSplit(data: RDD[Array[String]], features: Array[(String, Int)]) //: RDD[Array[String]] = {
//  = {
//    features.map({case (feature, index) => (feature, partitionEntropy(data.map(x => (x(index), x))))}).minBy({case (feature, (list, entropy)) => entropy})//.minBy({case (feature, weighted_ent) => weighted_ent})(Ordering[Double])
//  }
//
//
// def partitionEntropy(value: RDD[(String, Array[String])]) ={
//   val total_labels = value.count()
//   println(total_labels)
//   // Calculates total weighted entropy for specific feature
//   val weighted_entropy = value.map({case (ftVal, row) => (ftVal, row(row.length-1))}).groupByKey().map({case (ftVal, labelSplit) => entropy(labelSplit.toList) * labelSplit.toList.length / total_labels}).sum
//    val splits = value.groupByKey().map({case (ftVal, split) => (ftVal, split.toList)})
//    (splits, weighted_entropy)
//
// }
//
//  //Takes in a list of labels and returns the entropy
//  def entropy(data: List[String]) = {
//    val counts = data.groupBy(identity).mapValues(_.size)
//    val probs = counts.map({case (label, count) => count.toDouble / data.length})
//    probs.map(prob => -prob * math.log(prob)).sum
//  }
//
//  def findClassEntropy(labels: RDD[Array[String]]) = {
//    val rddSize = labels.count()
//    labels.map(x => x(x.length-1)).countByValue().map({case (label, count) => count * 1.0 / rddSize}).toList.map(prob => -prob * math.log(prob)).sum
//
//  }


}