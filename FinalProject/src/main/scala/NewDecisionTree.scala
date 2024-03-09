import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.math.log
//Decision tree class holding functions to train and inference on the model


case class NewDecisionTree(maxDepth : Int = 4, minSamplesLeaf: Int = 1,
                           minInformationGain: Double = 0.0, numOfFeaturesSplitting : String = "all",
                           amtOfSay :  Double = -0.0 //, sc : SparkContext
                       )  {

  //Goes through all data with remaining features and finds best feature, returns (featureName, featureIdx), RDD[(featureValue, List(rows with featureValue))], entropy)
  def getBestSplit(data: RDD[Array[String]], features: Array[(String, Int)]): ((String, Int), (RDD[(String, List[Array[String]])], Double)) //: RDD[Array[String]] = {
  = {
    features.map({case (feature, index) => ((feature, index), partitionEntropy(data.map(x => (x(index), x))))})
      .minBy({case ((feature, index), (list, entropy)) => entropy})
  }


  // Gets total weighted entropy and splits per value for a feature
  def partitionEntropy(value: RDD[(String, Array[String])]): (RDD[(String, List[Array[String]])], Double) = {
    val total_labels = value.count()
    // Calculates total weighted entropy for specific feature
    val weighted_entropy = value.map({ case (ftVal, row) => (ftVal, row(row.length - 1)) }).groupByKey().map({ case (ftVal, labelSplit) => entropy(labelSplit.toList) * labelSplit.toList.length / total_labels }).sum
    val splits = value.groupByKey().map({ case (ftVal, split) => (ftVal, split.toList) })
    (splits, weighted_entropy)

  }

  //Takes in a list of labels and returns the entropy
  def entropy(data: List[String]): Double = {
    val counts = data.groupBy(identity).mapValues(_.size)
    val probs = counts.map({ case (label, count) => count.toDouble / data.length })
    probs.map(prob => -prob * math.log(prob)).sum
  }

  // Given split, finds class entropy
  def findClassEntropy(labels: RDD[Array[String]]): (List[Double], Double) = {
    val rddSize = labels.count()
    val label_probs = labels.map(x => x(x.length - 1)).countByValue().map({ case (label, count) => count * 1.0 / rddSize }).toList
    (label_probs, label_probs.map(prob => -prob * math.log(prob)).sum)
  }

  def create_tree(data: RDD[Array[String]], currentDepth: Int, features : Array[(String, Int)], parent: String): NewTreeNode = {
    println()
    println(parent)
    if (currentDepth > maxDepth || features.length==0 || data.count() == 0) {
      return null
    }
    val splitResult = getBestSplit(data, features)

    splitResult._2._1.foreach(x => println(x._1))

    val parentEntropyAndProbs = findClassEntropy(data)

    //Get information gain from best split weighted entropy and parent Entropy
    val informationGain = parentEntropyAndProbs._2 - splitResult._2._2

    val node = NewTreeNode(data, splitResult._1._1, splitResult._1._2, parentEntropyAndProbs._1, informationGain)

    // If the split contains any samples that are smaller than the minimum samples at a leaf, return the node
    if (splitResult._2._1.map({case (ftVal, split) => split.length < minSamplesLeaf}).filter(_ == true).count > 0){
      return node
    }
    // If information gain is worse than minimum described
    else if (informationGain < minInformationGain){
      return node
    }
    // Don't really want to collect this because its an large rdd of splits but trying for now
    node.children = splitResult._2._1.collect.map({case (featVal, split) =>
      (featVal,
        create_tree(data.filter(row => row(splitResult._1._2) == featVal), currentDepth + 1, features.filter({case (ft, idx) => idx != splitResult._1._2}), featVal))})
    node
  }

  def recursive_print(node : NewTreeNode, level:Int=0): Unit = {
    if (node != null) {
      println(
        "    " *
          4 * level + "->" + node.toString
      )
      if (node.children != null) {
        for (child <- node.children) {
          recursive_print(child._2, level + 1)
        }
      }
    }
  }

//  def evaluate(testRow: Array[String], node: NewTreeNode): String = {
//    //base case
//
//    //for a row in testData, traverseTree
//    val newNode = node.children.filter({case (featVal, childNode) => testRow(node.featureIdx) == childNode.featureName})
//
//
//
//  }



}





