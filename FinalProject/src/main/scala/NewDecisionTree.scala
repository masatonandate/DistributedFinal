import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.math.log
//Decision tree class holding functions to train and inference on the model


case class NewDecisionTree(maxDepth : Int = 4, minSamplesLeaf: Int = 1,
                           minInformationGain: Double = 0.0, numOfFeaturesSplitting : String = "all",
                           amtOfSay :  Double = -0.0 //, sc : SparkContext
                       )  {

  //Goes through all data with remaining features and finds best feature,
  def getBestSplit(data: RDD[Array[String]], features: Array[(String, Int)]): ((String, Int), (RDD[(String, List[Array[String]])], Double)) //: RDD[Array[String]] = {
  = {
    features.map({case (feature, index) => ((feature, index), partitionEntropy(data.map(x => (x(index), x))))}).minBy({case ((feature, index), (list, entropy)) => entropy})//.minBy({case (feature, weighted_ent) => weighted_ent})(Ordering[Double])
  }


  // Gets total weighted entropy and splits per feature
  def partitionEntropy(value: RDD[(String, Array[String])]): (RDD[(String, List[Array[String]])], Double) = {
    val total_labels = value.count()
//    println(total_labels)
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
    if (currentDepth > maxDepth){
      return null
    }
    if (features.length==0){
      return null
    }

    if (data.count() == 0){
      return null
    }

    val splitResult = getBestSplit(data, features)
//  println("Split Result: " + splitResult._1._1)
//    println("Split Entropy: " + splitResult._2._2)
    val parentEntropyAndProbs = findClassEntropy(data)

    //Get information gain from best split weighted entropy and parent Entropy
    val informationGain = parentEntropyAndProbs._2 - splitResult._2._2
//    println("Information Gain: " + informationGain)

    val node = NewTreeNode(data, splitResult._1._1, parentEntropyAndProbs._1, informationGain)

    // If the split contains any samples that are smaller than the minimum samples at a leaf, return the node
    //splitResult._2._1.map({case (ftVal, split) => split.length}).foreach(println)
    if (splitResult._2._1.map({case (ftVal, split) => split.length < minSamplesLeaf}).filter(_ == true).count > 0){
      return node
    }
      //If information gain is worse than minimum described
    else if (informationGain < minInformationGain){
      return node
    }
    //Don't really want to collect this because its an large rdd of splits but trying for now
    node.children = splitResult._2._1.collect.map({case (featVal, split) => (featVal, create_tree(data.filter(row => row(splitResult._1._2) == featVal), currentDepth + 1, features.filter({case (ft, idx) => idx != splitResult._1._2}), featVal))})

    return node
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
    //      print(
    //        "" *
    //          4 * level + "->" + node.toString
    //      )
  }

//  def _create_tree(self, data: np.array, current_depth: int) -> TreeNode:
//    """
//        Recursive, depth first tree creation algorithm
//        """
//
//  # Check if the max depth has been reached (stopping criteria)
//  if current_depth > self.max_depth:
//  return None
//
//  # Find best split
//  split_1_data, split_2_data, split_feature_idx, split_feature_val, split_entropy = self._find_best_split(data)
//
//  # Find label probs for the node
//    label_probabilities = self._find_label_probs(data)
//
//  # Calculate information gain
//  node_entropy = self._entropy(label_probabilities)
//  information_gain = node_entropy - split_entropy
//
//  # Create node
//    node = TreeNode(data, split_feature_idx, split_feature_val, label_probabilities, information_gain)
//
//  # Check if the min_samples_leaf has been satisfied (stopping criteria)
//  if self.min_samples_leaf > split_1_data.shape[0] or self.min_samples_leaf > split_2_data.shape[0]:
//  return node
//  # Check if the min_information_gain has been satisfied (stopping criteria)
//  elif information_gain < self.min_information_gain:
//  return node
//
//  current_depth += 1
//  node.left = self._create_tree(split_1_data, current_depth)
//  node.right = self._create_tree(split_2_data, current_depth)
//
//  return node

}





