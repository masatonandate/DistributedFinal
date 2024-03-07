import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

import scala.math.{log, random}
//Decision tree class holding functions to train and inference on the model

case class DecisionTree(maxDepth : Int = 4, minSamplesLeaf: Int = 1,
                        minInformationGain: Double = 0.0, numOfFeaturesSplitting : String = "all",
      amtOfSay :  Double = -0.0, sc : SparkContext
                       ){

   def _entropy(classProbs : RDD[Double]): Double = {
      classProbs.filter(_>0).reduce((x,y) => x + -y * log(y)/log(2))
  }

   def _classProbs(labels : RDD[Any]): RDD[Double] = {
      val rddSize = labels.count()
      this.sc.parallelize(labels.countByValue().map({ case (label, count) => count * 1.0 / rddSize}).toList)
  }

   def _data_entropy(labels: RDD[Any]): Double = {
    _entropy(_classProbs(labels))
  }

  //Gets Entropy
  def _partition_entropy(subsets: RDD[List[String]]) : Double = {
    subsets.persist
    val totalCount = subsets.flatMap(x => x).count
    val weighted_entropies = subsets.map(lst => _data_entropy(sc.parallelize(lst)) * lst.length / totalCount ).reduce((x,y) => x + y)
    weighted_entropies
  }

  //this function splits the data frame based on feature_idx (column_idx)
  //Assuming that we are only using categorical variables, we will return splits based on distinct categories
  //Input: Column of DataFrame
  def _split(data: DataFrame, featureIdx: String) : RDD[List[Row]] = {
    val splits = data.rdd.map(row => (row.getAs[String](featureIdx), row))
    val groups = splits.groupByKey().map({case (key, list) => list.toList})
    groups
  }

  //
   def _find_best_split(data : DataFrame): (RDD[List[Row]], String, Double) = {
     val featuresToUse = data.columns
     val entropies = featuresToUse.map(feature => (feature, _split(data, feature))) // .collect(), case (feature, Array(value, List[Row]))
       .map({case (feature, split) => (split, feature, _partition_entropy(split.map(lst => lst.map(x=> x.getAs[String]("income")))))})
     entropies.sortBy({case (split, feature, entropy) => entropy}).take(0)(0) //               ^ List[Row] ^ Row
   }
// (feature: String, Array(value: String, rows: List[Row]))
  def _find_label_probs(data: RDD[Row]): RDD[Double] = { // returns rdd of probs for each label (>=50k or <50k)
    val labelCounts = data.map(x => (x, 1))
      .reduceByKey(_+_)
    val totalCount = labelCounts.values.reduce((x, y) => x+y)
    labelCounts.map(_._2 / totalCount.toDouble)
  }

  def _create_tree(data: RDD[Row], current_depth: Integer): TreeNode = {
    if (current_depth > maxDepth) {
      return null
    }

    val (rows, featureIdx, entropy) = _find_best_split(data)
    val labelProbs = _find_label_probs(data.map(lst => lst.map(x => x.getAs[String]("income"))))

    //    """
//        Recursive, depth first tree creation algorithm
//        """
//
//    # Check if the max depth has been reached (stopping criteria)
//    if current_depth > self.max_depth:
//    return None
//
//    # Find best split
//    split_1_data, split_2_data, split_feature_idx, split_feature_val, split_entropy = self._find_best_split(data)
//
//    # Find label probs for the node
//      label_probabilities = self._find_label_probs(data)
//
//    # Calculate information gain
//    node_entropy = self._entropy(label_probabilities)
//    information_gain = node_entropy - split_entropy
//
//    # Create node
//      node = TreeNode(data, split_feature_idx, split_feature_val, label_probabilities, information_gain)
//
//    # Check if the min_samples_leaf has been satisfied (stopping criteria)
//    if self.min_samples_leaf > split_1_data.shape[0] or self.min_samples_leaf > split_2_data.shape[0]:
//    return node
//    # Check if the min_information_gain has been satisfied (stopping criteria)
//    elif information_gain < self.min_information_gain:
//    return node
//
//    current_depth += 1
//    node.left = self._create_tree(split_1_data, current_depth)
//    node.right = self._create_tree(split_2_data, current_depth)
//
//    return node
  }

  def _predict_one_sample(X: RDD[Any]): RDD[Any] = {
//    """Returns prediction for 1 dim array"""
//    node = self.tree
      node = self.tree
//
//    # Finds the leaf which X belongs,
//    while node:
//      pred_probs = node.prediction_probs
//    if X[node.feature_idx] < node.feature_val:
//      node = node.left
//    else:
//    node = node.right
//
//    return pred_probs
  }
}
/*
class TreeNode():
    def __init__(self, data, feature_idx, feature_val, prediction_probs, information_gain) -> None:
        self.data = data
        self.feature_idx = feature_idx
        self.feature_val = feature_val
        self.prediction_probs = prediction_probs
        self.information_gain = information_gain
        self.feature_importance = self.data.shape[0] * self.information_gain
        self.left = None
        self.right = None

    def node_def(self) -> str:

        if (self.left or self.right):
            return f"NODE | Information Gain = {self.information_gain} | Split IF X[{self.feature_idx}] < {self.feature_val} THEN left O/W right"
        else:
            unique_values, value_counts = np.unique(self.data[:,-1], return_counts=True)
            output = ", ".join([f"{value}->{count}" for value, count in zip(unique_values, value_counts)])
            return f"LEAF | Label Counts = {output} | Pred Probs = {self.prediction_probs}"
 */
// copilot generated
case class TreeNode() {

}


