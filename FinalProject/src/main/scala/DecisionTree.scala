import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.math.{log, random}
//Decision tree class holding functions to train and inference on the model


case class DecisionTree(maxDepth : Int = 4, minSamplesLeaf: Int = 1,
                        minInformationGain: Double = 0.0, numOfFeaturesSplitting : String = "all",
      amtOfSay :  Double = -0.0//, sc : SparkContext
                       )  {

  val _entropy = (classProbs: List[Double])=> {
    classProbs.filter(_ > 0).reduce((x, y) => x + -y * log(y) / log(2))
  }

//  val _classProbs = (labels: RDD[String]) => {
//    val rddSize = labels.count()
//    this.sc.parallelize(labels.countByValue().map({ case (label, count) => count * 1.0 / rddSize }).toList)
//  }
  val _classProbs = (labels: List[String]) => {
    val rddSize = labels.size
    labels.groupBy(identity).mapValues(_.size).map({case (x, count) => count * 1.0 / rddSize}).toList
  }

//  val _data_entropy = (labels: RDD[String]) => {
//    _entropy(_classProbs(labels))
//  }
val _data_entropy = (labels: List[String]) => {
  _entropy(_classProbs(labels))
}

  //Gets Entropy
//  def _partition_entropy(subsets: RDD[List[String]]): Double = {
//    subsets.persist
//    val totalCount = subsets.flatMap(x => x).count
//    val weighted_entropies = subsets.map(lst => _data_entropy(sc.parallelize(lst)) * lst.length / totalCount).reduce((x, y) => x + y)
//    weighted_entropies
//  }
  def _partition_entropy(subsets: RDD[List[String]]): Double = {
    subsets.persist
    val totalCount = subsets.flatMap(x => x).count
    val weighted_entropies = subsets.map(lst => _data_entropy(lst) * lst.length / totalCount).reduce((x, y) => x + y)
    weighted_entropies
  }

  //this function splits the data frame based on feature_idx (column_idx)
  //Assuming that we are only using categorical variables, we will return splits based on distinct categories
  //Input: Column of DataFrame
  def _split(rows: RDD[Adult], featureIdx: String): RDD[List[Adult]] = {
    val splits = rows.map(x => (x.getFeatureAsString(featureIdx), x))
    val groups = splits.groupByKey().map({ case (key, list) => list.toList })
    groups
  }

  // 
  def _find_best_split(data: RDD[Adult]): (RDD[List[Row]], String, Double) = {
    val featuresToUse = data.first.getFeatures.filter(feature => feature != "income")
    val entropies = featuresToUse.map(feature => (feature, _split(data, feature))) // .collect(), case (feature, Array(value, List[Row]))
      .map({ case (feature, split) => (split, feature, _partition_entropy(split.map(lst => lst.map(x => x.getAs[String]("income"))))) })
    val sortedEnt = entropies.sortBy({ case (split, feature, entropy) => entropy })//.take(0)(0) // //              ^ List[Row] ^ Row
  sortedEnt(0)
  }

  // (feature: String, Array(value: String, rows: List[Row]))
  def _find_label_probs(data: RDD[String]): RDD[Double] = { // returns rdd of probs for each label (>=50k or <50k)
    val labelCounts = data.map(x => (x, 1))
      .reduceByKey(_ + _)
    val totalCount = labelCounts.values.reduce((x, y) => x + y)
    labelCounts.map(_._2 / totalCount.toDouble)
  }

  def _create_tree(data: RDD[Adult], current_depth: Integer): //= {
  Unit = {
    if (current_depth > maxDepth) {
      return null
    }


    val (rows, featureIdx, entropy) = _find_best_split(data)
    val labelProbs = _find_label_probs(data.map(lst => lst.getAs[String]("income")))

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
    //TreeNode()
  }
}



