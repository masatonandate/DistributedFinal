import org.apache.spark.rdd.RDD
//Decision tree class holding functions to train and inference on the model

package FinalProject {
  case class NewDecisionTree(maxDepth: Int = 3, minSamplesLeaf: Int = 1,
                             minInformationGain: Double = 0.0,
                             amtOfSay: Double = -0.0 //, sc : SparkContext
                            ) {

    //Goes through all data with remaining features and finds best feature, returns (featureName, featureIdx), RDD[(featureValue, List(rows with featureValue))], entropy)
    def getBestSplit(data: RDD[Array[String]], features: Array[(String, Int)]): Array[((String, Int), (RDD[(String, List[Array[String]])], Double))] //: RDD[Array[String]] = {
    = {
      features.map({ case (feature, index) => ((feature, index), partitionEntropy(data.map(x => (x(index), x)))) })
        .sortBy({case ((feature, index), (list, entropy)) => -entropy})
    }

    // Gets total weighted entropy and splits per value for a feature
    def partitionEntropy(value: RDD[(String, Array[String])]): (RDD[(String, List[Array[String]])], Double) = {
      val total_labels = value.count()
      // Calculates total weighted entropy for specific feature
      val weighted_entropy = value
        .map({ case (ftVal, row) => (ftVal, row(row.length - 1)) })
        .groupByKey()
        .map({ case (ftVal, labelSplit) => entropy(labelSplit.toList) * labelSplit.toList.length / total_labels }).sum
      val splits = value
        .groupByKey()
        .map({ case (ftVal, split) => (ftVal, split.toList) })
      (splits, weighted_entropy)
    }

    // Takes in a list of labels and returns the entropy
    def entropy(data: List[String]): Double = {
      val counts = data.groupBy(identity).mapValues(_.size)
      val probs = counts.map({ case (label, count) => count.toDouble / data.length })
      probs.map(prob => -prob * math.log(prob)).sum
    }

    // Given the numerator - information gain, the subsets, and parentDataLength, returns InformationGainRatio
    def calculateRatio(sublist: RDD[(String, List[Array[String]])], numerator: Double, parentDataLength: Double): Double = {
      val subCounts = sublist.map({case (featureVal, subrows) => subrows.length * 1.0})
      val denominator = subCounts.map(subLength => -(subLength / parentDataLength) * math.log(subLength / parentDataLength))
      if (denominator.sum() != -0.0) {numerator / denominator.sum()}
      else {0}
    }

    // Given split, finds class entropy
    def findClassEntropy(labels: RDD[Array[String]]): (List[(String, Double)], Double) = {
      val rddSize = labels.count()
      val label_probs = labels.map(x => x(x.length - 1)).countByValue().map({ case (label, count) => (label, count * 1.0 / rddSize) }).toList
      (label_probs, label_probs.map({ case (label, prob) => -prob * math.log(prob) }).sum)
    }

    def create_tree(data: RDD[Array[String]], currentDepth: Int, features: Array[(String, Int)], parent: String): NewTreeNode = {
      if (currentDepth > maxDepth || features.length == 0 || data.count() == 0) {
        return null
      }
      val splitResult = getBestSplit(data, features)

      val parentEntropyAndProbs = findClassEntropy(data)
      val parentEntropy = parentEntropyAndProbs._2
      val parentLength = data.collect().length

      // Calculate All Information Gains
      // val informationGain = parentEntropyAndProbs._2 - splitResult._2._2

      // This gets information gain for all columns
      val informationGains = splitResult
        .map({case ((feature, index), (list, entropy)) => ((feature, index), (list, entropy, parentEntropy - entropy))})
      //this calculate gain ratio for all columns
      val informationGainRatio = informationGains
        .map({case ((feature, index), (list, entropy, gainRatio)) => ((feature, index), (list, entropy, calculateRatio(list, gainRatio, parentLength)))})
      //want to get highest gain ratio
      val bestSplit = informationGainRatio.minBy({case ((feature, index), (list, entropy, gainRatio)) => -gainRatio})

      val node = NewTreeNode(data, bestSplit._1._1, bestSplit._1._2, parentEntropyAndProbs._1, bestSplit._2._3)

      // If the split contains any samples that are smaller than the minimum samples at a leaf, return the node
      if (bestSplit._2._1.map({ case (ftVal, split) => split.length < minSamplesLeaf }).filter(_ == true).count > 0) {
        return node
      }
      // If information gain is worse than minimum described
      else if (bestSplit._2._3 <= minInformationGain) {
        return node
      }

      //This change ensures that, if we are on the last feature to be chosen, it is not ommited from the list for the children nodes
      //Instead the children nodes are made inplace here, using the splits already predetermined as part of finding lowest entropy class
      if (features.length==1){
        node.children = bestSplit._2._1.collect.map({case (featVal, split)=> {val newData = data.filter(row => row(features(0)._2) == featVal);
        val childEntAndProbs = findClassEntropy(newData);
          (featVal, NewTreeNode(newData, features(0)._1, features(0)._2, childEntAndProbs._1, bestSplit._2._2 - childEntAndProbs._2))}})
      }
      else if (bestSplit._2._2 == 0.0){
        node.children = bestSplit._2._1.collect.map({case (featVal, split)=> {val newData = data.filter(row => row(bestSplit._1._2) == featVal);
          val childEntAndProbs = findClassEntropy(newData);
          (featVal, NewTreeNode(newData, bestSplit._1._1, bestSplit._1._2, childEntAndProbs._1, bestSplit._2._2 - childEntAndProbs._2))}})
      }
      else {
        // Don't really want to collect this because its an large rdd of splits but trying for now
        node.children = bestSplit._2._1.collect.map({ case (featVal, split) =>
          (featVal,
            create_tree(data.filter(row => row(bestSplit._1._2) == featVal), currentDepth + 1, features.filter({ case (ft, idx) => idx != bestSplit._1._2 }), featVal))
        })
      }
      node
    }

    def recursive_print(node: NewTreeNode, level: Int = 0): Unit = {
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

    def evaluate(testRow: Array[String], node: NewTreeNode): String = {
      if (node.children == null || node.children.length == 0 || !node.children.forall({ case (ftVal, node) => node != null })){
        return node.mostLikelyLabel
      }
      val newNode = node.children.filter({ case (featVal, childNode) => testRow(node.featureIdx) == featVal })
      // Base case when we reach a leaf
      if (newNode.length == 0) {
        return node.mostLikelyLabel
      }

      //for a row in testData, traverseTree
      evaluate(testRow, newNode(0)._2)
    }
  }
}