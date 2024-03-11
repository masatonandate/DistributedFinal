import org.apache.spark.rdd.RDD
//Decision tree class holding functions to train and inference on the model

package FinalProject {
  case class NewDecisionTree(maxDepth: Int = 3, minSamplesLeaf: Int = 1,
                             minInformationGain: Double = 0.0,
                             amtOfSay: Double = -0.0 //, sc : SparkContext
                            ) {

    // Given feature of integers, finds 25th, 50th, and 75th percentiles
    def find_percentiles(data: RDD[Int]): (Int, Int, Int) = {
      val featureVals = data.collect().sorted(Ordering[Int])
      (featureVals((featureVals.length * 0.25).toInt), featureVals((featureVals.length * 0.5).toInt), featureVals((featureVals.length * 0.75).toInt))
    }

    //Goes through all data with remaining features and finds best feature, returns (featureName, featureIdx), RDD[(featureValue, List(rows with featureValue))], entropy)
    def getBestSplit(data: RDD[Array[String]], categoricalFeatures: Array[(String, Int)], numericFeatures: Array[(String, Int)]): ((String, Int), (RDD[(String, List[Array[String]])], Double)) //: RDD[Array[String]] = {
    = {
      categoricalFeatures.map({ case (feature, index) => ((feature, index), partitionEntropy(data.map(x => (x(index), x)))) })
        .union(numericFeatures.map({ case (feature, index) => ((feature, index), partitionEntropy(data.map(x => (x(index), x)), isNumeric=true)) }))
        .minBy({ case ((feature, index), (list, entropy)) => entropy })
    }

    def getKeyFromPercentile(value: Int, percentiles: (Int, Int, Int)): String = {
      if (value < percentiles._1) {
        "0"
      } else if (value < percentiles._2) {
        "1"
      } else if (value < percentiles._3) {
        "2"
      } else {
        "3"
      }
    }

    // Gets total weighted entropy and splits per value for a feature
    def partitionEntropy(value: RDD[(String, Array[String])], isNumeric: Boolean = false): (RDD[(String, List[Array[String]])], Double) = {
      val total_labels = value.count()
      if (isNumeric) {
        // Calculates total weighted entropy for each percentile
        val percentiles = find_percentiles(value.map({ case (ftVal, row) => ftVal.toInt }))
        val weighted_entropy = value
          .map({ case (ftVal, row) => (getKeyFromPercentile(ftVal.toInt, percentiles), row(row.length - 1)) })
          .groupByKey()
          .map({ case (ftVal, labelSplit) => entropy(labelSplit.toList) * labelSplit.toList.length / total_labels }).sum
        val splits = value
          .map({ case (ftVal, split) => (getKeyFromPercentile(ftVal.toInt, percentiles), split) })
          .groupByKey()
          .map({ case (ftVal, split) => (ftVal, split.toList) })
        (splits, weighted_entropy)
      } else {
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
    }

    //Takes in a list of labels and returns the entropy
    def entropy(data: List[String]): Double = {
      val counts = data.groupBy(identity).mapValues(_.size)
      val probs = counts.map({ case (label, count) => count.toDouble / data.length })
      probs.map(prob => -prob * math.log(prob)).sum
    }

    // Given split, finds class entropy
    def findClassEntropy(labels: RDD[Array[String]]): (List[(String, Double)], Double) = {
      val rddSize = labels.count()
      val label_probs = labels.map(x => x(x.length - 1)).countByValue().map({ case (label, count) => (label, count * 1.0 / rddSize) }).toList
      (label_probs, label_probs.map({ case (label, prob) => -prob * math.log(prob) }).sum)
    }

    def create_tree(data: RDD[Array[String]], currentDepth: Int, categoricalFeatures: Array[(String, Int)], numericFeatures: Array[(String, Int)], parent: String): NewTreeNode = {
      if (currentDepth > maxDepth || (categoricalFeatures.length == 0 && numericFeatures.length == 0) || data.count() == 0) {
        return null
      }
      val splitResult = getBestSplit(data, categoricalFeatures, numericFeatures)

      println(splitResult._1, splitResult._2._2)

      val parentEntropyAndProbs = findClassEntropy(data)

      // If the parent is already a pure node, return null
      if (parentEntropyAndProbs._2 == 0.0) {
        return null
      }

      //Get information gain from best split weighted entropy and parent Entropy
      val informationGain = parentEntropyAndProbs._2 - splitResult._2._2

      val node = NewTreeNode(data, splitResult._1._1, splitResult._1._2, parentEntropyAndProbs._1, informationGain)

      // If the split contains any samples that are smaller than the minimum samples at a leaf, return the node
      if (splitResult._2._1.map({ case (ftVal, split) => split.length < minSamplesLeaf }).filter(_ == true).count > 0) {
        return node
      }
      // If information gain is worse than minimum described
      else if (informationGain < minInformationGain) {
        return node
      }

      //This change ensures that, if we are on the last feature to be chosen, it is not omitted from the list for the children nodes
      //Instead the children nodes are made inplace here, using the splits already predetermined as part of finding lowest entropy class
      if (categoricalFeatures.length==1 && numericFeatures.length==0){
        node.children = splitResult._2._1.collect.map({case (featVal, split) => val newData = data.filter(row => row(categoricalFeatures(0)._2) == featVal)
          val childEntAndProbs = findClassEntropy(newData)
          (featVal, NewTreeNode(newData, categoricalFeatures(0)._1, categoricalFeatures(0)._2, childEntAndProbs._1, splitResult._2._2 - childEntAndProbs._2))})
      } else if (categoricalFeatures.length==0 && numericFeatures.length==1){
        node.children = splitResult._2._1.collect.map({case (featVal, split) => val newData = data.filter(row => row(numericFeatures(0)._2) == featVal)
          val childEntAndProbs = findClassEntropy(newData)
          (featVal, NewTreeNode(newData, numericFeatures(0)._1, numericFeatures(0)._2, childEntAndProbs._1, splitResult._2._2 - childEntAndProbs._2))})
      } else {
        // Don't really want to collect this because its an large rdd of splits but trying for now
        node.children = splitResult._2._1.collect.map({ case (featVal, split) =>
          (featVal,
            create_tree(data.filter(row => row(splitResult._1._2) == featVal), currentDepth + 1, categoricalFeatures.filter({ case (ft, idx) => idx != splitResult._1._2 }), numericFeatures.filter({ case (ft, idx) => idx != splitResult._1._2 }), featVal))
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
      val newNode = node.children.filter({ case (featVal, childNode) => testRow(node.featureIdx) == childNode.featureName })
      // Base case when we reach a leaf
      if (node.children == null || node.children.length == 0 || newNode.length == 0) {
        return node.mostLikelyLabel
      }

      //for a row in testData, traverseTree
      evaluate(testRow, newNode(0)._2)
    }
  }
}