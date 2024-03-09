import org.apache.spark.rdd.RDD

package FinalProject {
  case class NewTreeNode(data: RDD[Array[String]], featureName: String, featureIdx: Int, label_probabilities: List[(String, Double)], information_gain: Double) {
    /*Data Structure that contains information about a certain tree node.
     Note: When extracting featureVals, need to make splits and featureVals are in same order
     May have to put featureVals and splits together*/
    val mostLikelyLabel: String = label_probabilities.minBy(x => -x._2)._1
    var children: Array[(String, NewTreeNode)] = null

    override def toString: String = {
      if (children != null && children.forall({ case (ftVal, node) => node != null })) {
        val children_paths = children.zipWithIndex.map({ case ((feature, child), index) => s"Child $index: ${feature}" }).mkString(", ")
        s"NODE | Information Gain = $information_gain | Children = $children_paths"
      } else {
        val value_counts = data.map(x => x(x.length - 1)).countByValue
        val output = value_counts.map({ case (label, count) => s"${label}->${count}" }).mkString(", ")
        s"LEAF | Label Counts = $output | Pred Probs = ${label_probabilities.mkString(", ")}"
      }
    }
  }
}

