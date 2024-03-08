import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}


case class TreeNode(data : RDD[Row], feature_idx : String, featureVals : RDD[String], label_probabilities : RDD[Double], information_gain :Double){
    /*Data Structure that contains information about a certain tree node.
     Note: When extracting featureVals, need to make splits and featureVals are in same order
     May have to put featureVals and splits together*/
  var children : RDD[TreeNode] = null
    override def toString: String = {
        if (children != null) {
            val children_paths = featureVals.zipWithIndex().map({case (feature, index) => s"Child $index: ${feature}"}).collect().mkString(", ")
            s"NODE | Information Gain = $information_gain | Children = $children_paths"
        } else {
            val value_counts = data.map(x => x.getAs[String]("income")).countByValue
            val output = value_counts.map({case (label, count) => s"${label}->${count}"}).mkString(", ")
          s"LEAF | Label Counts = $output | Pred Probs = ${label_probabilities.collect().mkString(", ")}"
        }
    }
}

