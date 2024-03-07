import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.math.log
//Decision tree class holding functions to train and inference on the model

case class DecisionTree(maxDepth : Int = 4, minSamplesLeaf: Int = 1,
                        minInformationGain: Double = 0.0, numOfFeaturesSplitting : String = "all",
      amtOfSay :  Double = -0.0, sc : SparkContext
                       ){

  private def _entropy(classProbs : RDD[Double]): Double = {
      classProbs.filter(_>0).reduce((x,y) => x + -y * log(y)/log(2))
  }

  private def _classProbs(labels : RDD[Any]):  RDD[Double] = {
      val rddSize = labels.count()
      this.sc.parallelize(labels.countByValue().map({ case (label, count) => count.toDouble / rddSize}).toList)
  }

  private def _data_entropy(labels: RDD[Any]): Double = {
    _entropy(_classProbs(labels))
  }

}


