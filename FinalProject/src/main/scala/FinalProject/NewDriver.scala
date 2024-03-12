import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

package FinalProject {
  object NewDriver {
    def main(args: Array[String]): Unit = {
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)
      // Setup Spark session
      val conf = new SparkConf().setAppName("NameOfApp")
        .setMaster("local[4]")
      val sc = new SparkContext(conf)

      val text = sc.textFile("adult.csv") // If running on server, put Hadoop input file path
      val header = text.first()
      val values = text.filter(line => line != header)
        .map(line => line.split(","))

      //Splitting Training and Testing (80/20)
      val trainTest = values.randomSplit(Array(0.8, 0.2), 1)
      val training = trainTest(0)
      val testing = trainTest(1)
      //val training = values.sample(withReplacement = false, fraction = 0.8)
      println("training length:" ,training.collect().length)
//      val testing = values.subtract(training)
      println("testing length:", testing.collect().length)
      println("Training Positive Length", training.filter(x => (x(x.length - 1) == ">50K")).collect().length)

      //getting the answers
      val testAnswers = testing.map(row => (row(0), row(row.length - 1)))
      val trainAnswers = training.map(row => (row(0), row(row.length - 1)))

      // (featureName, featureIdx)
      val featureNames = Array(("workclass", 2), ("education", 4), ("marital-status", 6), ("race", 9), ("native-country", 14))
//      val testFeature = Array(("dummy", 15))
      //val featureNames = Array(("workclass", 2), ("education", 4))//, ("marital-status", 6), ("race", 9), ("native-country", 14))

      //Build tree on train Data
      val decisionTree = NewDecisionTree(maxDepth = 5)
//      val parentNode = decisionTree.create_tree(training, 0, testFeature, null)
      val parentNode = decisionTree.create_tree(values, 0, featureNames, null)

//      decisionTree.recursive_print(parentNode)
      //Feed Test Data into Tree and get Results
      val testOutput = testing.map(x => (x(0), decisionTree.evaluate(x, parentNode)))
      val trainOutput = training.map(x => (x(0), decisionTree.evaluate(x, parentNode)))


      //computing F-Score for testAnswers
      val joinedAnsOut = testAnswers.join(testOutput)
//      joinedAnsOut.collect().foreach(println)
      val tp = joinedAnsOut
        .filter({ case (rownum, (actual, predicted)) => actual == predicted && actual == ">50K" }).collect().length
      val fp = joinedAnsOut
        .filter({ case (rownum, (actual, predicted)) => actual != predicted && predicted == ">50K" }).collect().length
      val fn = joinedAnsOut
        .filter({ case (rownum, (actual, predicted)) => actual != predicted && predicted == "<=50K" }).collect().length
      val tn = joinedAnsOut
        .filter({case (rownum, (actual, predicted)) => actual == predicted && actual == "<=50K"}).collect().length

      val precision = tp.toDouble / (tp + fp)
      val recall = tp.toDouble / (tp + fn)

      val fscore = 2 * (precision * recall) / (precision + recall)
      println("Testing", fscore, precision, recall, tp, fp, fn, tn)

      //computing F-Score for train Answers
      val joinedTrainAnsOut = trainAnswers.join(trainOutput)
//      joinedTrainAnsOut.collect().foreach(println)
      val trtp = joinedTrainAnsOut
        .filter({ case (rownum, (actual, predicted)) => actual == predicted && actual.equals(">50K") }).collect().length
      val trfp = joinedTrainAnsOut
        .filter({ case (rownum, (actual, predicted)) => actual != predicted && predicted.equals(">50K") }).collect().length
      val trfn = joinedTrainAnsOut
        .filter({ case (rownum, (actual, predicted)) => actual != predicted && predicted.equals("<=50K") }).collect().length
      val trtn = joinedTrainAnsOut
        .filter({case (rownum, (actual, predicted)) => actual == predicted && actual.equals("<=50K")}).collect().length

      val trprecision = trtp.toDouble / (trtp + trfp)
      val trrecall = trtp.toDouble / (trtp + trfn)

      val trfscore = 2 * (trprecision * trrecall) / (trprecision + trrecall)
      println("Training", trfscore, trprecision, trrecall, trtp, trfp, trfn, trtn)
      decisionTree.recursive_print(parentNode, 3)
    }
  }
}